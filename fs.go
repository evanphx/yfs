package yfs

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/aclements/go-rabin/rabin"
	"github.com/evanphx/yfs/format"
	"github.com/golang/crypto/blake2b"
)

const (
	Window       = 64
	AverageBlock = 4 << 10
	MinBlock     = 512
	MaxBlock     = 32 << 10
)

var table = rabin.NewTable(rabin.Poly64, Window)

const (
	File = 1
	Dir  = 2
	Link = 3
)

const (
	SetUID = 04000
	SetGID = 02000
)

type FS struct {
	txnlock sync.Mutex

	root    string
	tocPath string

	toclock   sync.Mutex
	toc       *format.TOC
	tocBlocks *format.BlockTOC

	tocSet *format.BlockSet

	blockslock sync.RWMutex
	blocks     *format.BlockTOC

	tocHeader format.TOCHeader

	blockAccess blockAccess
}

const bufferSize = 1024

var (
	buffers = sync.Pool{New: func() interface{} { return make([]byte, bufferSize) }}
	blocks  = sync.Pool{New: func() interface{} { return make([]byte, AverageBlock*2) }}
)

const roundBuffer = 256

func roundUp(sz int) int {
	return sz + roundBuffer - (sz % roundBuffer)
}

func getBlockBuf(sz int) []byte {
	buf := blocks.Get().([]byte)
	if len(buf) < sz {
		blocks.Put(buf)
		buf = make([]byte, roundUp(sz))
	}

	return buf
}

func putBlockBuf(buf []byte) {
	blocks.Put(buf[:cap(buf)])
}

var DefaultHead = "primary"

func NewFS(root string, opts ...Option) (*FS, error) {
	err := os.MkdirAll(filepath.Join(root, "heads"), 0755)
	if err != nil {
		return nil, err
	}

	fs := &FS{
		root:    root,
		tocPath: filepath.Join("heads", DefaultHead),
	}

	fs.toc = &format.TOC{
		Paths: make(map[string]*format.Entry),
	}

	fs.tocBlocks = &format.BlockTOC{}

	fs.blocks = &format.BlockTOC{}

	for _, opt := range opts {
		opt(fs)
	}

	fs.blockAccess.root = filepath.Join(root, "blocks")
	err = os.MkdirAll(fs.blockAccess.root, 0755)
	if err != nil {
		return nil, err
	}

	err = fs.readTOC()
	if err != nil {
		return nil, err
	}

	err = fs.readBlocksTOC()
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func (f *FS) Txn(write bool) *Txn {
	if write {
		f.txnlock.Lock()
	}

	return &Txn{
		f:     f,
		write: write,

		root:    f.root,
		tocPath: f.tocPath,

		toc:       f.toc,
		tocBlocks: f.tocBlocks,
		blocks:    f.blocks,

		tocSet: f.tocSet,

		tocHeader: f.tocHeader,

		blockAccess: f.blockAccess,

		updates: &format.TOC{
			Paths: make(map[string]*format.Entry),
		},
	}
}

func (f *FS) CopyFile(path string, of *os.File) error {
	txn := f.Txn(true)

	defer txn.Commit()

	return txn.CopyFile(path, of)
}

func (f *FS) WriteFile(path string, r io.Reader) error {
	txn := f.Txn(true)

	defer txn.Commit()

	return txn.WriteFile(path, r)
}

var (
	ErrCompressionMismatch = errors.New("compression setting mismatched")
	ErrWrongEncryptionKey  = errors.New("wrong encryption key provided")
	ErrCorruptTOC          = errors.New("table of contents is corrupt")
)

func (f *FS) readTOC() error {
	data, err := ioutil.ReadFile(filepath.Join(f.root, f.tocPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var fheader format.TOCHeader

	sz := data[0]

	err = fheader.Unmarshal(data[1 : 1+sz])
	if err != nil {
		return err
	}

	if f.tocHeader.Compressed != fheader.Compressed {
		return ErrCompressionMismatch
	}

	if !bytes.Equal(f.tocHeader.KeyId, fheader.KeyId) {
		return ErrWrongEncryptionKey
	}

	dataSum := blake2b.Sum256(data[256 : 256+fheader.TocSize])

	if !bytes.Equal(fheader.Sum, dataSum[:]) {
		return ErrCorruptTOC
	}

	var (
		tocSize   = fheader.TocSize
		blockSize = fheader.BlocksSize
	)

	buf, err := f.blockAccess.readTransform(data[256 : 256+tocSize])
	if err != nil {
		return err
	}

	var set format.BlockSet

	err = set.Unmarshal(buf)
	if err != nil {
		return err
	}

	setData, err := f.blockAccess.readSet(&set)
	if err != nil {
		return err
	}

	err = f.toc.Unmarshal(setData)
	if err != nil {
		return err
	}

	var bs format.BlockTOC

	bsData := data[256+tocSize : 256+tocSize+blockSize]

	data, err = f.blockAccess.readTransform(bsData)
	if err != nil {
		return err
	}

	err = bs.Unmarshal(data)
	if err != nil {
		return err
	}

	f.tocSet = &set

	f.tocBlocks = &bs

	return nil
}

func (f *FS) readBlocksTOC() error {
	of, err := os.Open(filepath.Join(f.root, "blocks.idx"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return nil
	}

	defer of.Close()

	buf := buffers.Get().([]byte)

	stat, err := of.Stat()
	if err != nil {
		return err
	}

	tocSize := int(stat.Size())

	if len(buf) < tocSize {
		buf = make([]byte, tocSize+64)
	}

	defer buffers.Put(buf)

	_, err = of.Read(buf[:tocSize])
	if err != nil {
		return err
	}

	return f.blocks.Unmarshal(buf[:tocSize])
}

func (f *FS) ReaderFor(path string) (io.Reader, error) {
	return f.Txn(false).ReaderFor(path)
}

func (f *FS) WriterFor(path string) (io.WriteCloser, error) {
	txn := f.Txn(true)

	wc, err := txn.WriterFor(path)
	if err != nil {
		return nil, err
	}

	wc.(*blockWriter).commit = true

	return wc, nil
}

var ErrCorruptFile = errors.New("corrupt file detected")

func (f *FS) RemoveFile(path string) error {
	txn := f.Txn(true)
	defer txn.Commit()

	return txn.RemoveFile(path)
}

func (fs *FS) CreateSnapshot(name string) error {
	txn := fs.Txn(true)
	defer txn.Commit()

	return txn.CreateSnapshot(name)
}

func (fs *FS) ReadSnapshot(name string) (*FS, error) {
	return NewFS(fs.root, WithSettingsFrom(fs), WithHead(name))
}
