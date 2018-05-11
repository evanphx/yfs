package yfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

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
	root    string
	tocPath string

	toc    *format.TOC
	blocks *format.BlockTOC

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

func NewFS(root string, opts ...Option) (*FS, error) {
	fs := &FS{
		root:    root,
		tocPath: "toc.dat",
	}

	fs.toc = &format.TOC{
		Paths: make(map[string]*format.Entry),
	}

	fs.blocks = &format.BlockTOC{}

	for _, opt := range opts {
		opt(fs)
	}

	fs.blockAccess.root = filepath.Join(root, "blocks")
	err := os.MkdirAll(fs.blockAccess.root, 0755)
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

func (f *FS) CopyFile(path string, of *os.File) error {
	stat, err := of.Stat()
	if err != nil {
		return err
	}

	if !stat.Mode().IsRegular() {
		return fmt.Errorf("only supports files atm")
	}

	ent := &format.Entry{
		Perm: int32(stat.Mode().Perm()),
	}

	if sys, ok := stat.Sys().(*syscall.Stat_t); ok {
		if u, err := user.LookupId(strconv.Itoa(int(sys.Uid))); err == nil {
			ent.Uname = u.Username
		}

		if g, err := user.LookupGroupId(strconv.Itoa(int(sys.Gid))); err == nil {
			ent.Gname = g.Name
		}

		ent.CreatedAt = &format.TimeSpec{Seconds: sys.Ctimespec.Sec, Nanoseconds: int32(sys.Ctimespec.Nsec)}
	}

	ent.ModifiedAt = &format.TimeSpec{stat.ModTime().Unix(), int32(stat.ModTime().Nanosecond())}

	_, err = f.writeFile(path, of, ent)
	return err
}

func (f *FS) WriteFile(path string, r io.Reader) error {
	_, err := f.writeFile(path, r, &format.Entry{})
	return err
}

func (f *FS) writeAsBlocks(r io.Reader) (*format.BlockSet, error) {
	backing := getBlockBuf(0)

	buf := bytes.NewBuffer(backing[:0])

	defer putBlockBuf(backing)

	var total int64

	fh, err := blake2b.New256(nil)
	if err != nil {
		return nil, err
	}

	c := rabin.NewChunker(table, io.TeeReader(r, buf), MinBlock, AverageBlock, MaxBlock)

	var blocks []*format.Block

	for i := 0; ; i++ {
		len, err := c.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		total += int64(len)

		h, err := blake2b.New256(nil)
		if err != nil {
			return nil, err
		}

		block := buf.Next(len)

		_, err = h.Write(block)
		if err != nil {
			return nil, err
		}

		sum := h.Sum(nil)

		fh.Write(sum[:])

		clen, err := f.writeBlock(sum[:], block)
		if err != nil {
			return nil, err
		}

		if clen != -1 {
			f.blocks.Blocks = append(f.blocks.Blocks, &format.BlockInfo{
				Id:         sum[:],
				ByteSize:   int64(len),
				CompSize:   clen,
				References: 1,
			})
		}

		blocks = append(blocks, &format.Block{
			Id: sum[:],
		})
	}

	fhSum := fh.Sum(nil)

	set := &format.BlockSet{
		Blocks:   blocks,
		Sum:      fhSum[:],
		ByteSize: total,
	}

	return set, nil
}

func (f *FS) writeFile(path string, r io.Reader, ent *format.Entry) (int64, error) {
	set, err := f.writeAsBlocks(r)
	if err != nil {
		return 0, err
	}

	ent.ByteSize = set.ByteSize
	ent.Type = File
	ent.Hash = set.Sum
	ent.Blocks = set

	f.toc.Paths[path] = ent

	err = f.flushBlockTOC()
	if err != nil {
		return 0, err
	}

	return set.ByteSize, f.flushTOC()
}

func (f *FS) writeBlock(sum []byte, block []byte) (int64, error) {
	if blk, ok := f.blocks.FindBlock(sum); ok {
		blk.References++

		return -1, nil
	}

	return f.blockAccess.writeBlock(sum, block)
}

func (f *FS) flushTOC() error {
	buf := getBlockBuf(f.toc.Size())

	tlen, err := f.toc.MarshalTo(buf)
	if err != nil {
		return err
	}

	set, err := f.writeAsBlocks(bytes.NewReader(buf[:tlen]))
	if err != nil {
		return err
	}

	putBlockBuf(buf)

	buf = getBlockBuf(set.Size())
	defer putBlockBuf(buf)

	slen, err := set.MarshalTo(buf)
	if err != nil {
		return err
	}

	of, err := os.Create(filepath.Join(f.root, f.tocPath))
	if err != nil {
		return err
	}

	defer of.Close()

	buf, err = f.blockAccess.writeTransform(buf[:slen])
	if err != nil {
		return err
	}

	tocSum := blake2b.Sum256(buf)

	f.tocHeader.Sum = tocSum[:]

	hdata := make([]byte, 256)

	hlen, err := f.tocHeader.MarshalTo(hdata[1:])
	if hlen > 255 {
		return fmt.Errorf("header too large! %d > 255", hlen)
	}

	hdata[0] = byte(hlen)

	_, err = of.Write(hdata)
	if err != nil {
		return err
	}

	_, err = of.Write(buf)
	return err
}

func (f *FS) flushBlockTOC() error {
	buf := buffers.Get().([]byte)

	tocSize := f.blocks.Size()

	if len(buf) < tocSize {
		buf = make([]byte, tocSize+64)
	}

	defer buffers.Put(buf)

	len, err := f.blocks.MarshalTo(buf)
	if err != nil {
		return err
	}

	of, err := os.Create(filepath.Join(f.root, "blocks.dat"))
	if err != nil {
		return err
	}

	defer of.Close()

	_, err = of.Write(buf[:len])
	return err
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

	dataSum := blake2b.Sum256(data[256:])

	if !bytes.Equal(fheader.Sum, dataSum[:]) {
		return ErrCorruptTOC
	}

	buf, err := f.blockAccess.readTransform(data[256:])
	if err != nil {
		return err
	}

	var set format.BlockSet

	err = set.Unmarshal(buf)
	if err != nil {
		return err
	}

	data, err = f.blockAccess.readSet(&set)
	if err != nil {
		return err
	}

	return f.toc.Unmarshal(data)
}

func (f *FS) readBlocksTOC() error {
	of, err := os.Open(filepath.Join(f.root, "blocks.dat"))
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

type blockReader struct {
	f      *FS
	blocks []*format.Block
	cur    *bytes.Reader
	clz    io.Reader
}

func (b *blockReader) Read(buf []byte) (int, error) {
	if b.cur == nil {
		block := b.blocks[0]
		b.blocks = b.blocks[1:]

		data, err := b.f.blockAccess.readBlock(block.Id)
		if err != nil {
			return 0, err
		}

		b.cur = bytes.NewReader(data)
		return b.cur.Read(buf)
	}

	n, err := b.cur.Read(buf)
	if err == nil {
		return n, nil
	}

	b.cur = nil

	if n > 0 {
		buf = buf[n:]
	}

	if len(b.blocks) == 0 {
		return n, io.EOF
	}

	block := b.blocks[0]
	b.blocks = b.blocks[1:]

	data, err := b.f.blockAccess.readBlock(block.Id)
	if err != nil {
		return 0, err
	}

	b.cur = bytes.NewReader(data)

	m, err := b.cur.Read(buf)
	return n + m, err
}

func (b *blockReader) WriteTo(w io.Writer) (int64, error) {
	var total int64

	if b.cur != nil {
		n, err := io.Copy(w, b.cur)
		if err != nil {
			return 0, err
		}

		total += int64(n)
	}

	for _, blk := range b.blocks {
		data, err := b.f.blockAccess.readBlock(blk.Id)
		if err != nil {
			return total, err
		}

		n, err := w.Write(data)
		if err != nil {
			return total, err
		}

		total += int64(n)
	}

	b.blocks = nil

	return total, nil
}

func (f *FS) ReaderFor(path string) (io.Reader, error) {
	entry, ok := f.toc.Paths[path]
	if !ok {
		return nil, os.ErrNotExist
	}

	return &blockReader{f: f, blocks: entry.Blocks.Blocks}, nil
}

type blockWriter struct {
	f     *FS
	path  string
	entry *format.Entry

	pr   *io.PipeReader
	pw   *io.PipeWriter
	bg   bool
	werr chan error
}

func (b *blockWriter) consume() {
	_, err := b.f.writeFile(b.path, b.pr, b.entry)
	b.werr <- err
}

func (b *blockWriter) Write(data []byte) (int, error) {
	if !b.bg {
		b.pr, b.pw = io.Pipe()
		go b.consume()
		b.bg = true
	}

	return b.pw.Write(data)
}

func (b *blockWriter) ReadFrom(r io.Reader) (int64, error) {
	if b.bg {
		// mixed use, have to keep using the background version
		return io.Copy(b.pw, r)
	}

	return b.f.writeFile(b.path, r, b.entry)
}

func (b *blockWriter) Close() error {
	if b.bg {
		b.pw.Close()
		return <-b.werr
	}

	return nil
}

func (f *FS) WriterFor(path string) (io.WriteCloser, error) {
	entry, ok := f.toc.Paths[path]
	if !ok {
		entry = &format.Entry{}
	}

	bw := &blockWriter{
		f:     f,
		path:  path,
		entry: entry,
		werr:  make(chan error, 1),
	}

	return bw, nil
}

var ErrCorruptFile = errors.New("corrupt file detected")

func (f *FS) RemoveFile(path string) error {
	entry, ok := f.toc.Paths[path]
	if !ok {
		return os.ErrNotExist
	}

	for _, blk := range entry.Blocks.Blocks {
		bi, ok := f.blocks.FindBlock(blk.Id)
		if ok != true {
			return ErrCorruptFile
		}

		bi.References--
	}

	return f.gcBlocks()
}

func (f *FS) gcBlocks() error {
	fanChecks := map[string]struct{}{}

	for _, blk := range f.blocks.Blocks {
		if blk.References == 0 {
			id := hex.EncodeToString(blk.Id)
			fanPath := filepath.Join(f.root, "blocks", id[:6])
			fanChecks[fanPath] = struct{}{}

			err := os.Remove(filepath.Join(fanPath, id))
			if err != nil {
				return err
			}
		}
	}

	for path, _ := range fanChecks {
		f, err := os.Open(path)
		if err == nil {
			if _, err = f.Readdir(1); err == io.EOF {
				err = os.Remove(path)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (fs *FS) CreateSnapshot(name string) error {
	for _, bi := range fs.blocks.Blocks {
		bi.References++
	}

	dir := filepath.Join(fs.root, "snapshots")
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	err = fs.flushTOC()
	if err != nil {
		return err
	}

	t, err := os.Open(filepath.Join(fs.root, fs.tocPath))
	if err != nil {
		return err
	}

	defer t.Close()

	f, err := os.Create(filepath.Join(fs.root, "snapshots", name+".dat"))
	if err != nil {
		return err
	}

	defer f.Close()

	_, err = io.Copy(f, t)
	return err
}

func (fs *FS) ReadSnapshot(name string) (*FS, error) {
	sfs := &FS{
		root:    fs.root,
		tocPath: filepath.Join("snapshots", name+".dat"),
	}

	sfs.blockAccess = fs.blockAccess

	sfs.toc = &format.TOC{
		Paths: make(map[string]*format.Entry),
	}

	sfs.blocks = fs.blocks

	err := sfs.readTOC()
	if err != nil {
		return nil, err
	}

	return sfs, nil
}
