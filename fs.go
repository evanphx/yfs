package yfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

	return f.writeFile(path, of, ent)
}

func (f *FS) WriteFile(path string, r io.Reader) error {
	return f.writeFile(path, r, &format.Entry{})
}

func (f *FS) writeFile(path string, r io.Reader, ent *format.Entry) error {
	backing := getBlockBuf(0)

	buf := bytes.NewBuffer(backing[:0])

	defer putBlockBuf(backing)

	var total int64

	fh, err := blake2b.New256(nil)
	if err != nil {
		return err
	}

	c := rabin.NewChunker(table, io.TeeReader(r, buf), MinBlock, AverageBlock, MaxBlock)

	var blocks []*format.Block

	for i := 0; ; i++ {
		len, err := c.Next()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		total += int64(len)

		h, err := blake2b.New256(nil)
		if err != nil {
			return err
		}

		block := buf.Next(len)

		_, err = h.Write(block)
		if err != nil {
			return err
		}

		sum := h.Sum(nil)

		fh.Write(sum[:])

		clen, err := f.writeBlock(sum[:], block)
		if err != nil {
			return err
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

	ent.ByteSize = total
	ent.Type = File
	ent.Hash = fhSum[:]
	ent.Blocks = blocks

	f.toc.Paths[path] = ent

	err = f.flushBlockTOC()
	if err != nil {
		return err
	}

	return f.flushTOC()
}

func (f *FS) writeBlock(sum []byte, block []byte) (int64, error) {
	if blk, ok := f.blocks.FindBlock(sum); ok {
		blk.References++

		return -1, nil
	}

	return f.blockAccess.writeBlock(sum, block)
}

func (f *FS) flushTOC() error {
	buf := buffers.Get().([]byte)

	tocSize := f.toc.Size()

	if len(buf) < tocSize {
		buf = make([]byte, tocSize+64)
	}

	defer buffers.Put(buf)

	len, err := f.toc.MarshalTo(buf)
	if err != nil {
		return err
	}

	of, err := os.Create(filepath.Join(f.root, f.tocPath))
	if err != nil {
		return err
	}

	defer of.Close()

	_, err = of.Write(buf[:len])
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

func (f *FS) readTOC() error {
	of, err := os.Open(filepath.Join(f.root, f.tocPath))
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

	return f.toc.Unmarshal(buf[:tocSize])
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
	cur    io.Reader
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

func (f *FS) ReadFile(path string) (io.Reader, error) {
	entry, ok := f.toc.Paths[path]
	if !ok {
		return nil, os.ErrNotExist
	}

	return &blockReader{f: f, blocks: entry.Blocks}, nil
}

var ErrCorruptFile = errors.New("corrupt file detected")

func (f *FS) RemoveFile(path string) error {
	entry, ok := f.toc.Paths[path]
	if !ok {
		return os.ErrNotExist
	}

	for _, blk := range entry.Blocks {
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