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
	"syscall"

	"github.com/aclements/go-rabin/rabin"
	"github.com/evanphx/yfs/format"
	"github.com/golang/crypto/blake2b"
)

type Txn struct {
	f     *FS
	write bool

	root    string
	tocPath string

	toc       *format.TOC
	tocBlocks *format.BlockTOC

	blocks *format.BlockTOC

	tocHeader format.TOCHeader

	blockAccess blockAccess

	updates      *format.TOC
	blockUpdates *format.BlockTOC
	removal      []string

	tocSet *format.BlockSet
}

var ErrReadOnly = errors.New("only read operations allowed")

func (t *Txn) entryFor(path string) (*format.Entry, bool) {
	entry, ok := t.updates.Paths[path]
	if ok {
		return entry, true
	}

	entry, ok = t.toc.Paths[path]
	return entry, ok
}

func (t *Txn) ReaderFor(path string) (io.Reader, error) {
	entry, ok := t.entryFor(path)
	if !ok {
		return nil, os.ErrNotExist
	}

	return &blockReader{t: t, blocks: entry.Blocks.Blocks}, nil
}

func (t *Txn) WriterFor(path string) (io.WriteCloser, error) {
	if !t.write {
		return nil, ErrReadOnly
	}

	entry, ok := t.entryFor(path)
	if !ok {
		entry = &format.Entry{}
	}

	bw := &blockWriter{
		t:     t,
		path:  path,
		entry: entry,
		werr:  make(chan error, 1),
	}

	return bw, nil
}

func (t *Txn) RemoveFile(path string) error {
	if !t.write {
		return ErrReadOnly
	}

	_, ok := t.entryFor(path)
	if !ok {
		return os.ErrNotExist
	}

	t.removal = append(t.removal, path)

	return nil
}

func (t *Txn) WriteFile(path string, r io.Reader) error {
	_, err := t.writeFile(path, r, &format.Entry{})
	return err
}

func (t *Txn) CopyFile(path string, of *os.File) error {
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

	_, err = t.writeFile(path, of, ent)
	if err != nil {
		return err
	}

	return t.flushTOC()
}

func (t *Txn) CreateSnapshot(name string) error {
	if !t.write {
		return ErrReadOnly
	}

	err := t.flushTOC()
	if err != nil {
		return err
	}

	s, err := os.Open(filepath.Join(t.root, t.tocPath))
	if err != nil {
		return err
	}

	defer s.Close()

	f, err := os.Create(filepath.Join(t.root, "heads", name))
	if err != nil {
		return err
	}

	defer f.Close()

	_, err = io.Copy(f, s)
	return err
}

func (t *Txn) Commit() error {
	if !t.write {
		return nil
	}

	defer t.f.txnlock.Unlock()

	err := t.flushTOC()
	if err != nil {
		return err
	}

	err = t.gcBlocks()
	if err != nil {
		return err
	}

	return t.flushBlockTOC()
}

func (t *Txn) writeBlock(bid BlockId, block []byte) (int64, error) {
	return t.blockAccess.writeBlock(bid, block)
}

func (t *Txn) writeAsBlocks(r io.Reader) (*format.BlockSet, error) {
	backing := getBlockBuf(0)

	buf := bytes.NewBuffer(backing[:0])

	defer putBlockBuf(backing)

	var total int64

	fh, err := blake2b.New256(nil)
	if err != nil {
		return nil, err
	}

	c := rabin.NewChunker(table, io.TeeReader(r, buf), MinBlock, AverageBlock, MaxBlock)

	var (
		blocks  []*format.Block
		updates []*format.BlockInfo
	)

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

		bid := BlockId(sum[:])

		blocks = append(blocks, &format.Block{
			Id: bid,
		})

		// if this is an existing block, then inc our internal
		// refs to it.
		if info, ok := t.lookupTOCBlock(bid); ok {
			info.References++
			continue
		}

		clen, err := t.writeBlock(bid, block)
		if err != nil {
			return nil, err
		}

		info := &format.BlockInfo{
			Id:         bid,
			ByteSize:   int64(len),
			CompSize:   clen,
			References: 1,
		}

		t.addTOCBlock(info)

		updates = append(updates, info)

	}

	t.f.blockslock.Lock()
	t.blocks.Blocks = append(t.blocks.Blocks, updates...)
	t.f.blockslock.Unlock()

	fhSum := fh.Sum(nil)

	set := &format.BlockSet{
		Blocks:   blocks,
		Sum:      fhSum[:],
		ByteSize: total,
	}

	return set, nil
}

func (t *Txn) writeFile(path string, r io.Reader, ent *format.Entry) (int64, error) {
	set, err := t.writeAsBlocks(r)
	if err != nil {
		return 0, err
	}

	ent.ByteSize = set.ByteSize
	ent.Type = File
	ent.Hash = set.Sum
	ent.Blocks = set

	t.updates.Paths[path] = ent

	return set.ByteSize, nil
}

func (t *Txn) lookupTOCBlock(bid BlockId) (*format.BlockInfo, bool) {
	for _, info := range t.tocBlocks.Blocks {
		if bytes.Equal(info.Id, bid) {
			return info, true
		}
	}

	return nil, false
}

func (t *Txn) addTOCBlock(info *format.BlockInfo) {
	t.tocBlocks.Blocks = append(t.tocBlocks.Blocks, info)
}

func (t *Txn) flushTOC() error {
	t.f.toclock.Lock()
	defer t.f.toclock.Unlock()

	for path, entry := range t.updates.Paths {
		t.toc.Paths[path] = entry
	}

	for _, path := range t.removal {
		if entry, ok := t.toc.Paths[path]; ok {
			for _, blk := range entry.Blocks.Blocks {
				if info, ok := t.tocBlocks.FindBlock(blk.Id); ok {
					info.References--

					if info.References == 0 {
						t.tocBlocks.RemoveBlock(BlockId(blk.Id))
					}
				}
			}
		}

		delete(t.toc.Paths, path)
	}

	t.updates = &format.TOC{
		Paths: make(map[string]*format.Entry),
	}

	buf := getBlockBuf(t.toc.Size())

	tlen, err := t.toc.MarshalTo(buf)
	if err != nil {
		return err
	}

	set, err := t.writeAsBlocks(bytes.NewReader(buf[:tlen]))
	if err != nil {
		return err
	}

	if t.tocSet != nil {
		for _, blk := range t.tocSet.Blocks {
			if info, ok := t.tocBlocks.FindBlock(blk.Id); ok {
				info.References--

				if info.References == 0 {
					t.tocBlocks.RemoveBlock(BlockId(blk.Id))
				}
			}
		}
	}

	t.f.tocSet = set
	t.tocSet = set

	putBlockBuf(buf)

	buf = getBlockBuf(set.Size())
	defer putBlockBuf(buf)

	slen, err := set.MarshalTo(buf)
	if err != nil {
		return err
	}

	of, err := os.Create(filepath.Join(t.root, t.tocPath))
	if err != nil {
		return err
	}

	defer of.Close()

	buf, err = t.blockAccess.writeTransform(buf[:slen])
	if err != nil {
		return err
	}

	tocSum := blake2b.Sum256(buf)

	t.tocHeader.Sum = tocSum[:]
	t.tocHeader.TocSize = int64(len(buf))

	// Now marshal the blockTOC

	bbuf := getBlockBuf(t.tocBlocks.Size())

	n, err := t.tocBlocks.MarshalTo(bbuf)
	if err != nil {
		return err
	}

	bdata, err := t.blockAccess.writeTransform(bbuf[:n])
	if err != nil {
		return err
	}

	t.tocHeader.BlocksSize = int64(len(bdata))

	hdata := make([]byte, 256)

	hlen, err := t.tocHeader.MarshalTo(hdata[1:])
	if hlen > 247 {
		return fmt.Errorf("header too large! %d > 247", hlen)
	}

	hdata[0] = byte(hlen)

	_, err = of.Write(hdata)
	if err != nil {
		return err
	}

	_, err = of.Write(buf)
	if err != nil {
		return err
	}

	_, err = of.Write(bdata)
	return err
}

func (t *Txn) flushBlockTOC() error {
	buf := buffers.Get().([]byte)

	tocSize := t.blocks.Size()

	if len(buf) < tocSize {
		buf = make([]byte, tocSize+64)
	}

	defer buffers.Put(buf)

	len, err := t.blocks.MarshalTo(buf)
	if err != nil {
		return err
	}

	of, err := os.Create(filepath.Join(t.root, "blocks.idx"))
	if err != nil {
		return err
	}

	defer of.Close()

	_, err = of.Write(buf[:len])
	return err
}

func (t *Txn) gcBlocks() error {
	var (
		fanChecks = map[string]struct{}{}
		foundRefs = map[string]int64{}
	)

	heads, err := ioutil.ReadDir(filepath.Join(t.root, "heads"))
	if err != nil {
		return err
	}

	for _, head := range heads {
		path := filepath.Join(t.root, "heads", head.Name())
		_, _, blocks, err := t.f.unmarshalTOC(path)
		if err != nil {
			return err
		}

		for _, blk := range blocks.Blocks {
			foundRefs[BlockId(blk.Id).String()]++
		}
	}

	for _, blk := range t.blocks.Blocks {
		id := hex.EncodeToString(blk.Id)

		if foundRefs[id] == 0 {
			fanPath := filepath.Join(t.root, "blocks", id[:6])
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
