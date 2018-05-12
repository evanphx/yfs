package yfs

import (
	"bytes"
	"io"

	"github.com/evanphx/yfs/format"
)

type blockReader struct {
	t      *Txn
	blocks []*format.Block
	cur    *bytes.Reader
	clz    io.Reader
}

func (b *blockReader) Read(buf []byte) (int, error) {
	if b.cur == nil {
		block := b.blocks[0]
		b.blocks = b.blocks[1:]

		data, err := b.t.blockAccess.readBlock(block.Id)
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

	data, err := b.t.blockAccess.readBlock(block.Id)
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
		data, err := b.t.blockAccess.readBlock(blk.Id)
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

type blockWriter struct {
	t     *Txn
	path  string
	entry *format.Entry

	pr     *io.PipeReader
	pw     *io.PipeWriter
	bg     bool
	commit bool
	werr   chan error
}

func (b *blockWriter) consume() {
	_, err := b.t.writeFile(b.path, b.pr, b.entry)
	if err != nil {
		b.werr <- err
		return
	}

	if b.commit {
		err = b.t.Commit()
		if err != nil {
			b.werr <- err
			return
		}
	}

	b.werr <- nil
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

	n, err := b.t.writeFile(b.path, r, b.entry)
	if err != nil {
		return n, err
	}

	if b.commit {
		return n, b.t.Commit()
	}

	return 0, nil
}

func (b *blockWriter) Close() error {
	if b.bg {
		b.pw.Close()
		return <-b.werr
	}

	return nil
}
