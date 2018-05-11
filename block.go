package yfs

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/evanphx/yfs/format"
	"github.com/golang/crypto/blake2b"
)

type blockTransform interface {
	Transform(src []byte) ([]byte, []byte, error)
}

type blockAccess struct {
	root string

	write struct {
		compression blockTransform
		encryption  blockTransform
	}

	read struct {
		compression blockTransform
		encryption  blockTransform
	}
}

func (ba *blockAccess) writeTransform(block []byte) ([]byte, error) {
	if ba.write.compression != nil {
		out, _, err := ba.write.compression.Transform(block)
		if err != nil {
			return nil, err
		}

		block = out
	}

	if ba.write.encryption != nil {
		out, _, err := ba.write.encryption.Transform(block)
		if err != nil {
			return nil, err
		}

		block = out
	}

	return block, nil
}

func (ba *blockAccess) writeBlock(sum []byte, block []byte) (int64, error) {
	id := hex.EncodeToString(sum)

	dir := filepath.Join(ba.root, id[:6])
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return 0, err
	}

	path := filepath.Join(dir, id)

	block, err = ba.writeTransform(block)
	if err != nil {
		return 0, err
	}

	of, err := os.Create(path)
	if err != nil {
		return 0, err
	}

	defer of.Close()

	_, err = of.Write(block)
	if err != nil {
		return 0, err
	}

	stat, err := of.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func (ba *blockAccess) readTransform(block []byte) ([]byte, error) {
	if ba.read.encryption != nil {
		out, _, err := ba.read.encryption.Transform(block)
		if err != nil {
			return nil, err
		}

		block = out
	}

	if ba.read.compression != nil {
		out, _, err := ba.read.compression.Transform(block)
		if err != nil {
			return nil, err
		}

		block = out
	}

	return block, nil
}

var ErrCorruptBlock = errors.New("corrupt block detected")

func (ba *blockAccess) readBlock(sum []byte) ([]byte, error) {
	hid := hex.EncodeToString(sum)

	path := filepath.Join(ba.root, hid[:6], hid)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	rawBlock, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	data, err := ba.readTransform(rawBlock)
	if err != nil {
		return nil, err
	}

	seenSum := blake2b.Sum256(data)

	if !bytes.Equal(sum, seenSum[:]) {
		return data, ErrCorruptBlock
	}

	return data, nil
}

func (ba *blockAccess) readSet(set *format.BlockSet) ([]byte, error) {
	var buf bytes.Buffer

	for _, blk := range set.Blocks {
		data, err := ba.readBlock(blk.Id)
		if err != nil {
			return nil, err
		}

		buf.Write(data)
	}

	return buf.Bytes(), nil
}
