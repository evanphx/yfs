package yfs

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
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

func (ba *blockAccess) writeBlock(sum []byte, block []byte) (int64, error) {
	id := hex.EncodeToString(sum)

	dir := filepath.Join(ba.root, id[:6])
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return 0, err
	}

	path := filepath.Join(dir, id)

	if ba.write.compression != nil {
		out, temp, err := ba.write.compression.Transform(block)
		if err != nil {
			return 0, err
		}

		if temp != nil {
			defer putBlockBuf(temp)
		}

		block = out
	}

	if ba.write.encryption != nil {
		out, temp, err := ba.write.encryption.Transform(block)
		if err != nil {
			return 0, err
		}

		if temp != nil {
			defer putBlockBuf(temp)
		}

		block = out
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

	if ba.read.encryption != nil {
		out, _, err := ba.read.encryption.Transform(rawBlock)
		if err != nil {
			return nil, err
		}

		rawBlock = out
	}

	if ba.read.compression != nil {
		out, _, err := ba.read.compression.Transform(rawBlock)
		if err != nil {
			return nil, err
		}

		rawBlock = out
	}

	return rawBlock, nil
}
