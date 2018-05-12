package yfs

import (
	"bytes"
	"io/ioutil"

	"github.com/evanphx/yfs/format"
	"github.com/golang/crypto/blake2b"
)

func (f *FS) unmarshalTOC(path string) (*format.TOCHeader, *format.TOC, *format.BlockTOC, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, nil, nil, err
	}

	var fheader format.TOCHeader

	sz := data[0]

	err = fheader.Unmarshal(data[1 : 1+sz])
	if err != nil {
		return nil, nil, nil, err
	}

	if f.tocHeader.Compressed != fheader.Compressed {
		return nil, nil, nil, ErrCompressionMismatch
	}

	if !bytes.Equal(f.tocHeader.KeyId, fheader.KeyId) {
		return nil, nil, nil, ErrWrongEncryptionKey
	}

	dataSum := blake2b.Sum256(data[256 : 256+fheader.TocSize])

	if !bytes.Equal(fheader.Sum, dataSum[:]) {
		return nil, nil, nil, ErrCorruptTOC
	}

	var (
		tocSize   = fheader.TocSize
		blockSize = fheader.BlocksSize
	)

	buf, err := f.blockAccess.readTransform(data[256 : 256+tocSize])
	if err != nil {
		return nil, nil, nil, err
	}

	var set format.BlockSet

	err = set.Unmarshal(buf)
	if err != nil {
		return nil, nil, nil, err
	}

	setData, err := f.blockAccess.readSet(&set)
	if err != nil {
		return nil, nil, nil, err
	}

	var toc format.TOC

	err = toc.Unmarshal(setData)
	if err != nil {
		return nil, nil, nil, err
	}

	var bs format.BlockTOC

	bsData := data[256+tocSize : 256+tocSize+blockSize]

	data, err = f.blockAccess.readTransform(bsData)
	if err != nil {
		return nil, nil, nil, err
	}

	err = bs.Unmarshal(data)
	if err != nil {
		return nil, nil, nil, err
	}

	return &fheader, &toc, &bs, nil
}
