package yfs

import "github.com/pierrec/lz4"

type lz4Writer struct{}

func (l lz4Writer) Transform(block []byte) ([]byte, []byte, error) {
	out := getBlockBuf(len(block) + 2)

	clen, err := lz4.CompressBlock(block, out[2:], 0)
	if err != nil {
		return nil, nil, err
	}

	// Welp, not compressable, bummer.
	if clen == 0 {
		copy(out[2:], block)
		out[0] = 0
		out[1] = 0
		return out[:len(block)+2], out, nil
	}

	out[0] = byte(len(block) & 0xff)
	out[1] = byte((len(block) >> 8) & 0xff)

	return out[:clen+2], out, nil
}

type lz4Reader struct{}

func (l lz4Reader) Transform(block []byte) ([]byte, []byte, error) {
	var plen uint16 = uint16(block[0]) | (uint16(block[1]) << 8)

	block = block[2:]

	// It wasn't compressed
	if plen == 0 {
		return block, nil, nil
	}

	out := getBlockBuf(int(plen))

	len, err := lz4.UncompressBlock(block, out, 0)
	if err != nil {
		return nil, nil, err
	}

	return out[:len], out, nil
}

func WithLZ4() func(f *FS) {
	return func(f *FS) {
		f.tocHeader.Compressed = true
		f.blockAccess.write.compression = lz4Writer{}
		f.blockAccess.read.compression = lz4Reader{}
	}
}
