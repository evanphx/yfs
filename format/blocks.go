package format

import bytes "bytes"

func (bs *BlockTOC) FindBlock(id []byte) (*BlockInfo, bool) {
	for _, b := range bs.Blocks {
		if bytes.Equal(id, b.Id) {
			return b, true
		}
	}

	return nil, false
}
