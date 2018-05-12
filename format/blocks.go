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

func (bs *BlockTOC) RemoveBlock(id []byte) bool {
	for n, b := range bs.Blocks {
		if bytes.Equal(id, b.Id) {
			bs.Blocks = append(bs.Blocks[:n], bs.Blocks[n+1:]...)
			return true
		}
	}

	return false
}
