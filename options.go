package yfs

import "path/filepath"

type Option func(*FS)

func WithHead(name string) Option {
	return Option(func(f *FS) {
		f.tocPath = filepath.Join("heads", name)
	})
}

func WithSettingsFrom(parent *FS) Option {
	return Option(func(f *FS) {
		f.blockAccess.read = parent.blockAccess.read
		f.blockAccess.write = parent.blockAccess.write
	})
}
