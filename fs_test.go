package yfs

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/neko"
)

func TestFS(t *testing.T) {
	n := neko.Modern(t)

	root, err := ioutil.TempDir("", "yfs")
	require.NoError(t, err)
	defer os.RemoveAll(root)

	path := filepath.Join(root, "test")
	n.Setup(func() {
		os.RemoveAll(path)
		os.MkdirAll(path, 0755)
	})

	helloPath := filepath.Join(root, "hello.txt")

	hello, err := os.Create(helloPath)
	require.NoError(t, err)

	hello.WriteString("hello")
	require.NoError(t, hello.Close())

	n.It("adds files and stores them as blocks", func(t *testing.T) {
		fs, err := NewFS(path)
		require.NoError(t, err)

		err = fs.WriteFile("foo", strings.NewReader("hello"))
		require.NoError(t, err)

		fs2, err := NewFS(path)
		require.NoError(t, err)

		r, err := fs2.ReadFile("foo")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, "hello", string(data))
	})

	n.It("dedups common blocks between files", func(t *testing.T) {
		com := make([]byte, AverageBlock)

		n, err := rand.Read(com)
		require.NoError(t, err)
		require.Equal(t, n, AverageBlock)

		fs, err := NewFS(path)
		require.NoError(t, err)

		fooData := make([]byte, len(com))
		copy(fooData, com)

		fooData = append(fooData, []byte("hello")...)

		err = fs.WriteFile("foo", bytes.NewReader(fooData))
		require.NoError(t, err)

		barData := make([]byte, len(com))
		copy(barData, com)

		barData = append(barData, []byte("goodbye")...)

		err = fs.WriteFile("bar", bytes.NewReader(barData))
		require.NoError(t, err)

		fooBlocks := fs.toc.Paths["foo"]
		barBlocks := fs.toc.Paths["bar"]

		common := len(fooBlocks.Blocks) - 1

		assert.Equal(t, fooBlocks.Blocks[:common], barBlocks.Blocks[:common])

		for _, blk := range fooBlocks.Blocks[:common] {
			blkinfo, ok := fs.blocks.FindBlock(blk.Id)
			require.True(t, ok)

			assert.Equal(t, int64(2), blkinfo.References)
		}
	})

	n.It("can pickup the details about a file on the filesystem", func(t *testing.T) {
		fs, err := NewFS(path)
		require.NoError(t, err)

		f, err := os.Open(helloPath)
		require.NoError(t, err)

		defer f.Close()

		err = fs.CopyFile("hello.txt", f)
		require.NoError(t, err)
	})

	n.It("compresses blocks", func(t *testing.T) {
		fs, err := NewFS(path, WithLZ4())
		require.NoError(t, err)

		zeros := append(make([]byte, AverageBlock*2), []byte("hello")...)

		err = fs.WriteFile("foo", bytes.NewReader(zeros))
		require.NoError(t, err)

		fs2, err := NewFS(path, WithLZ4())
		require.NoError(t, err)

		r, err := fs2.ReadFile("foo")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, "hello", string(data[AverageBlock*2:]))

		assert.True(t, fs2.blocks.Blocks[0].CompSize < AverageBlock)
	})

	n.It("deletes blocks when they are no longer referenced", func(t *testing.T) {
		fs, err := NewFS(path)
		require.NoError(t, err)

		err = fs.WriteFile("foo", strings.NewReader("hello"))
		require.NoError(t, err)

		fds, err := ioutil.ReadDir(filepath.Join(path, "blocks"))
		require.NoError(t, err)

		assert.Equal(t, len(fds), 1)

		err = fs.RemoveFile("foo")
		require.NoError(t, err)

		fds, err = ioutil.ReadDir(filepath.Join(path, "blocks"))
		require.NoError(t, err)

		assert.Equal(t, len(fds), 0)
	})

	n.It("can snapshot the filesystem", func(t *testing.T) {
		fs, err := NewFS(path)
		require.NoError(t, err)

		err = fs.WriteFile("foo", strings.NewReader("hello"))
		require.NoError(t, err)

		err = fs.CreateSnapshot("snap1")
		require.NoError(t, err)

		err = fs.RemoveFile("foo")
		require.NoError(t, err)

		fds, err := ioutil.ReadDir(filepath.Join(path, "blocks"))
		require.NoError(t, err)

		assert.Equal(t, len(fds), 1)

		snap, err := fs.ReadSnapshot("snap1")
		require.NoError(t, err)

		r, err := snap.ReadFile("foo")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, "hello", string(data))
	})

	n.It("encrypts blocks", func(t *testing.T) {
		key := GenerateKey()

		fs, err := NewFS(path, WithEncryption(key))
		require.NoError(t, err)

		zeros := []byte("hello")

		err = fs.WriteFile("foo", bytes.NewReader(zeros))
		require.NoError(t, err)

		fs2, err := NewFS(path)
		require.NoError(t, err)

		r, err := fs2.ReadFile("foo")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.NotEqual(t, "hello", string(data))

		fs3, err := NewFS(path, WithEncryption(key))
		require.NoError(t, err)

		r, err = fs3.ReadFile("foo")
		require.NoError(t, err)

		data, err = ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, "hello", string(data))
	})

	n.It("can encrypt many blocks", func(t *testing.T) {
		com := make([]byte, AverageBlock*100)

		_, err := rand.Read(com)
		require.NoError(t, err)

		key := GenerateKey()

		fs, err := NewFS(path, WithEncryption(key))
		require.NoError(t, err)

		err = fs.WriteFile("foo", bytes.NewReader(com))
		require.NoError(t, err)

		fooBlocks := fs.toc.Paths["foo"]

		assert.True(t, len(fooBlocks.Blocks) > 1)

		fs2, err := NewFS(path, WithEncryption(key))
		require.NoError(t, err)

		r, err := fs2.ReadFile("foo")
		require.NoError(t, err)

		result, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, com, result)
	})

	n.It("can encrypt and compress many blocks", func(t *testing.T) {
		com := make([]byte, AverageBlock*100)

		_, err := rand.Read(com)
		require.NoError(t, err)

		key := GenerateKey()

		fs, err := NewFS(path, WithEncryption(key), WithLZ4())
		require.NoError(t, err)

		err = fs.WriteFile("foo", bytes.NewReader(com))
		require.NoError(t, err)

		fooBlocks := fs.toc.Paths["foo"]

		assert.True(t, len(fooBlocks.Blocks) > 1)

		fs2, err := NewFS(path, WithEncryption(key), WithLZ4())
		require.NoError(t, err)

		r, err := fs2.ReadFile("foo")
		require.NoError(t, err)

		result, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, com, result)
	})

	n.Meow()
}
