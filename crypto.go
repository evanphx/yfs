package yfs

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/curve25519"
)

type Key struct {
	priv, pub [32]byte
}

func GenerateKey() *Key {
	var pubkey, privkey [32]byte

	if _, err := io.ReadFull(rand.Reader, privkey[:]); err != nil {
		panic(err)
	}

	curve25519.ScalarBaseMult(&pubkey, &privkey)
	return &Key{privkey, pubkey}
}

type cryptWriteWrapper struct {
	key *Key
}

func cipherChaChaPoly(k [32]byte) aeadCipher {
	c, err := chacha20poly1305.New(k[:])
	if err != nil {
		panic(err)
	}

	return aeadCipher{
		c,
		func(n uint64) []byte {
			var nonce [12]byte
			binary.LittleEndian.PutUint64(nonce[4:], n)
			return nonce[:]
		},
	}
}

type aeadCipher struct {
	cipher.AEAD
	nonce func(uint64) []byte
}

func (c aeadCipher) Encrypt(out []byte, n uint64, plaintext []byte) ([]byte, []byte) {
	nonce := c.nonce(n)
	return c.Seal(out, nonce, plaintext, nil), nonce
}

func (c aeadCipher) Decrypt(out, nonce []byte, ciphertext []byte) ([]byte, error) {
	return c.Open(out, nonce, ciphertext, nil)
}

func newCryptWriter(key *Key) (*cryptWriter, error) {
	temp := GenerateKey()

	var dst, in, base [32]byte
	copy(in[:], temp.priv[:])
	copy(base[:], key.pub[:])
	curve25519.ScalarMult(&dst, &in, &base)

	cipher, err := chacha20poly1305.New(dst[:])
	if err != nil {
		return nil, err
	}

	return &cryptWriter{
		pkey:   key,
		temp:   temp,
		key:    dst[:],
		cipher: cipher,
	}, nil
}

func nonceBytes(n uint64) []byte {
	var nonce [12]byte
	binary.LittleEndian.PutUint64(nonce[4:], n)
	return nonce[:]
}

type cryptWriter struct {
	pkey *Key
	temp *Key

	key []byte

	cipher cipher.AEAD
	nonce  uint64
}

const CryptoOverhead = 32 + 12

func (c *cryptWriter) Transform(block []byte) ([]byte, []byte, error) {
	c.nonce++

	out := getBlockBuf(len(block) + CryptoOverhead + c.cipher.Overhead())

	copy(out, c.temp.pub[:])

	nonce := nonceBytes(c.nonce)

	// log.Printf("encryption key: %s", spew.Sdump(c.key))

	copy(out[32:], nonce)

	// log.Printf("encryption nonce: %s", spew.Sdump(nonce))

	space := out[CryptoOverhead:]

	ct := c.cipher.Seal(space[:0], nonce, block, nil)

	result := out[:CryptoOverhead+len(ct)]

	// log.Printf("encryption ciphertext: %s", spew.Sdump(ct))

	return result, out, nil
}

type cryptReader struct {
	key *Key

	prevPub []byte
	prevKey []byte
}

func (c *cryptReader) Transform(block []byte) ([]byte, []byte, error) {
	out := getBlockBuf(len(block) + CryptoOverhead)

	var key []byte

	if c.prevPub != nil && bytes.Equal(c.prevPub, block[:32]) {
		key = c.prevKey
	} else {
		var dst, in, base [32]byte
		copy(in[:], c.key.priv[:])
		copy(base[:], block[:32])
		curve25519.ScalarMult(&dst, &in, &base)

		key = dst[:]
		c.prevKey = key
		c.prevPub = block[:32]
	}

	// log.Printf("decryption key: %s", spew.Sdump(key))
	// log.Printf("decryption nonce: %s", spew.Sdump(block[32:44]))
	// log.Printf("decryption ciphertext: %s", spew.Sdump(block[CryptoOverhead:]))

	cipher, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, nil, err
	}

	pt, err := cipher.Open(out[:0], block[32:44], block[CryptoOverhead:], nil)
	if err != nil {
		return nil, nil, err
	}

	return pt, out, nil
}

func WithEncryption(key *Key) func(*FS) {
	return func(fs *FS) {
		cw, err := newCryptWriter(key)
		if err != nil {
			panic(err)
		}

		fs.blockAccess.write.encryption = cw
		fs.blockAccess.read.encryption = &cryptReader{key: key}
	}
}
