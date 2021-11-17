package proto

import (
	"encoding/binary"
	"io"
)

// Buffer implements ClickHouse binary protocol encoding.
type Buffer struct {
	Buf []byte
}

// Ensure Buf length.
func (b *Buffer) Ensure(n int) {
	b.Buf = append(b.Buf[:0], make([]byte, n)...)
}

// Encoder implements encoding to Buffer.
type Encoder interface {
	Encode(b *Buffer)
}

// Encode value that implements Encoder.
func (b *Buffer) Encode(e Encoder) {
	e.Encode(b)
}

// Reset buffer to zero length.
func (b *Buffer) Reset() {
	b.Buf = b.Buf[:0]
}

// Read implements io.Reader.
func (b *Buffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(b.Buf) == 0 {
		return 0, io.EOF
	}
	n = copy(p, b.Buf)
	b.Buf = b.Buf[n:]
	return n, nil
}

// PutUVarInt encodes x  as uvarint.
func (b *Buffer) PutUVarInt(x uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, x)
	b.Buf = append(b.Buf, buf[:n]...)
}

// PutInt encodes integer as uvarint.
func (b *Buffer) PutInt(x int) {
	b.PutUVarInt(uint64(x))
}

func (b *Buffer) PutInt128(v [16]byte) {
	b.Buf = append(b.Buf, v[:]...)
}

// PutByte encodes byte ad uvarint.
func (b *Buffer) PutByte(x byte) {
	b.PutUVarInt(uint64(x))
}

// PutLen encodes length to buffer as uvarint.
func (b *Buffer) PutLen(x int) {
	b.PutUVarInt(uint64(x))
}

// PutString encodes sting value to buffer.
func (b *Buffer) PutString(s string) {
	b.PutLen(len(s))
	b.Buf = append(b.Buf, s...)
}
