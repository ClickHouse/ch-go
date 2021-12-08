package proto

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Buffer implements ClickHouse binary protocol encoding.
type Buffer struct {
	Buf []byte
}

// Reader returns new *Reader from *Buffer.
func (b *Buffer) Reader() *Reader {
	return NewReader(bytes.NewReader(b.Buf))
}

// Ensure Buf length.
func (b *Buffer) Ensure(n int) {
	b.Buf = append(b.Buf[:0], make([]byte, n)...)
}

// Encoder implements encoding to Buffer.
type Encoder interface {
	Encode(b *Buffer)
}

// AwareEncoder implements encoding to Buffer that depends on version.
type AwareEncoder interface {
	EncodeAware(b *Buffer, version int)
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

// PutRaw writes v as raw bytes to buffer.
func (b *Buffer) PutRaw(v []byte) {
	b.Buf = append(b.Buf, v...)
}

// PutUVarInt encodes x as uvarint.
func (b *Buffer) PutUVarInt(x uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, x)
	b.Buf = append(b.Buf, buf[:n]...)
}

// PutInt encodes integer as uvarint.
func (b *Buffer) PutInt(x int) {
	b.PutUVarInt(uint64(x))
}

// PutInt128 puts 16-byte integer.
func (b *Buffer) PutInt128(v [128 / 8]byte) {
	b.Buf = append(b.Buf, v[:]...)
}

// PutByte encodes byte as uint8.
func (b *Buffer) PutByte(x byte) {
	b.PutUInt8(x)
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

func (b *Buffer) PutUInt32(x uint32) {
	buf := make([]byte, 32/8)
	bin.PutUint32(buf, x)
	b.Buf = append(b.Buf, buf...)
}

func (b *Buffer) PutUInt64(x uint64) {
	buf := make([]byte, 64/8)
	bin.PutUint64(buf, x)
	b.Buf = append(b.Buf, buf...)
}

func (b *Buffer) PutInt64(x int64) {
	b.PutUInt64(uint64(x))
}

func (b *Buffer) PutInt32(x int32) {
	b.PutUInt32(uint32(x))
}

func (b *Buffer) PutUInt8(x uint8) {
	b.Buf = append(b.Buf, x)
}

func (b *Buffer) PutBool(v bool) {
	if v {
		b.PutUInt8(boolTrue)
	} else {
		b.PutUInt8(boolFalse)
	}
}
