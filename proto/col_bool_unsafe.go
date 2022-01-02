//go:build amd64 && !nounsafe

package proto

import (
	"unsafe"

	"github.com/go-faster/errors"
)

// EncodeColumn encodes Bool rows to *Buffer.
func (c ColBool) EncodeColumn(b *Buffer) {
	if len(c) == 0 {
		return
	}
	offset := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, len(c))...)
	s := *(*slice)(unsafe.Pointer(&c))
	src := *(*[]byte)(unsafe.Pointer(&s))
	dst := b.Buf[offset:]
	copy(dst, src)
}

// DecodeColumn decodes Bool rows from *Reader.
func (c *ColBool) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	*c = append(*c, make([]bool, rows)...)
	s := *(*slice)(unsafe.Pointer(c))
	dst := *(*[]byte)(unsafe.Pointer(&s))
	if err := r.ReadFull(dst); err != nil {
		return errors.Wrap(err, "read full")
	}
	return nil
}
