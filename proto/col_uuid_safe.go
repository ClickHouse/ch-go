//go:build !(amd64 || arm64) || purego

package proto

import "github.com/go-faster/errors"

func (c *ColUUID) DecodeColumn(r *Reader, rows int) error {
	const size = 16
	data, err := r.ReadRaw(rows * size)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	v := *c
	for i := 0; i < len(data); i += size {
		// In-place conversion from slice to array.
		// https://go.dev/ref/spec#Conversions_from_slice_to_array_pointer
		v = append(v, *(*[size]byte)(data[i : i+size]))
	}
	*c = v
	return nil
}

func (c ColUUID) EncodeColumn(b *Buffer) {
	const size = 16
	offset := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, size*len(c))...)
	for _, v := range c {
		copy(b.Buf[offset:offset+size], v[:])
		offset += size
	}
}
