//go:build !amd64 || nounsafe

package proto

import "github.com/go-faster/errors"

// EncodeColumn encodes Bool rows to *Buffer.
func (c ColBool) EncodeColumn(b *Buffer) {
	start := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, len(c))...)
	for i := range c {
		if c[i] {
			b.Buf[i+start] = boolTrue
		} else {
			b.Buf[i+start] = boolFalse
		}
	}
}

// DecodeColumn decodes Bool rows from *Reader.
func (c *ColBool) DecodeColumn(r *Reader, rows int) error {
	data, err := r.ReadRaw(rows)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	v := *c
	v = append(v, make([]bool, rows)...)
	for i := range data {
		switch data[i] {
		case boolTrue:
			v[i] = true
		case boolFalse:
			v[i] = false
		default:
			return errors.Errorf("[%d]: bad value %d for Bool", i, data[i])
		}
	}
	*c = v
	return nil
}
