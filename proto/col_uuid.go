package proto

import (
	"github.com/go-faster/errors"
	"github.com/google/uuid"
)

// ColUUID is UUID column.
type ColUUID []uuid.UUID

// Compile-time assertions for ColUUID.
var (
	_ Input  = ColUUID{}
	_ Result = (*ColUUID)(nil)
	_ Column = (*ColUUID)(nil)
)

func (c ColUUID) Type() ColumnType {
	return ColumnTypeUUID
}

func (c ColUUID) Rows() int { return len(c) }

func (c *ColUUID) DecodeColumn(r *Reader, rows int) error {
	const size = 16
	data, err := r.ReadRaw(rows * size)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	v := *c
	for i := 0; i < len(data); i += size {
		v = append(v, *(*[size]byte)(data[i : i+size]))
	}
	*c = v
	return nil
}

func (c *ColUUID) Reset() {
	*c = (*c)[:0]
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
