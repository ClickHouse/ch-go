// Code generated by ./cmd/ch-gen-int, DO NOT EDIT.

package proto

import (
	"encoding/binary"
	"github.com/go-faster/errors"
)

// ClickHouse uses LittleEndian.
var _ = binary.LittleEndian

// ColInt128 represents Int128 column.
type ColInt128 []Int128

// Compile-time assertions for ColInt128.
var (
	_ ColInput  = ColInt128{}
	_ ColResult = (*ColInt128)(nil)
	_ Column    = (*ColInt128)(nil)
)

// Type returns ColumnType of Int128.
func (ColInt128) Type() ColumnType {
	return ColumnTypeInt128
}

// Rows returns count of rows in column.
func (c ColInt128) Rows() int {
	return len(c)
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *ColInt128) Reset() {
	*c = (*c)[:0]
}

// NewArrInt128 returns new Array(Int128).
func NewArrInt128() *ColArr {
	return &ColArr{
		Data: new(ColInt128),
	}
}

// AppendInt128 appends slice of Int128 to Array(Int128).
func (c *ColArr) AppendInt128(data []Int128) {
	d := c.Data.(*ColInt128)
	*d = append(*d, data...)
	c.Offsets = append(c.Offsets, uint64(len(*d)))
}

// EncodeColumn encodes Int128 rows to *Buffer.
func (c ColInt128) EncodeColumn(b *Buffer) {
	const size = 128 / 8
	offset := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, size*len(c))...)
	for _, v := range c {
		binPutUInt128(
			b.Buf[offset:offset+size],
			UInt128(v),
		)
		offset += size
	}
}

// DecodeColumn decodes Int128 rows from *Reader.
func (c *ColInt128) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	const size = 128 / 8
	data, err := r.ReadRaw(rows * size)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	v := *c
	// Move bound check out of loop.
	//
	// See https://github.com/golang/go/issues/30945.
	_ = data[len(data)-size]
	for i := 0; i <= len(data)-size; i += size {
		v = append(v,
			Int128(binUInt128(data[i:i+size])),
		)
	}
	*c = v
	return nil
}
