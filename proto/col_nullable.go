package proto

import "github.com/go-faster/errors"

// ColNullable represents Nullable(T) column.
//
// Nulls is nullable "mask" on Values column.
// For example, to encode [null, "", "hello", null, "world"]
//	Values: ["", "", "hello", "", "world"] (len: 5)
//	Nulls:  [ 1,  0,       0,  1,       0] (len: 5)
// Values and Nulls row counts are always equal.
type ColNullable struct {
	Nulls  ColUInt8
	Values Column
}

func (c ColNullable) Type() ColumnType {
	return ColumnTypeNullable.Sub(c.Values.Type())
}

func (c ColNullable) Rows() int {
	return c.Nulls.Rows()
}

func (c *ColNullable) DecodeColumn(r *Reader, rows int) error {
	if err := c.Nulls.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "nulls")
	}
	if err := c.Values.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "values")
	}
	return nil
}

func (c *ColNullable) Reset() {
	c.Nulls.Reset()
	c.Values.Reset()
}

func (c ColNullable) EncodeColumn(b *Buffer) {
	c.Nulls.EncodeColumn(b)
	c.Values.EncodeColumn(b)
}

func (c ColNullable) IsElemNull(i int) bool {
	if i < c.Rows() {
		return c.Nulls[i] == boolTrue
	}
	return false
}
