//go:build go1.18

package proto

import "github.com/go-faster/errors"

// ColumnOf is generic Column(T) constraint.
type ColumnOf[T any] interface {
	Column
	Append(v T)
	AppendArr(v []T)
	Row(i int) T
}

// ColArrOf is generic ColArr.
type ColArrOf[T any, C ColumnOf[T]] struct {
	Offsets ColUInt64
	Data    C
}

// ArrayOf returns ColArrOf of c.
//
// Example: ArrayOf[string](&ColStr{})
func ArrayOf[T any, C ColumnOf[T]](c C) *ColArrOf[T, C] {
	return &ColArrOf[T, C]{
		Data: c,
	}
}

func (c ColArrOf[T, C]) Type() ColumnType {
	return ColumnTypeArray.Sub(c.Data.Type())
}

func (c ColArrOf[T, C]) Rows() int {
	return c.Offsets.Rows()
}

func (c ColArrOf[T, C]) RowAppend(i int, target []T) []T {
	var start int
	end := int(c.Offsets[i])
	if i > 0 {
		start = int(c.Offsets[i-1])
	}
	for idx := start; idx < end; idx++ {
		target = append(target, c.Data.Row(i))
	}

	return target
}

func (c ColArrOf[T, C]) Row(i int) []T {
	return c.RowAppend(i, nil)
}

func (c ColArrOf[T, C]) DecodeColumn(r *Reader, rows int) error {
	if err := c.Offsets.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "read offsets")
	}
	var size int
	if l := len(c.Offsets); l > 0 {
		// Pick last offset as total size of "elements" column.
		size = int(c.Offsets[l-1])
	}
	if err := c.Data.DecodeColumn(r, size); err != nil {
		return errors.Wrap(err, "decode data")
	}
	return nil
}

func (c *ColArrOf[T, C]) Reset() {
	c.Data.Reset()
	c.Offsets.Reset()
}

func (c ColArrOf[T, C]) EncodeColumn(b *Buffer) {
	c.Offsets.EncodeColumn(b)
	c.Data.EncodeColumn(b)
}

func (c *ColStr) Array() *ColArrOf[string, *ColStr] {
	return &ColArrOf[string, *ColStr]{
		Data: c,
	}
}

func (c *ColArrOf[T, C]) Append(v []T) {
	c.Data.AppendArr(v)
}

func (c *ColArrOf[T, C]) AppendArr(v [][]T) {
	for _, e := range v {
		c.Append(e)
	}
}
