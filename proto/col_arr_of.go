package proto

import "github.com/go-faster/errors"

// Compile-time assertions for ArrayOf.
var (
	_ ColInput  = ArrayOf[string]((*ColStr)(nil))
	_ ColResult = ArrayOf[string]((*ColStr)(nil))
	_ Column    = ArrayOf[string]((*ColStr)(nil))
)

// ColumnOf is generic Column(T) constraint.
type ColumnOf[T any] interface {
	Column
	Append(v T)
	AppendArr(v []T)
	Row(i int) T
}

// ColArrOf is generic ColArr.
type ColArrOf[T any] struct {
	Offsets ColUInt64
	Data    ColumnOf[T]
}

// ArrayOf returns ColArrOf of c.
//
// Example: ArrayOf[string](&ColStr{})
func ArrayOf[T any](c ColumnOf[T]) *ColArrOf[T] {
	return &ColArrOf[T]{
		Data: c,
	}
}

func (c ColArrOf[T]) Type() ColumnType {
	return ColumnTypeArray.Sub(c.Data.Type())
}

func (c ColArrOf[T]) Rows() int {
	return c.Offsets.Rows()
}

func (c ColArrOf[T]) RowAppend(i int, target []T) []T {
	var start int
	end := int(c.Offsets[i])
	if i > 0 {
		start = int(c.Offsets[i-1])
	}
	for idx := start; idx < end; idx++ {
		target = append(target, c.Data.Row(idx))
	}

	return target
}

func (c ColArrOf[T]) Row(i int) []T {
	return c.RowAppend(i, nil)
}

func (c *ColArrOf[T]) DecodeColumn(r *Reader, rows int) error {
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

func (c *ColArrOf[T]) Reset() {
	c.Data.Reset()
	c.Offsets.Reset()
}

func (c ColArrOf[T]) EncodeColumn(b *Buffer) {
	c.Offsets.EncodeColumn(b)
	c.Data.EncodeColumn(b)
}

func (c *ColStr) Array() *ColArrOf[string] {
	return &ColArrOf[string]{
		Data: c,
	}
}

func (c *ColArrOf[T]) Append(v []T) {
	c.Data.AppendArr(v)
}

func (c *ColArrOf[T]) AppendArr(v [][]T) {
	for _, e := range v {
		c.Append(e)
	}
}
