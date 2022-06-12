package proto

import "github.com/go-faster/errors"

// Compile-time assertions for ArrayOf.
var (
	_ ColInput     = ArrayOf[string]((*ColStr)(nil))
	_ ColResult    = ArrayOf[string]((*ColStr)(nil))
	_ Column       = ArrayOf[string]((*ColStr)(nil))
	_ StateEncoder = ArrayOf[string]((*ColStr)(nil))
	_ StateDecoder = ArrayOf[string]((*ColStr)(nil))
)

// ColumnOf is generic Column(T) constraint.
type ColumnOf[T any] interface {
	Column
	Append(v T)
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

// Type returns type of array, i.e. Array(T).
func (c ColArrOf[T]) Type() ColumnType {
	return ColumnTypeArray.Sub(c.Data.Type())
}

// Rows returns rows count.
func (c ColArrOf[T]) Rows() int {
	return c.Offsets.Rows()
}

func (c *ColArrOf[T]) DecodeState(r *Reader) error {
	if s, ok := c.Data.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "data state")
		}
	}
	return nil
}

func (c *ColArrOf[T]) EncodeState(b *Buffer) {
	if s, ok := c.Data.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

// Prepare ensures Preparable column propagation.
func (c *ColArrOf[T]) Prepare() error {
	if v, ok := c.Data.(Preparable); ok {
		if err := v.Prepare(); err != nil {
			return errors.Wrap(err, "prepare data")
		}
	}
	return nil
}

// RowAppend appends i-th row to target and returns it.
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

// Row returns i-th row.
func (c ColArrOf[T]) Row(i int) []T {
	return c.RowAppend(i, nil)
}

// DecodeColumn implements ColResult.
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

// Reset implements ColResult.
func (c *ColArrOf[T]) Reset() {
	c.Data.Reset()
	c.Offsets.Reset()
}

// EncodeColumn implements ColInput.
func (c ColArrOf[T]) EncodeColumn(b *Buffer) {
	c.Offsets.EncodeColumn(b)
	c.Data.EncodeColumn(b)
}

// Append appends new row to column.
func (c *ColArrOf[T]) Append(v []T) {
	for _, s := range v {
		c.Data.Append(s)
	}
	c.Offsets = append(c.Offsets, uint64(c.Data.Rows()))
}

// Result for current column.
func (c *ColArrOf[T]) Result(column string) ResultColumn {
	return ResultColumn{Name: column, Data: c}
}

// Results return Results containing single column.
func (c *ColArrOf[T]) Results(column string) Results {
	return Results{c.Result(column)}
}
