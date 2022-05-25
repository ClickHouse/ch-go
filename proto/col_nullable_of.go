package proto

import "github.com/go-faster/errors"

// Compile-time assertions for ColNullableOf.
var (
	_ ColInput                   = (*ColNullableOf[string])(nil)
	_ ColResult                  = (*ColNullableOf[string])(nil)
	_ Column                     = (*ColNullableOf[string])(nil)
	_ ColumnOf[Nullable[string]] = (*ColNullableOf[string])(nil)
	_ StateEncoder               = (*ColNullableOf[string])(nil)
	_ StateDecoder               = (*ColNullableOf[string])(nil)

	_ = ColNullableOf[string]{
		Values: new(ColStr),
	}
)

// Nullable is T value that can be null.
type Nullable[T any] struct {
	Set   bool
	Value T
}

// NewNullable returns set value of Nullable[T] to v.
func NewNullable[T any](v T) Nullable[T] {
	return Nullable[T]{Set: true, Value: v}
}

// Null returns null value for Nullable[T].
func Null[T any]() Nullable[T] {
	return Nullable[T]{}
}

func (n Nullable[T]) IsSet() bool { return n.Set }

func (n Nullable[T]) Or(v T) T {
	if n.Set {
		return v
	}
	return n.Value
}

// ColNullableOf is Nullable(T) column.
type ColNullableOf[T any] struct {
	Nulls  ColUInt8
	Values ColumnOf[T]
}

func (c *ColNullableOf[T]) DecodeState(r *Reader) error {
	if s, ok := c.Values.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "values state")
		}
	}
	return nil
}

func (c ColNullableOf[T]) EncodeState(b *Buffer) {
	if s, ok := c.Values.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

func (c ColNullableOf[T]) Type() ColumnType {
	return ColumnTypeNullable.Sub(c.Values.Type())
}

func (c *ColNullableOf[T]) DecodeColumn(r *Reader, rows int) error {
	if err := c.Nulls.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "nulls")
	}
	if err := c.Values.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "values")
	}
	return nil
}

func (c ColNullableOf[T]) Rows() int {
	return c.Nulls.Rows()
}

func (c *ColNullableOf[T]) Append(v Nullable[T]) {
	null := boolTrue
	if v.Set {
		null = boolFalse
	}
	c.Nulls.Append(null)
	c.Values.Append(v.Value)
}

func (c *ColNullableOf[T]) AppendArr(v []Nullable[T]) {
	for _, vv := range v {
		c.Append(vv)
	}
}

func (c ColNullableOf[T]) Row(i int) Nullable[T] {
	return Nullable[T]{
		Value: c.Values.Row(i),
		Set:   c.Nulls.Row(i) == boolFalse,
	}
}

func (c *ColNullableOf[T]) Reset() {
	c.Nulls.Reset()
	c.Values.Reset()
}

func (c ColNullableOf[T]) EncodeColumn(b *Buffer) {
	c.Nulls.EncodeColumn(b)
	c.Values.EncodeColumn(b)
}

func (c ColNullableOf[T]) IsElemNull(i int) bool {
	if i < c.Rows() {
		return c.Nulls[i] == boolTrue
	}
	return false
}
