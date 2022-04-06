package proto

import "github.com/go-faster/errors"

// Compile-time assertions for ColMapOf.
var (
	_ ColInput = ColMapOf[string, string]{
		Keys:   &ColStr{},
		Values: &ColStr{},
	}
	_ ColResult = &ColMapOf[string, string]{
		Keys:   &ColStr{},
		Values: &ColStr{},
	}
	_ Column = &ColMapOf[string, string]{
		Keys:   &ColStr{},
		Values: &ColStr{},
	}
)

type ColMapOf[K comparable, V any] struct {
	Offsets ColUInt64
	Keys    ColumnOf[K]
	Values  ColumnOf[V]
}

func (c ColMapOf[K, V]) Get(k K) (v V, ok bool) {
	return v, ok
}

func (c ColMapOf[K, V]) Type() ColumnType {
	return ColumnTypeMap.Sub(c.Keys.Type(), c.Values.Type())
}

func (c ColMapOf[K, V]) Rows() int {
	return c.Offsets.Rows()
}

func (c *ColMapOf[K, V]) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	if err := c.Offsets.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "offsets")
	}

	count := int(c.Offsets[rows-1])
	if err := checkRows(count); err != nil {
		return errors.Wrap(err, "keys count")
	}
	if err := c.Keys.DecodeColumn(r, count); err != nil {
		return errors.Wrap(err, "keys")
	}
	if err := c.Values.DecodeColumn(r, count); err != nil {
		return errors.Wrap(err, "values")
	}

	return nil
}

func (c *ColMapOf[K, V]) Reset() {
	c.Offsets.Reset()
	c.Keys.Reset()
	c.Values.Reset()
}

func (c ColMapOf[K, V]) EncodeColumn(b *Buffer) {
	if c.Rows() == 0 {
		return
	}

	c.Offsets.EncodeColumn(b)
	c.Keys.EncodeColumn(b)
	c.Values.EncodeColumn(b)
}
