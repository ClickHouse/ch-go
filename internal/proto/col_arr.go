package proto

import (
	"strings"

	"github.com/go-faster/errors"
)

type Column interface {
	Result
	Input
}

// ColArr represents Array[T].
type ColArr struct {
	Offsets ColUInt64
	Data    Column
}

func (c ColArr) EncodeColumn(b *Buffer) {
	c.Offsets.EncodeColumn(b)
	c.Data.EncodeColumn(b)
}

func (c ColArr) Type() ColumnType {
	var b strings.Builder
	b.WriteString(string(ColumnTypeArray))
	b.WriteRune('(')
	b.WriteString(string(c.Data.Type()))
	b.WriteRune(')')

	return ColumnType(b.String())
}

func (c ColArr) Rows() int {
	return len(c.Offsets)
}

func (c *ColArr) DecodeColumn(r *Reader, rows int) error {
	if err := c.Offsets.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "read offsets")
	}

	var start uint64
	for i := 0; i < rows; i++ {
		end := c.Offsets[i]
		size := int(end - start)
		if err := c.Data.DecodeColumn(r, size); err != nil {
			return errors.Wrap(err, "decode data")
		}
		start = end
	}

	return nil
}

func (c *ColArr) Reset() {
	c.Offsets = c.Offsets[:0]
	c.Data.Reset()
}
