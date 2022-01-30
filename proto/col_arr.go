package proto

import (
	"github.com/go-faster/errors"
)

// ColArr represents Array[T].
type ColArr struct {
	Offsets ColUInt64
	Data    Column
}

// Compile-time assertions for ColArr.
var (
	_ ColInput  = ColArr{}
	_ ColResult = (*ColArr)(nil)
	_ Column    = (*ColArr)(nil)
)

func (c ColArr) EncodeColumn(b *Buffer) {
	c.Offsets.EncodeColumn(b)
	c.Data.EncodeColumn(b)
}

func (c ColArr) Type() ColumnType {
	return c.Data.Type().Array()
}

func (c ColArr) Rows() int {
	return len(c.Offsets)
}

func (c *ColArr) DecodeState(r *Reader) error {
	if s, ok := c.Data.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "data state")
		}
	}
	return nil
}

func (c *ColArr) EncodeState(b *Buffer) {
	if s, ok := c.Data.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

func (c *ColArr) DecodeColumn(r *Reader, rows int) error {
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

func (c *ColArr) Reset() {
	c.Offsets = c.Offsets[:0]
	c.Data.Reset()
}

// ColInfo wraps Name and Type of column.
type ColInfo struct {
	Name string
	Type ColumnType
}

// ColInfoInput saves column info on decoding.
type ColInfoInput []ColInfo

func (s *ColInfoInput) Reset() {
	*s = (*s)[:0]
}

func (s *ColInfoInput) DecodeResult(r *Reader, b Block) error {
	s.Reset()
	if b.Rows > 0 {
		return errors.New("got unexpected rows")
	}
	for i := 0; i < b.Columns; i++ {
		columnName, err := r.Str()
		if err != nil {
			return errors.Wrapf(err, "column [%d] name", i)
		}
		columnTypeRaw, err := r.Str()
		if err != nil {
			return errors.Wrapf(err, "column [%d] type", i)
		}
		*s = append(*s, ColInfo{
			Name: columnName,
			Type: ColumnType(columnTypeRaw),
		})
	}
	return nil
}
