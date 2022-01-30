package proto

import "github.com/go-faster/errors"

// ColTuple is Tuple column.
//
// Basically it is just a group of columns.
type ColTuple []Column

func (c ColTuple) DecodeState(r *Reader) error {
	for i, v := range c {
		if s, ok := v.(StateDecoder); ok {
			if err := s.DecodeState(r); err != nil {
				return errors.Wrapf(err, "[%d]", i)
			}
		}
	}
	return nil
}

func (c ColTuple) EncodeState(b *Buffer) {
	for _, v := range c {
		if s, ok := v.(StateEncoder); ok {
			s.EncodeState(b)
		}
	}
}

func (c ColTuple) Type() ColumnType {
	var types []ColumnType
	for _, v := range c {
		types = append(types, v.Type())
	}
	return ColumnTypeTuple.Sub(types...)
}

func (c ColTuple) First() Column {
	if len(c) == 0 {
		return nil
	}
	return c[0]
}

func (c ColTuple) Rows() int {
	if f := c.First(); f != nil {
		return f.Rows()
	}
	return 0
}

func (c ColTuple) DecodeColumn(r *Reader, rows int) error {
	for i, v := range c {
		if err := v.DecodeColumn(r, rows); err != nil {
			return errors.Wrapf(err, "[%d]", i)
		}
	}
	return nil
}

func (c ColTuple) Reset() {
	for _, v := range c {
		v.Reset()
	}
}

func (c ColTuple) EncodeColumn(b *Buffer) {
	for _, v := range c {
		v.EncodeColumn(b)
	}
}
