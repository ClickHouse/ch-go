package proto

import "github.com/go-faster/errors"

// ColMap represents Map column.
type ColMap struct {
	Offsets ColUInt64
	Keys    Column
	Values  Column
}

func (c ColMap) Type() ColumnType {
	return ColumnTypeMap.Sub(c.Keys.Type(), c.Values.Type())
}

func (c ColMap) Rows() int {
	return c.Offsets.Rows()
}

func (c *ColMap) DecodeColumn(r *Reader, rows int) error {
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

func (c *ColMap) Reset() {
	c.Offsets.Reset()
	c.Keys.Reset()
	c.Values.Reset()
}

func (c ColMap) EncodeColumn(b *Buffer) {
	if c.Rows() == 0 {
		return
	}

	c.Offsets.EncodeColumn(b)
	c.Keys.EncodeColumn(b)
	c.Values.EncodeColumn(b)
}
