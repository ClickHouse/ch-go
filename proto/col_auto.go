package proto

import "github.com/go-faster/errors"

// ColAuto is column that is initialized during decoding.
type ColAuto struct {
	Data     Column
	DataType ColumnType
}

// Infer and initialize Column from ColumnType.
func (c *ColAuto) Infer(t ColumnType) error {
	if c.Data != nil && !c.Type().Conflicts(t) {
		// Already ok.
		c.DataType = t // update subtype if needed
		return nil
	}
	if c.inferNumeric(t) {
		c.DataType = t
		return nil
	}
	switch t {
	case ColumnTypeString:
		c.Data = new(ColStr)
	case ColumnTypeBool:
		c.Data = new(ColBool)
	case ColumnTypeDateTime:
		c.Data = new(ColDateTime)
	case ColumnTypeDate:
		c.Data = new(ColDate)
	default:
		switch t.Base() {
		case ColumnTypeLowCardinality:
			if t.Elem() == ColumnTypeString {
				c.Data = &ColLowCardinality{
					Index: new(ColStr),
				}
				c.DataType = t
				return nil
			}
		case ColumnTypeDateTime:
			c.Data = new(ColDateTime)
			c.DataType = t
			return nil
		case ColumnTypeDateTime64:
			v := new(ColDateTime64Auto)
			if err := v.Infer(t); err != nil {
				return errors.Wrap(err, "datetime")
			}
			c.Data = v
			c.DataType = t
			return nil
		}
		return errors.Errorf("automatic column inference not supported for %q", t)
	}

	c.DataType = t
	return nil
}

var (
	_ Column    = &ColAuto{}
	_ Inferable = &ColAuto{}
)

func (c ColAuto) Type() ColumnType {
	return c.DataType
}

func (c ColAuto) Rows() int {
	return c.Data.Rows()
}

func (c ColAuto) DecodeColumn(r *Reader, rows int) error {
	return c.Data.DecodeColumn(r, rows)
}

func (c ColAuto) Reset() {
	c.Data.Reset()
}

func (c ColAuto) EncodeColumn(b *Buffer) {
	c.Data.EncodeColumn(b)
}
