package proto

import "github.com/go-faster/errors"

// ColAuto is column that is initialized during decoding.
type ColAuto struct {
	Data Column
}

// Infer and initialize Column from ColumnType.
func (c *ColAuto) Infer(t ColumnType) error {
	if c.Data != nil && !c.Type().Conflicts(t) {
		// Already ok.
		return nil
	}
	if c.inferNumeric(t) {
		return nil
	}
	switch t {
	case ColumnTypeString:
		c.Data = new(ColStr)
	case ColumnTypeBool:
		c.Data = new(ColBool)
	default:
		return errors.Errorf("automatic column inference not supported for %q", t)
	}

	return nil
}

var (
	_ Column      = &ColAuto{}
	_ InferColumn = &ColAuto{}
)

// InferColumn is Column that supports type inference.
type InferColumn interface {
	Column
	Infer(t ColumnType) error
}

func (c ColAuto) Type() ColumnType {
	return c.Data.Type()
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
