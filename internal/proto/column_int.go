package proto

import "github.com/go-faster/errors"

type ColumnUInt8 []uint8

func (c ColumnUInt8) Rows() int {
	return len(c)
}

func (c ColumnUInt8) EncodeColumn(b *Buffer) {
	for _, v := range c {
		b.PutUInt8(v)
	}
}

func (c *ColumnUInt8) DecodeColumn(r *Reader, rows int) error {
	for i := 0; i < rows; i++ {
		v, err := r.UInt8()
		if err != nil {
			return errors.Wrapf(err, "[%d]: read", i)
		}
		*c = append(*c, v)
	}
	return nil
}

func (c ColumnUInt8) Type() ColumnType { return ColumnTypeUInt8 }
