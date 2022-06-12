package proto

import (
	"strconv"
	"time"

	"github.com/go-faster/errors"
)

var (
	_ ColumnOf[time.Time] = (*ColDateTime64Auto)(nil)
	_ Inferable           = (*ColDateTime64Auto)(nil)
)

// ColDateTime64Auto implements ColumnOf[time.Time].
type ColDateTime64Auto struct {
	ColDateTime64
	Precision Precision
}

func (c ColDateTime64Auto) Type() ColumnType {
	sub := ColumnType(strconv.Itoa(int(c.Precision)))
	return ColumnTypeDateTime64.Sub(sub)
}

func (c *ColDateTime64Auto) Infer(t ColumnType) error {
	// TODO(ernado): handle (ignore) timezone
	pRaw := t.Elem()
	n, err := strconv.ParseUint(string(pRaw), 10, 8)
	if err != nil {
		return errors.Wrap(err, "parse precision")
	}
	p := Precision(n)
	if !p.Valid() {
		return errors.Errorf("precision %d is invalid", n)
	}
	c.Precision = p
	return nil
}

func (c ColDateTime64Auto) Row(i int) time.Time {
	return c.ColDateTime64.Row(i).Time(c.Precision)
}

func (c ColDateTime64Auto) Append(v time.Time) {
	c.ColDateTime64.Append(ToDateTime64(v, c.Precision))
}
