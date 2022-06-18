package proto

import (
	"strconv"
	"time"

	"github.com/go-faster/errors"
)

var (
	_ ColumnOf[time.Time] = (*ColDateTime64)(nil)
	_ Inferable           = (*ColDateTime64)(nil)
	_ Column              = (*ColDateTime64)(nil)
)

// ColDateTime64 implements ColumnOf[time.Time].
type ColDateTime64 struct {
	Data      []DateTime64
	Precision Precision
	Location  *time.Location
}

func (c *ColDateTime64) WithPrecision(p Precision) *ColDateTime64 {
	c.Precision = p
	return c
}

func (c *ColDateTime64) WithLocation(loc *time.Location) *ColDateTime64 {
	c.Location = loc
	return c
}

func (c ColDateTime64) Rows() int {
	return len(c.Data)
}

func (c *ColDateTime64) Reset() {
	c.Data = c.Data[:0]
}

func (c ColDateTime64) Type() ColumnType {
	sub := ColumnType(strconv.Itoa(int(c.Precision)))
	return ColumnTypeDateTime64.Sub(sub)
}

func (c *ColDateTime64) Infer(t ColumnType) error {
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

func (c ColDateTime64) Row(i int) time.Time {
	return c.Data[i].Time(c.Precision).In(c.loc())
}

func (c ColDateTime64) loc() *time.Location {
	if c.Location == nil {
		// Defaulting to local timezone (not UTC).
		return time.Local
	}
	return c.Location
}

func (c *ColDateTime64) Append(v time.Time) {
	c.Data = append(c.Data, ToDateTime64(v, c.Precision))
}
