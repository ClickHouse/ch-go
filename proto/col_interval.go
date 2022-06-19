package proto

import (
	"fmt"
	"strings"

	"github.com/go-faster/errors"
)

//go:generate go run github.com/dmarkham/enumer -type IntervalScale -output interval_enum.go

type IntervalScale byte

const (
	IntervalSecond IntervalScale = iota
	IntervalMinute
	IntervalHour
	IntervalDay
	IntervalWeek
	IntervalMonth
	IntervalQuarter
	IntervalYear
)

type Interval struct {
	Scale IntervalScale
	Value int64
}

func (i Interval) String() string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("%d", i.Value))
	out.WriteRune(' ')
	out.WriteString(strings.ToLower(strings.TrimPrefix(i.Scale.String(), ColumnTypeInterval.String())))
	return out.String()
}

type ColInterval struct {
	Scale  IntervalScale
	Values ColInt64
}

func (c *ColInterval) Infer(t ColumnType) error {
	scale, err := IntervalScaleString(t.String())
	if err != nil {
		return errors.Wrap(err, "scale")
	}
	c.Scale = scale
	return nil
}

func (c *ColInterval) Append(v Interval) {
	if v.Scale != c.Scale {
		panic(fmt.Sprintf("append: cant append %s to %s", v.Scale, c.Scale))
	}
	c.Values.Append(v.Value)
}

func (c ColInterval) Row(i int) Interval {
	return Interval{
		Scale: c.Scale,
		Value: c.Values.Row(i),
	}
}

func (c ColInterval) Type() ColumnType {
	return ColumnType(c.Scale.String())
}

func (c ColInterval) Rows() int {
	return len(c.Values)
}

func (c *ColInterval) DecodeColumn(r *Reader, rows int) error {
	return c.Values.DecodeColumn(r, rows)
}

func (c *ColInterval) Reset() {
	c.Values.Reset()
}

func (c ColInterval) EncodeColumn(b *Buffer) {
	c.Values.EncodeColumn(b)
}
