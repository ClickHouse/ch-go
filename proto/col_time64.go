package proto

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-faster/errors"
)

// ColTime64 implements ColumnOf[time.Duration] for ClickHouse Time64 type.
// Note: The Location field is only used for type metadata (e.g., for
// generating the ClickHouse column type string). It does not affect
// value conversion or storage.
type ColTime64 struct {
	Data         []Time64
	Precision    Precision
	PrecisionSet bool
	Location     *time.Location
}

var (
	_ ColumnOf[time.Duration] = (*ColTime64)(nil)
	_ Inferable               = (*ColTime64)(nil)
)

func (c *ColTime64) WithPrecision(p Precision) *ColTime64 {
	c.Precision = p
	c.PrecisionSet = true
	return c
}

func (c ColTime64) Rows() int {
	return len(c.Data)
}

func (c *ColTime64) Reset() {
	c.Data = c.Data[:0]
}

func (c ColTime64) Type() ColumnType {
	if c.PrecisionSet {
		if c.Location != nil {
			return ColumnTypeTime64.With(strconv.Itoa(int(c.Precision)), c.Location.String())
		}
		return ColumnTypeTime64.With(strconv.Itoa(int(c.Precision)))
	}
	return ColumnTypeTime64
}

func (c *ColTime64) Infer(t ColumnType) error {
	elem := string(t.Elem())
	if elem == "" {
		return errors.Errorf("invalid Time64: no elements in %q", t)
	}
	pStr, locStr, hasloc := strings.Cut(elem, ",")
	pStr = strings.Trim(pStr, `' `)
	locStr = strings.Trim(locStr, `' `)
	n, err := strconv.ParseUint(pStr, 10, 8)
	if err != nil {
		return errors.Wrap(err, "parse precision")
	}
	p := Precision(n)
	if !p.Valid() {
		return errors.Errorf("precision %d is invalid", n)
	}
	c.Precision = p
	c.PrecisionSet = true
	if hasloc {
		loc, err := time.LoadLocation(locStr)
		if err != nil {
			return errors.Wrap(err, "invalid location")
		}
		c.Location = loc
	}
	return nil
}

func (c ColTime64) Row(i int) time.Duration {
	if !c.PrecisionSet {
		panic("Time64: no precision set")
	}
	return c.Data[i].Duration(c.Precision)
}

func (c *ColTime64) AppendRaw(v Time64) {
	c.Data = append(c.Data, v)
}

func (c *ColTime64) Append(v time.Duration) {
	if !c.PrecisionSet {
		panic("Time64: no precision set")
	}
	c.AppendRaw(Time64FromDuration(v, c.Precision))
}

func (c *ColTime64) AppendArr(v []time.Duration) {
	if !c.PrecisionSet {
		panic("Time64: no precision set")
	}
	for _, item := range v {
		c.Append(item)
	}
}

// Helper to trim quotes and spaces from a string.
func trimQuotesAndSpaces(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "'")
	return s
}
