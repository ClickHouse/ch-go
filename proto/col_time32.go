package proto

import (
	"github.com/go-faster/errors"
	"time"
)

// ColTime32 implements ColumnOf[time.Duration] for ClickHouse Time type.
type ColTime32 struct {
	Data []Time32
}

var (
	_ ColumnOf[time.Duration] = (*ColTime32)(nil)
	_ Inferable               = (*ColTime32)(nil)
)

func (c *ColTime32) Append(v time.Duration) {
	t32, err := Time32FromDuration(v)
	if err != nil {
		panic(err)
	}
	c.Data = append(c.Data, t32)
}

func (c *ColTime32) AppendRaw(v Time32) {
	c.Data = append(c.Data, v)
}

func (c *ColTime32) AppendArr(v []time.Duration) {
	for _, d := range v {
		c.Append(d)
	}
}

func (c *ColTime32) Row(i int) time.Duration {
	return c.Data[i].Duration()
}

func (c *ColTime32) Rows() int {
	return len(c.Data)
}

func (c *ColTime32) Reset() {
	c.Data = c.Data[:0]
}

func (c *ColTime32) Type() ColumnType {
	return ColumnTypeTime32
}

func (c *ColTime32) Infer(t ColumnType) error {
	if t != ColumnTypeTime32 && t != "Time" {
		return errors.Errorf("unsupported type: %s", t)
	}
	return nil
}
