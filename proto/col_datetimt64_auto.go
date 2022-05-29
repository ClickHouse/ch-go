package proto

import "time"

var _ ColumnOf[time.Time] = (*ColDateTime64Auto)(nil)

// ColDateTime64Auto implements ColumnOf[time.Time].
type ColDateTime64Auto struct {
	ColDateTime64
	Precision Precision
}

func (c ColDateTime64Auto) Row(i int) time.Time {
	return c.ColDateTime64.Row(i).Time(c.Precision)
}

func (c ColDateTime64Auto) Append(v time.Time) {
	c.ColDateTime64.Append(ToDateTime64(v, c.Precision))
}

func (c ColDateTime64Auto) AppendArr(v []time.Time) {
	for _, t := range v {
		c.ColDateTime64.Append(ToDateTime64(t, c.Precision))
	}
}
