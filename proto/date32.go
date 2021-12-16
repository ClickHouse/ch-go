package proto

import "time"

// Date32 represents Date32 value.
//
// https://clickhouse.com/docs/en/sql-reference/data-types/date32/
type Date32 uint32

// date32Epoch is unix time of 1925-01-01.
const date32Epoch = -1420070400

// Time returns starting time.Time of Date32.
func (d Date32) Time() time.Time {
	return time.Unix(secInDay*int64(d)+date32Epoch, 0)
}

func (d Date32) String() string {
	return d.Time().UTC().Format(DateLayout)
}

// TimeToDate32 returns Date32 of time.Time in UTC.
func TimeToDate32(t time.Time) Date32 {
	return Date32((t.Unix() - date32Epoch) / secInDay)
}

// NewDate32 returns the Date32 corresponding to year, month and day in UTC.
func NewDate32(year int, month time.Month, day int) Date32 {
	return TimeToDate32(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}
