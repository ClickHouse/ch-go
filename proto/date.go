package proto

import "time"

// Date represents Date value.
//
// https://clickhouse.com/docs/en/sql-reference/data-types/date/
type Date uint16

// DateLayout is default time format for Date.
const DateLayout = "2006-01-02"

// secInDay represents seconds in day.
//
// NB: works only on UTC, use time.Date, time.Time.AddDate.
const secInDay = 24 * 60 * 60

// Time returns starting time.Time of Date.
func (d Date) Time() time.Time {
	return time.Unix(secInDay*int64(d), 0)
}

func (d Date) String() string {
	return d.Time().UTC().Format(DateLayout)
}

// TimeToDate returns Date of time.Time in UTC.
func TimeToDate(t time.Time) Date {
	return Date(t.Unix() / secInDay)
}

// NewDate returns the Date corresponding to year, month and day in UTC.
func NewDate(year int, month time.Month, day int) Date {
	return TimeToDate(time.Date(year, month, day, 0, 0, 0, 0, time.UTC))
}
