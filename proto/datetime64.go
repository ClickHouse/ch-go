package proto

import (
	"time"
)

// Precision of DateTime64 and Time64.
//
// Tick size (precision): 10^(-precision) seconds.
// Valid range: [0:9].
type Precision byte

// Duration returns duration of single tick for precision.
func (p Precision) Duration() time.Duration {
	return time.Nanosecond * time.Duration(p.Scale())
}

// Valid reports whether precision is valid.
func (p Precision) Valid() bool {
	return p <= PrecisionMax
}

func (p Precision) Scale() int64 {
	d := int64(1)
	for i := PrecisionNano; i > p; i-- {
		d *= 10
	}
	return d
}

const (
	// PrecisionSecond is one second precision.
	PrecisionSecond Precision = 0
	// PrecisionMilli is millisecond precision.
	PrecisionMilli Precision = 3
	// PrecisionMicro is microsecond precision.
	PrecisionMicro Precision = 6
	// PrecisionNano is nanosecond precision.
	PrecisionNano Precision = 9

	// PrecisionMax is maximum precision (nanosecond).
	PrecisionMax = PrecisionNano
)

// DateTime64 represents DateTime64 type.
//
// See https://clickhouse.com/docs/en/sql-reference/data-types/datetime64/.
type DateTime64 int64

// ToDateTime64 converts time.Time to DateTime64.
func ToDateTime64(t time.Time, p Precision) DateTime64 {
	if t.IsZero() {
		return 0
	}
	// Compute the tick count at the column's own scale rather than via t.UnixNano(): the int64
	// nanosecond count overflows just past 2262-04-11, so far-future values (e.g. ClickHouse's
	// 9999-12-31 ceiling for DateTime64(3)) would otherwise wrap to a garbage tick on the way in.
	// secScale is ticks-per-second (10^precision); the result is the same int64 tick count ClickHouse
	// stores, so each precision reaches exactly ClickHouse's DateTime64(precision) range. Nanosecond
	// precision still tops out near 2262-04-11, where an int64 count of nanoseconds runs out — there
	// this reduces to the old UnixNano computation.
	secScale := int64(1e9) / p.Scale()
	return DateTime64(t.Unix()*secScale + int64(t.Nanosecond())/p.Scale())
}

// Time returns DateTime64 as time.Time.
func (d DateTime64) Time(p Precision) time.Time {
	// Split the tick count into whole seconds and sub-second ticks before scaling up to
	// nanoseconds, so a far-future value never forms an int64-overflowing nanosecond intermediate.
	secScale := int64(1e9) / p.Scale()
	return time.Unix(int64(d)/secScale, (int64(d)%secScale)*p.Scale())
}
