package proto

import "time"

// Precision of DateTime64.
//
// Tick size (precision): 10^(-precision) seconds.
// Valid range: [0:9].
type Precision byte

// Duration returns duration of single tick for precision.
func (p Precision) Duration() time.Duration {
	d := time.Nanosecond
	for i := PrecisionNano; i > p; i-- {
		d *= 10
	}
	return d
}

// Valid reports whether precision is valid.
func (p Precision) Valid() bool {
	return p <= PrecisionMax
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
	switch p {
	case PrecisionMicro:
		return DateTime64(t.UnixMicro())
	case PrecisionMilli:
		return DateTime64(t.UnixMilli())
	case PrecisionNano:
		return DateTime64(t.UnixNano())
	case PrecisionSecond:
		return DateTime64(t.Unix())
	default:
		// TODO(ernado): support all precisions
		panic("precision not supported")
	}
}

// Time returns DateTime64 as time.Time.
func (d DateTime64) Time(p Precision) time.Time {
	switch p {
	case PrecisionMicro:
		return time.UnixMicro(int64(d))
	case PrecisionMilli:
		return time.UnixMilli(int64(d))
	case PrecisionNano:
		nsec := int64(d)
		return time.Unix(nsec/1e9, nsec%1e9)
	case PrecisionSecond:
		sec := int64(d)
		return time.Unix(sec, 0)
	default:
		// TODO(ernado): support all precisions
		panic("precision not supported")
	}
}
