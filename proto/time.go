package proto

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Time32 represents duration in seconds.
type Time32 int32

// Time64 represents duration up until nanoseconds.
type Time64 int64

func (t Time32) String() string {
	d := time.Duration(t) * time.Second

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, secs)
}

func (t Time64) String() string {
	d := time.Duration(t)

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60
	nanos := d.Nanoseconds() % 1e9

	// NOTE(kavi): do we need multiple formatting depending on precision (3, 6, 9) instead of
	// always 9?
	return fmt.Sprintf("%02d:%02d:%02d.%09d", hours, minutes, secs, nanos)
}

func ParseTime32(s string) (Time32, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: %s", s)
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}

	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}

	seconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, err
	}

	totalSeconds := int32(hours*3600 + minutes*60 + seconds)
	return Time32(totalSeconds), nil
}

func ParseTime64(s string) (Time64, error) {
	// Parse time string like "12:34:56.789"
	timePart, fractionalStr, ok := strings.Cut(s, ".")
	if !ok {
		fractionalStr = ""
	}

	parts := strings.Split(timePart, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: %s", s)
	}

	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}

	minutes, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}

	seconds, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, err
	}

	// Calculate total seconds since midnight
	totalSeconds := int64(hours*3600 + minutes*60 + seconds)

	// Parse fractional part (default to nanoseconds scale)
	var fractional int64
	if fractionalStr != "" {
		// Pad or truncate to 9 digits (nanoseconds)
		for len(fractionalStr) < 9 {
			fractionalStr += "0"
		}
		if len(fractionalStr) > 9 {
			fractionalStr = fractionalStr[:9]
		}
		fractional, err = strconv.ParseInt(fractionalStr, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	// Store as decimal with nanosecond scale
	return Time64(totalSeconds*1e9 + fractional), nil
}

func IntoTime32(t time.Duration) Time32 {
	return Time32(int(t.Seconds()))
}

// IntoTime64 converts time.Time to Time64 with default precision (9 - nanoseconds)
func IntoTime64(t time.Duration) Time64 {
	return IntoTime64WithPrecision(t, PrecisionMax)
}

// IntoTime64WithPrecision converts time.Time to Time64 with specified precision
// Time64 stores time as a decimal with configurable scale, similar to DateTime64
func IntoTime64WithPrecision(d time.Duration, precision Precision) Time64 {
	res := truncateDuration(d, precision)
	return Time64(res)
}

func (t Time32) Duration() time.Duration {
	seconds := int32(t)
	return time.Second * time.Duration(seconds)
}

// ToTime converts Time64 to time.Time with default precision (9 - nanoseconds)
func (t Time64) Duration() time.Duration {
	return t.ToTimeWithPrecision(9)
}

// ToTimeWithPrecision converts Time64 to time.Time with specified precision
// Time64 stores time as a decimal with configurable scale, similar to DateTime64
func (t Time64) ToTimeWithPrecision(precision Precision) time.Duration {
	d := time.Duration(t)
	return truncateDuration(d, precision)
}

func truncateDuration(d time.Duration, precision Precision) time.Duration {
	var res time.Duration
	switch precision {
	case PrecisionSecond:
		res = d.Truncate(time.Second)
	case PrecisionMilli:
		res = d.Truncate(time.Millisecond)
	case PrecisionMicro:
		res = d.Truncate(time.Microsecond)
	// NOTE: NO additional case needed for PrecisionMax, given it's type alias for PrecisionNano
	case PrecisionNano:
		res = d
	default:
		// if wrong precision, treat it as Millisecond.
		res = d.Truncate(time.Millisecond)
	}

	return res
}
