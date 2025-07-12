package proto

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Time32 represents time of day in seconds since midnight.
type Time32 int32

// Time64 represents time of day with precision in nanoseconds since midnight.
type Time64 int64

func (t Time32) String() string {
	seconds := int32(t)
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, secs)
}

func (t Time64) String() string {
	seconds := int64(t) / 1e9
	nanos := int64(t) % 1e9
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
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

	totalSeconds := int64(hours*3600 + minutes*60 + seconds)
	return Time64(totalSeconds * 1e9), nil
}

func FromTime32(t time.Time) Time32 {
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	totalSeconds := int32(hour*3600 + minute*60 + second)
	return Time32(totalSeconds)
}

func FromTime64(t time.Time) Time64 {
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	nanosecond := t.Nanosecond()
	totalSeconds := int64(hour*3600 + minute*60 + second)
	totalNanos := totalSeconds*1e9 + int64(nanosecond)
	return Time64(totalNanos)
}

func (t Time32) ToTime32() time.Time {
	seconds := int32(t)
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	return time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), 0, time.UTC)
}

func (t Time64) ToTime() time.Time {
	seconds := int64(t) / 1e9
	nanos := int64(t) % 1e9
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60
	return time.Date(1970, 1, 1, int(hours), int(minutes), int(secs), int(nanos), time.UTC)
}
