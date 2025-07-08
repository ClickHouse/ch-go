package proto

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Time32 represents ClickHouse Time as seconds since midnight (int32).
type Time32 int32

// Allowed ranges
const maxTime32 = 999*3600 + 59*60 + 59
const minTime32 = -maxTime32

// Time32FromDuration converts a time.Duration (since midnight) to Time32 (seconds since midnight).
func Time32FromDuration(d time.Duration) (Time32, error) {
	secs := int64(d / time.Second)
	if secs < minTime32 || secs > maxTime32 {
		return 0, fmt.Errorf("Time32 out of range: %d", secs)
	}
	return Time32(secs), nil
}

// Duration returns the time.Duration value for this Time32.
func (t Time32) Duration() time.Duration {
	return time.Duration(t) * time.Second
}

// String returns a string representation of Time32 as "HH:MM:SS".
func (t Time32) String() string {
	d := t.Duration()
	neg := d < 0
	if neg {
		d = -d
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if neg {
		return fmt.Sprintf("-%02d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

// ParseTime32 parses a string in "HH:MM:SS" or "-HH:MM:SS" format to Time32.
func ParseTime32(s string) (Time32, error) {
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid hour: %w", err)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid minute: %w", err)
	}
	sec, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, fmt.Errorf("invalid second: %w", err)
	}

	// Add validation for proper HH:MM:SS format
	if h < 0 || m < 0 || sec < 0 || m >= 60 || sec >= 60 {
		return 0, fmt.Errorf("invalid time values: %02d:%02d:%02d", h, m, sec)
	}

	total := h*3600 + m*60 + sec
	if neg {
		total = -total
	}
	return Time32FromDuration(time.Duration(total) * time.Second)
}

// ParseTime32FromSeconds parses a string representing seconds since midnight to Time32.
func ParseTime32FromSeconds(s string) (Time32, error) {
	secs, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return Time32(secs), nil
}
