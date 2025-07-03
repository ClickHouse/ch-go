package proto

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Time64 represents ClickHouse Time64 wire value (fractional seconds since midnight).
type Time64 int64

// Time64FromDuration converts a time.Duration (since midnight) to Time64 with the given precision.
func Time64FromDuration(d time.Duration, p Precision) Time64 {
	if !p.Valid() {
		panic(fmt.Sprintf("invalid precision: %d", p))
	}
	return Time64(d.Nanoseconds() / p.Scale())
}

// Duration returns the time.Duration value for this Time64, given the precision.
func (t Time64) Duration(p Precision) time.Duration {
	if !p.Valid() {
		panic(fmt.Sprintf("invalid precision: %d", p))
	}
	return time.Duration(int64(t) * p.Scale())
}

// String returns a string representation of Time64 as "HH:MM:SS.sss..." (with given precision).
func (t Time64) String(p Precision) string {
    d := t.Duration(p)
    h := int(d.Hours())
    m := int(d.Minutes()) % 60
    s := int(d.Seconds()) % 60
    if p == 0 {
        return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
    }
    frac := int64(d) % int64(time.Second)
    fracVal := frac / p.Scale()
    return fmt.Sprintf("%02d:%02d:%02d.%0*d", h, m, s, int(p), fracVal)
}

// ParseTime64 parses a string in "HH:MM:SS[.fff...]" format to Time64 with the given precision.
func ParseTime64(s string, p Precision) (Time64, error) {
    parts := strings.SplitN(s, ".", 2)
    t, err := time.Parse("15:04:05", parts[0])
    if err != nil {
        return 0, err
    }
    d := time.Duration(t.Hour())*time.Hour +
        time.Duration(t.Minute())*time.Minute +
        time.Duration(t.Second())*time.Second
    if len(parts) == 2 && p > 0 {
        fracStr := parts[1]
        // Right-pad with zeros if fractional part is shorter than precision
        if len(fracStr) < int(p) {
            fracStr += strings.Repeat("0", int(p)-len(fracStr))
        }
        // Truncate if fractional part is longer than precision
        if len(fracStr) > int(p) {
            fracStr = fracStr[:p]
        }
        frac, err := strconv.ParseInt(fracStr, 10, 64)
        if err != nil {
            return 0, err
        }
        // Correctly scale the fractional part by multiplying with the precision's scale factor
        d += time.Duration(frac) * time.Duration(p.Scale())
    }
    return Time64FromDuration(d, p), nil
}
