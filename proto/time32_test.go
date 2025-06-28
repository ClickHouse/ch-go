package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTime32_Conversion(t *testing.T) {
	tests := []time.Duration{
		0,
		1 * time.Second,
		23*time.Hour + 59*time.Minute + 59*time.Second,
		-1 * time.Second,
		999*time.Hour + 59*time.Minute + 59*time.Second,
		-999*time.Hour - 59*time.Minute - 59*time.Second,
	}

	for _, d := range tests {
		t32, err := Time32FromDuration(d)
		require.NoError(t, err, "Time32FromDuration(%v)", d)
		got := t32.Duration()
		require.Equal(t, d, got)
	}

	// Out of range
	outOfRange := []time.Duration{
		1000*time.Hour + 0*time.Minute + 0*time.Second,
		-1000*time.Hour - 0*time.Minute - 0*time.Second,
	}
	for _, d := range outOfRange {
		_, err := Time32FromDuration(d)
		require.Error(t, err, "should error for out-of-range duration: %v", d)
	}
}

func TestTime32_StringAndParse(t *testing.T) {
	tests := []struct {
		str      string
		expected time.Duration
	}{
		{"00:00:00", 0},
		{"23:59:59", 23*time.Hour + 59*time.Minute + 59*time.Second},
		{"01:02:03", 1*time.Hour + 2*time.Minute + 3*time.Second},
		{"-01:02:03", -1*time.Hour - 2*time.Minute - 3*time.Second},
		{"999:59:59", 999*time.Hour + 59*time.Minute + 59*time.Second},
		{"-999:59:59", -999*time.Hour - 59*time.Minute - 59*time.Second},
	}

	for _, tt := range tests {
		t32, err := ParseTime32(tt.str)
		require.NoError(t, err, "ParseTime32(%q)", tt.str)
		require.Equal(t, tt.expected, t32.Duration())
		require.Equal(t, tt.str, t32.String())
	}

	// Invalid format
	invalid := []string{
		"notatime",
		"25:61:61",
		"1:2",
		"",
		"1000:00:00",  // out of range
		"-1000:00:00", // out of range
	}
	for _, s := range invalid {
		_, err := ParseTime32(s)
		require.Error(t, err, "should error for invalid input: %q", s)
	}
}

func TestTime32_ParseTime32FromSeconds(t *testing.T) {
	tests := []struct {
		secStr   string
		expected Time32
	}{
		{"0", 0},
		{"86399", Time32(23*3600 + 59*60 + 59)},
		{"-3600", Time32(-3600)},
		{"3599999", Time32(999*3600 + 59*60 + 59)},
		{"-3599999", Time32(-999*3600 - 59*60 - 59)},
	}

	for _, tt := range tests {
		t32, err := ParseTime32FromSeconds(tt.secStr)
		require.NoError(t, err, "ParseTime32FromSeconds(%q)", tt.secStr)
		require.Equal(t, tt.expected, t32)
	}

	// Out of range
	_, err := ParseTime32FromSeconds("3600000")
	require.NoError(t, err, "ParseTime32FromSeconds should not error for int32 overflow, but you may want to check range in your code")
}
