package proto

import (
	"testing"
	"time"
)

func TestTime32_String(t *testing.T) {
	tests := []struct {
		name     string
		time     Time32
		expected string
	}{
		{"zero", Time32(0), "00:00:00"},
		{"one hour", Time32(3600), "01:00:00"},
		{"one minute", Time32(60), "00:01:00"},
		{"one second", Time32(1), "00:00:01"},
		{"complex time", Time32(3661), "01:01:01"}, // 1 hour, 1 minute, 1 second
		{"max time", Time32(86399), "23:59:59"},    // 23:59:59
		{"midnight", Time32(0), "00:00:00"},
		{"noon", Time32(43200), "12:00:00"}, // 12:00:00
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.time.String(); got != tt.expected {
				t.Errorf("Time32.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTime64_String(t *testing.T) {
	tests := []struct {
		name     string
		time     Time64
		expected string
	}{
		{"zero", Time64(0), "00:00:00.000000000"},
		{"one second", Time64(1e9), "00:00:01.000000000"},
		{"one minute", Time64(60e9), "00:01:00.000000000"},
		{"one hour", Time64(3600e9), "01:00:00.000000000"},
		{"with nanoseconds", Time64(3661e9 + 123456789), "01:01:01.123456789"},
		{"max time", Time64(86399e9 + 999999999), "23:59:59.999999999"},
		{"noon", Time64(43200e9), "12:00:00.000000000"},
		{"microsecond precision", Time64(1e6), "00:00:00.001000000"},
		{"millisecond precision", Time64(1e3), "00:00:00.000001000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.time.String(); got != tt.expected {
				t.Errorf("Time64.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseTime32(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    Time32
		expectError bool
	}{
		{"valid time", "12:34:56", Time32(12*3600 + 34*60 + 56), false},
		{"zero time", "00:00:00", Time32(0), false},
		{"max time", "23:59:59", Time32(23*3600 + 59*60 + 59), false},
		{"noon", "12:00:00", Time32(12 * 3600), false},
		{"midnight", "00:00:00", Time32(0), false},
		{"invalid format", "12:34", Time32(0), true},
		{"invalid format 2", "12:34:56:78", Time32(0), true},
		{"non-numeric", "ab:cd:ef", Time32(0), true},
		{"empty string", "", Time32(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTime32(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("ParseTime32() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("ParseTime32() unexpected error: %v", err)
				}
				if got != tt.expected {
					t.Errorf("ParseTime32() = %v, want %v", got, tt.expected)
				}
			}
		})
	}
}

func TestParseTime64(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    Time64
		expectError bool
	}{
		{"valid time", "12:34:56", Time64((12*3600 + 34*60 + 56) * 1e9), false},
		{"zero time", "00:00:00", Time64(0), false},
		{"max time", "23:59:59", Time64((23*3600 + 59*60 + 59) * 1e9), false},
		{"noon", "12:00:00", Time64(12 * 3600 * 1e9), false},
		{"midnight", "00:00:00", Time64(0), false},
		{"invalid format", "12:34", Time64(0), true},
		{"invalid format 2", "12:34:56:78", Time64(0), true},
		{"non-numeric", "ab:cd:ef", Time64(0), true},
		{"empty string", "", Time64(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTime64(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("ParseTime64() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("ParseTime64() unexpected error: %v", err)
				}
				if got != tt.expected {
					t.Errorf("ParseTime64() = %v, want %v", got, tt.expected)
				}
			}
		})
	}
}

func TestFromTime32(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected Time32
	}{
		{"zero time", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Time32(0)},
		{"noon", time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC), Time32(12 * 3600)},
		{"complex time", time.Date(1970, 1, 1, 13, 45, 30, 0, time.UTC), Time32(13*3600 + 45*60 + 30)},
		{"max time", time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), Time32(23*3600 + 59*60 + 59)},
		{"with nanoseconds", time.Date(1970, 1, 1, 12, 0, 0, 123456789, time.UTC), Time32(12 * 3600)},
		{"different date", time.Date(2023, 6, 15, 14, 30, 45, 0, time.UTC), Time32(14*3600 + 30*60 + 45)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromTime32(tt.input); got != tt.expected {
				t.Errorf("FromTime32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestFromTime64(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected Time64
	}{
		{"zero time", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Time64(0)},
		{"noon", time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC), Time64(12 * 3600 * 1e9)},
		{"complex time", time.Date(1970, 1, 1, 13, 45, 30, 0, time.UTC), Time64((13*3600 + 45*60 + 30) * 1e9)},
		{"max time", time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), Time64((23*3600 + 59*60 + 59) * 1e9)},
		{"with nanoseconds", time.Date(1970, 1, 1, 12, 0, 0, 123456789, time.UTC), Time64(12*3600*1e9 + 123456789)},
		{"different date", time.Date(2023, 6, 15, 14, 30, 45, 123456789, time.UTC), Time64((14*3600+30*60+45)*1e9 + 123456789)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromTime64(tt.input); got != tt.expected {
				t.Errorf("FromTime64() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTime32_ToTime32(t *testing.T) {
	tests := []struct {
		name     string
		time     Time32
		expected time.Time
	}{
		{"zero", Time32(0), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"noon", Time32(12 * 3600), time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"complex time", Time32(13*3600 + 45*60 + 30), time.Date(1970, 1, 1, 13, 45, 30, 0, time.UTC)},
		{"max time", Time32(23*3600 + 59*60 + 59), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.time.ToTime32(); !got.Equal(tt.expected) {
				t.Errorf("Time32.ToTime32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTime64_ToTime(t *testing.T) {
	tests := []struct {
		name     string
		time     Time64
		expected time.Time
	}{
		{"zero", Time64(0), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"noon", Time64(12 * 3600 * 1e9), time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"complex time", Time64((13*3600 + 45*60 + 30) * 1e9), time.Date(1970, 1, 1, 13, 45, 30, 0, time.UTC)},
		{"max time", Time64((23*3600 + 59*60 + 59) * 1e9), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC)},
		{"with nanoseconds", Time64(12*3600*1e9 + 123456789), time.Date(1970, 1, 1, 12, 0, 0, 123456789, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.time.ToTime(); !got.Equal(tt.expected) {
				t.Errorf("Time64.ToTime() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTime32_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		time Time32
	}{
		{"zero", Time32(0)},
		{"noon", Time32(12 * 3600)},
		{"complex time", Time32(13*3600 + 45*60 + 30)},
		{"max time", Time32(23*3600 + 59*60 + 59)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert to string
			str := tt.time.String()

			// Parse back
			parsed, err := ParseTime32(str)
			if err != nil {
				t.Errorf("ParseTime32() failed: %v", err)
			}

			if parsed != tt.time {
				t.Errorf("Round trip failed: original = %v, parsed = %v", tt.time, parsed)
			}
		})
	}
}

func TestTime64_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		time Time64
	}{
		{"zero", Time64(0)},
		{"noon", Time64(12 * 3600 * 1e9)},
		{"complex time", Time64((13*3600 + 45*60 + 30) * 1e9)},
		{"max time", Time64((23*3600 + 59*60 + 59) * 1e9)},
		{"with nanoseconds", Time64(12*3600*1e9 + 123456789)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert to string
			str := tt.time.String()

			// Parse back (note: ParseTime64 doesn't handle nanoseconds, so we test the seconds part)
			parsed, err := ParseTime64(str[:8]) // Take only HH:MM:SS part
			if err != nil {
				t.Errorf("ParseTime64() failed: %v", err)
			}

			// Compare seconds part
			expectedSeconds := int64(tt.time) / 1e9
			parsedSeconds := int64(parsed) / 1e9
			if parsedSeconds != expectedSeconds {
				t.Errorf("Round trip failed: original seconds = %v, parsed seconds = %v", expectedSeconds, parsedSeconds)
			}
		})
	}
}

func TestTime32_TimeConversion(t *testing.T) {
	tests := []struct {
		name string
		time Time32
	}{
		{"zero", Time32(0)},
		{"noon", Time32(12 * 3600)},
		{"complex time", Time32(13*3600 + 45*60 + 30)},
		{"max time", Time32(23*3600 + 59*60 + 59)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert to time.Time
			t1 := tt.time.ToTime32()

			// Convert back
			t2 := FromTime32(t1)

			if t2 != tt.time {
				t.Errorf("Time conversion failed: original = %v, converted back = %v", tt.time, t2)
			}
		})
	}
}

func TestTime64_TimeConversion(t *testing.T) {
	tests := []struct {
		name string
		time Time64
	}{
		{"zero", Time64(0)},
		{"noon", Time64(12 * 3600 * 1e9)},
		{"complex time", Time64((13*3600 + 45*60 + 30) * 1e9)},
		{"max time", Time64((23*3600 + 59*60 + 59) * 1e9)},
		{"with nanoseconds", Time64(12*3600*1e9 + 123456789)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert to time.Time
			t1 := tt.time.ToTime()

			// Convert back
			t2 := FromTime64(t1)

			if t2 != tt.time {
				t.Errorf("Time conversion failed: original = %v, converted back = %v", tt.time, t2)
			}
		})
	}
}

func BenchmarkTime32_String(b *testing.B) {
	t := Time32(12*3600 + 34*60 + 56) // 12:34:56
	for i := 0; i < b.N; i++ {
		_ = t.String()
	}
}

func BenchmarkTime64_String(b *testing.B) {
	t := Time64(12*3600*1e9 + 34*60*1e9 + 56*1e9 + 123456789) // 12:34:56.123456789
	for i := 0; i < b.N; i++ {
		_ = t.String()
	}
}

func BenchmarkParseTime32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseTime32("12:34:56")
	}
}

func BenchmarkParseTime64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseTime64("12:34:56")
	}
}

func BenchmarkFromTime32(b *testing.B) {
	t := time.Date(1970, 1, 1, 12, 34, 56, 0, time.UTC)
	for i := 0; i < b.N; i++ {
		_ = FromTime32(t)
	}
}

func BenchmarkFromTime64(b *testing.B) {
	t := time.Date(1970, 1, 1, 12, 34, 56, 123456789, time.UTC)
	for i := 0; i < b.N; i++ {
		_ = FromTime64(t)
	}
}
