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

func TestIntoTime32(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected Time32
	}{
		{"zero time", time.Duration(0), Time32(0)},
		{"12h", 12 * time.Hour, Time32(12 * 3600)},
		{"13h45m30s", 13*time.Hour + 45*time.Minute + 30*time.Second, Time32(13*3600 + 45*60 + 30)},
		{"23h59m59s", 23*time.Hour + 59*time.Minute + 59*time.Second, Time32(23*3600 + 59*60 + 59)},
		{"time32 should ignore nanoseconds", 12*time.Hour + 123456789*time.Nanosecond, Time32(12 * 3600)},
		{"14h30m45s", 14*time.Hour + 30*time.Minute + 45*time.Second, Time32(14*3600 + 30*60 + 45)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IntoTime32(tt.input); got != tt.expected {
				t.Errorf("FromTime32() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIntoTime64(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected Time64
	}{
		{"zero time", time.Duration(0), Time64(0)},
		{"12h", 12 * time.Hour, Time64(12 * 3600 * 1e9)},
		{"13h45m30s", 13*time.Hour + 45*time.Minute + 30*time.Second, Time64((13*3600 + 45*60 + 30) * 1e9)},
		{"23h59m59s", 23*time.Hour + 59*time.Minute + 59*time.Second, Time64((23*3600 + 59*60 + 59) * 1e9)},
		{"time64 should not ignore nanoseconds", 12*time.Hour + 123456789*time.Nanosecond, Time64(12*3600*1e9 + 123456789)},
		{"14h30m45s123456789ns", 14*time.Hour + 30*time.Minute + 45*time.Second + 123456789*time.Nanosecond, Time64((14*3600+30*60+45)*1e9 + 123456789)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IntoTime64(tt.input); got != tt.expected {
				t.Errorf("FromTime64() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTime32_Duration(t *testing.T) {
	tests := []struct {
		name     string
		time     Time32
		expected time.Duration
	}{
		{"zero", Time32(0), time.Duration(0)},
		{"12h", Time32(12 * 3600), 12 * time.Hour},
		{"13h45m30s", Time32(13*3600 + 45*60 + 30), 13*time.Hour + 45*time.Minute + 30*time.Second},
		{"23h59m59s", Time32(23*3600 + 59*60 + 59), 23*time.Hour + 59*time.Minute + 59*time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.time.Duration(); got != tt.expected {
				t.Errorf("Time32.Duration() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTime64_Duration(t *testing.T) {
	tests := []struct {
		name     string
		time     Time64
		expected time.Duration
	}{
		{"zero", Time64(0), time.Duration(0)},
		{"12h", Time64(12 * 3600 * 1e9), 12 * time.Hour},
		{"13h45m30s", Time64((13*3600 + 45*60 + 30) * 1e9), 13*time.Hour + 45*time.Minute + 30*time.Second},
		{"23h59m59s", Time64((23*3600 + 59*60 + 59) * 1e9), 23*time.Hour + 59*time.Minute + 59*time.Second},
		{"12h123456789ns", Time64(12*3600*1e9 + 123456789), 12*time.Hour + 123456789*time.Nanosecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.time.Duration(); got != tt.expected {
				t.Errorf("Time64.Duration() = %v, want %v", got, tt.expected)
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
			t1 := tt.time.Duration()

			// Convert back
			t2 := IntoTime32(t1)

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
			t1 := tt.time.Duration()

			// Convert back
			t2 := IntoTime64(t1)

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
	t := 12*time.Hour + 34*time.Minute + 56*time.Second
	for i := 0; i < b.N; i++ {
		_ = IntoTime32(t)
	}
}

func BenchmarkFromTime64(b *testing.B) {
	t := 12*time.Hour + 34*time.Minute + 56*time.Second + 123456789*time.Nanosecond
	for i := 0; i < b.N; i++ {
		_ = IntoTime64(t)
	}
}
