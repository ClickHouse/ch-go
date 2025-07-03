package proto

import (
    "testing"
    "time"

    "github.com/stretchr/testify/require"
)

func TestTime64_Conversion(t *testing.T) {
    for _, p := range []Precision{0, 3, 6, 9} {
        d := 13*time.Hour + 5*time.Minute + 7*time.Second + 123*time.Millisecond
        t64 := Time64FromDuration(d, p)
        got := t64.Duration(p)
        // Compare after round-trip through Time64
        require.Equal(t, t64, Time64FromDuration(got, p))
    }
}

func TestTime64_StringAndParse(t *testing.T) {
	tests := []struct {
		d        time.Duration
		prec     Precision
		expected string
	}{
		{13*time.Hour + 5*time.Minute + 7*time.Second, 0, "13:05:07"},
		{13*time.Hour + 5*time.Minute + 7*time.Second + 120*time.Millisecond, 3, "13:05:07.120"},
		{13*time.Hour + 5*time.Minute + 7*time.Second + 120*time.Millisecond, 6, "13:05:07.120000"},
		{13*time.Hour + 5*time.Minute + 7*time.Second + 123456*time.Microsecond, 6, "13:05:07.123456"},
	}

    for _, tt := range tests {
        t64 := Time64FromDuration(tt.d, tt.prec)
        str := t64.String(tt.prec)
        require.Equal(t, tt.expected, str)

        parsed, err := ParseTime64(str, tt.prec)
        require.NoError(t, err)
        require.Equal(t, parsed, Time64FromDuration(parsed.Duration(tt.prec), tt.prec))
    }
}

func TestTime64_Parse_Invalid(t *testing.T) {
    _, err := ParseTime64("not-a-time", 3)
    require.Error(t, err)
    _, err = ParseTime64("25:61:61", 3)
    require.Error(t, err)
}

func TestTime64_Precision_Invalid(t *testing.T) {
    require.Panics(t, func() {
        Time64FromDuration(time.Second, 42)
    })
    require.Panics(t, func() {
        var t Time64
        _ = t.Duration(42)
    })
    require.Panics(t, func() {
        var t Time64
        _ = t.String(42)
    })
}