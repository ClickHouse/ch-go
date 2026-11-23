package proto

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDate32_Time(t *testing.T) {
	t.Parallel()
	t.Run("Single", func(t *testing.T) {
		v := time.Date(2011, 10, 10, 14, 59, 31, 401235, time.UTC)
		d := ToDate32(v)
		assert.Equal(t, Date32(15257), d) // SELECT toInt64(Date32('2011-10-10'))
		assert.Equal(t, NewDate32(2011, 10, 10), d)
		assert.Equal(t, d.String(), "2011-10-10")
		assert.Equal(t, d, ToDate32(d.Time()))
	})
	t.Run("Range", func(t *testing.T) {
		t.Parallel()
		var (
			start = time.Date(1925, 1, 1, 0, 0, 0, 0, time.UTC)
			end   = time.Date(2283, 11, 11, 0, 0, 0, 0, time.UTC)
		)
		for v := start; v.Before(end); v = v.AddDate(0, 0, 1) {
			date := ToDate32(v)
			newTime := date.Time()
			require.True(t, newTime.Equal(v))
			newDate32 := NewDate32(newTime.Year(), newTime.Month(), newTime.Day())
			require.Equal(t, date, newDate32)
			require.Equal(t, v.Format("2006-01-02"), date.String())
		}
	})
}

func TestToDate32(t *testing.T) {
	const secInMinute = 60
	const secInHour = 60 * secInMinute
	for _, tc := range []struct {
		name     string
		Value    time.Time
		expected *Date32
	}{
		{
			name:  "2006-01-02T06:04:03+07:00",
			Value: time.Date(2006, 1, 2, 6, 4, 3, 0, time.FixedZone("UTC+7", 7*secInHour)),
		},
		{
			name:  "2008-01-02T06:44:15+03:00",
			Value: time.Date(2008, 1, 2, 6, 44, 15, 0, time.FixedZone("UTC+3", 3*secInHour)),
		},
		{
			name:  "2009-01-01T06:03:31+12:00",
			Value: time.Date(2009, 1, 1, 6, 3, 31, 0, time.FixedZone("UTC+12", 12*secInHour)),
		},
		{
			name:  "2006-12-31T22:04:41-06:30",
			Value: time.Date(2006, 12, 31, 22, 4, 41, 0, time.FixedZone("UTC-6:30", -6*secInHour-30*secInMinute)),
		},
		{
			name:  "1969-12-31T00:00:00Z",
			Value: time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "1969-12-31T12:00:00Z",
			Value: time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC),
		},
		{
			name:  "1969-12-31T23:59:59Z",
			Value: time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC),
		},
		{
			name:  "1969-12-31T22:04:41-06:30",
			Value: time.Date(1969, 12, 31, 22, 4, 41, 0, time.FixedZone("UTC-6:30", -6*secInHour-30*secInMinute)),
		},
		{
			name:  "1925-01-01T06:44:15+03:00",
			Value: time.Date(1925, 1, 1, 6, 44, 15, 0, time.FixedZone("UTC+3", 3*secInHour)),
		},
		{
			name:  "1970-01-01T00:30:00+02:00",
			Value: time.Date(1970, 1, 1, 0, 30, 0, 0, time.FixedZone("UTC+2", 2*secInHour)),
		},
		{
			name:     "zero value",
			Value:    time.Time{},
			expected: new(Date32),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ToDate32(tc.Value)
			var expected Date32
			if tc.expected != nil {
				expected = *tc.expected
			} else {
				expected = NewDate32(tc.Value.Year(), tc.Value.Month(), tc.Value.Day())
				assert.Equal(t, tc.Value.Format(DateLayout), d.String())
			}
			assert.Equal(t, expected.String(), d.String())
			assert.Equal(t, expected, d)
		})
	}
}

func TestColDate32_PreEpoch(t *testing.T) {
	t.Parallel()
	inputs := []time.Time{
		time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC),
		time.Date(1925, 5, 10, 15, 30, 0, 0, time.FixedZone("UTC+3", 3*3600)),
	}
	var col ColDate32
	col.Append(inputs[0])
	col.AppendArr(inputs[1:])

	var buf Buffer
	col.EncodeColumn(&buf)

	var dec ColDate32
	require.NoError(t, dec.DecodeColumn(NewReader(bytes.NewReader(buf.Buf)), len(inputs)))
	require.Equal(t, len(inputs), dec.Rows())
	for i, in := range inputs {
		require.Equal(t, in.Format(DateLayout), dec.Row(i).Format(DateLayout))
	}
}

func BenchmarkDate32_Time(b *testing.B) {
	b.ReportAllocs()

	v := Date32(100)
	var t time.Time
	for i := 0; i < b.N; i++ {
		t = v.Time()
	}
	_ = t.IsZero()
}
