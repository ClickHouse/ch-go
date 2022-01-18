package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDate_Time(t *testing.T) {
	t.Run("Single", func(t *testing.T) {
		v := time.Date(2011, 10, 10, 14, 59, 31, 401235, time.UTC)
		d := ToDate(v)
		assert.Equal(t, Date(15257), d)
		assert.Equal(t, NewDate(2011, 10, 10), d)
		assert.Equal(t, d.String(), "2011-10-10")
		assert.Equal(t, d, ToDate(d.Time()))
	})
	t.Run("Range", func(t *testing.T) {
		var (
			start = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			end   = time.Date(2148, 1, 1, 0, 0, 0, 0, time.UTC)
		)
		for v := start; v.Before(end); v = v.AddDate(0, 0, 1) {
			date := ToDate(v)
			newTime := date.Time()
			assert.True(t, newTime.Equal(v))
			newDate := NewDate(newTime.Year(), newTime.Month(), newTime.Day())
			assert.Equal(t, date, newDate)
			assert.Equal(t, v.Format("2006-01-02"), date.String())
		}
	})
}

func BenchmarkDate_Time(b *testing.B) {
	b.ReportAllocs()

	v := Date(100)
	var t time.Time
	for i := 0; i < b.N; i++ {
		t = v.Time()
	}
	_ = t.IsZero()
}
