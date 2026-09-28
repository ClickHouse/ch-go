package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDateTime64_Time(t *testing.T) {
	for _, p := range []Precision{
		PrecisionSecond,
		1,
		PrecisionMilli,
		PrecisionMicro,
		PrecisionNano,
		8,
	} {
		t.Run(p.Duration().String(), func(t *testing.T) {
			for _, v := range []time.Time{
				time.Unix(0, 0).UTC(), // zero time
				time.Unix(1546290000, 0).UTC(),
			} {
				d := ToDateTime64(v, p)
				vt := d.Time(p)
				assert.Equal(t, ToDateTime64(v, p), d)
				assert.Equal(t, v.Unix(), vt.Unix())
				assert.True(t, p.Valid())
			}
		})

		t.Run("Zero_"+p.Duration().String(), func(t *testing.T) {
			t1 := time.Time{}
			t2 := time.Unix(0, 0).UTC()
			d1 := ToDateTime64(t1, p)
			d2 := ToDateTime64(t2, p)
			vt1 := d1.Time(p)
			vt2 := d2.Time(p)

			assert.True(t, t1.IsZero())
			assert.False(t, t2.IsZero())
			assert.Equal(t, d1, d2)
			assert.Equal(t, vt1.Unix(), int64(0))
			assert.Equal(t, vt2.Unix(), int64(0))
		})
	}
	t.Run("Duration", func(t *testing.T) {
		assert.Equal(t, time.Second, PrecisionSecond.Duration(), "sec")
		assert.Equal(t, time.Nanosecond, PrecisionNano.Duration(), "ns")
	})
}

// TestDateTime64_FarFuture guards against the t.UnixNano() overflow: int64 nanoseconds wrap just
// past 2262-04-11, which corrupted any later timestamp on both the write and read paths. For
// precisions coarser than a nanosecond the int64 tick count spans the full DateTime64 range, so
// values such as ClickHouse's 9999-12-31 ceiling must survive a round-trip.
func TestDateTime64_FarFuture(t *testing.T) {
	values := []time.Time{
		time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC), // last value the old UnixNano path got right
		time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC), // first value it corrupted
		time.Date(2999, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), // near ClickHouse's DateTime64 ceiling
	}
	// Precisions 8 and 9 are excluded: their int64 tick count tops out near years ~4900 and ~2262
	// respectively (the same limits ClickHouse's DateTime64(8)/(9) carry), so they cannot represent
	// these values by construction.
	for _, p := range []Precision{PrecisionSecond, 1, PrecisionMilli, PrecisionMicro, 7} {
		t.Run(p.Duration().String(), func(t *testing.T) {
			for _, v := range values {
				got := ToDateTime64(v, p).Time(p)
				assert.Truef(t, got.Equal(v), "precision %d: %s round-tripped to %s", p, v, got)
			}
		})
	}

	// Sub-second precision survives too: 9999-12-31 23:59:59.999 in DateTime64(3).
	t.Run("millisecond_fraction", func(t *testing.T) {
		v := time.Date(9999, 12, 31, 23, 59, 59, 999_000_000, time.UTC)
		got := ToDateTime64(v, PrecisionMilli).Time(PrecisionMilli)
		assert.True(t, got.Equal(v), got.String())
	})
}
