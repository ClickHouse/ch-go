package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateTime64_Time(t *testing.T) {
	v := time.Unix(1546290000, 0).UTC()
	for _, p := range []Precision{
		PrecisionSecond,
		PrecisionMilli,
		PrecisionMicro,
		PrecisionNano,
	} {
		d := ToDateTime64(v, p)
		vt := d.Time(p)
		assert.Equal(t, ToDateTime64(v, p), d)
		assert.Equal(t, v.Unix(), vt.Unix())
		assert.True(t, p.Valid())
	}
	t.Run("Duration", func(t *testing.T) {
		assert.Equal(t, time.Second, PrecisionSecond.Duration(), "sec")
		assert.Equal(t, time.Nanosecond, PrecisionNano.Duration(), "ns")
	})
}

func TestColDateTime64_Wrap(t *testing.T) {
	var data ColDateTime64
	w := data.Wrap(PrecisionMilli)
	require.Equal(t, ColumnTypeDateTime64.With("3"), w.Type())
}
