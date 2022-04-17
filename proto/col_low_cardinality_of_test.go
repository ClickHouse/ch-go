package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestLowCardinalityOf(t *testing.T) {
	v := (&ColStr{}).LowCardinality()

	require.NoError(t, v.Prepare())
	require.Equal(t, ColumnType("LowCardinality(String)"), v.Type())
}

func TestLowCardinalityOfStr(t *testing.T) {
	col := (&ColStr{}).LowCardinality()
	col.AppendArr([]string{"foo", "bar", "foo", "foo", "baz"})

	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_low_cardinality_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := (&ColStr{}).LowCardinality()

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		require.Equal(t, ColumnType("LowCardinality(String)"), dec.Type())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := (&ColStr{}).LowCardinality()
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := (&ColStr{}).LowCardinality()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}
