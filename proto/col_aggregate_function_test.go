package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColAggregateFunction_EncodeColumn(t *testing.T) {
	const columnType = ColumnType("AggregateFunction(quantilesDD(0.01, 0.5, 0.99), Float64)")
	data := NewAggregateFunction[DDSketch](columnType)
	for _, tt := range ddSketchServerStates {
		data.Append(tt.sketch)
	}
	rows := data.Rows()

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_aggregate_function_ddsketch")
	})
	t.Run("Type", func(t *testing.T) {
		require.Equal(t, columnType, data.Type())
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := NewAggregateFunction[DDSketch](columnType)
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		for i, tt := range ddSketchServerStates {
			require.Equal(t, tt.sketch, dec.Row(i))
		}

		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, columnType, dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		dec := NewAggregateFunction[DDSketch](columnType)
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := NewAggregateFunction[DDSketch](columnType)
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
	t.Run("WriteColumn", checkWriteColumn(data))
}
