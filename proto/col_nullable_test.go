package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestColNullable_EncodeColumn(t *testing.T) {
	const rows = 10
	var (
		rawValues = &ColStr{}
		data      = &ColNullable{
			Values: rawValues,
		}
	)
	type nullStr struct {
		Null  bool
		Value string
	}
	values := []nullStr{
		{Value: "value1"},
		{Value: "value2"},
		{Null: true},
		{Value: "value3"},
		{Null: true},
		{Value: ""},
		{Value: ""},
		{Value: "value4"},
		{Null: true},
		{Value: "value54"},
	}
	for _, v := range values {
		rawValues.Append(v.Value)
		if v.Null {
			data.Nulls = append(data.Nulls, 1)
		} else {
			data.Nulls = append(data.Nulls, 0)
		}
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_nullable_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := &ColNullable{Values: new(ColStr)}
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		for i, v := range values {
			got := nullStr{
				Null:  dec.IsElemNull(i),
				Value: dec.Values.(*ColStr).Row(i),
			}
			require.Equal(t, v, got, "[%d]", i)
		}
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Nullable(String)"), dec.Type())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := &ColNullable{Values: new(ColStr)}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := &ColNullable{Values: new(ColStr)}
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}
