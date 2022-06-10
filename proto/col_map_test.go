package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColMap_EncodeColumn(t *testing.T) {
	const rows = 2
	var (
		keys   = &ColStr{}
		values = &ColStr{}
		data   = &ColMap{
			Keys:   keys,
			Values: values,
		}
	)

	for _, v := range []struct {
		Key, Value string
	}{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	} {
		keys.Append(v.Key)
		values.Append(v.Value)
	}
	data.Offsets = ColUInt64{
		2, // [0:2]
		3, // [2:3]
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_map_str_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := &ColMap{
			Keys:   &ColStr{},
			Values: &ColStr{},
		}
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeMap.Sub(ColumnTypeString, ColumnTypeString), dec.Type())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		dec := &ColMap{
			Keys:   &ColStr{},
			Values: &ColStr{},
		}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := &ColMap{
			Keys:   &ColStr{},
			Values: &ColStr{},
		}
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}
