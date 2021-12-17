package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestColLowCardinality_DecodeColumn(t *testing.T) {
	t.Run("Str", func(t *testing.T) {
		const rows = 50
		var data ColStr
		for _, v := range []string{
			"neo",
			"trinity",
			"morpheus",
		} {
			data.Append(v)
		}
		col := &ColLowCardinality{
			Index: &data,
			Key:   KeyUInt8,
		}
		for i := 0; i < rows; i++ {
			col.AppendKey(i % data.Rows())
		}

		var buf Buffer
		col.EncodeColumn(&buf)
		t.Run("Golden", func(t *testing.T) {
			gold.Bytes(t, buf.Buf, "col_low_cardinality_i_str_k_8")
		})
		t.Run("Ok", func(t *testing.T) {
			br := bytes.NewReader(buf.Buf)
			r := NewReader(br)
			dec := &ColLowCardinality{
				Index: &data,
			}
			require.NoError(t, dec.DecodeColumn(r, rows))
			require.Equal(t, col, dec)
			require.Equal(t, rows, dec.Rows())
			dec.Reset()
			require.Equal(t, 0, dec.Rows())
			require.Equal(t, ColumnTypeLowCardinality.Sub(ColumnTypeString), dec.Type())
		})
		t.Run("ErrUnexpectedEOF", func(t *testing.T) {
			r := NewReader(bytes.NewReader(nil))
			dec := &ColLowCardinality{
				Index: &data,
			}
			require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
		})
		t.Run("NoShortRead", func(t *testing.T) {
			dec := &ColLowCardinality{
				Index: &data,
			}
			requireNoShortRead(t, buf.Buf, colAware(dec, rows))
		})
	})
	t.Run("Blank", func(t *testing.T) {
		// Blank columns (i.e. row count is zero) are not encoded.
		var data ColStr
		col := &ColLowCardinality{
			Index: &data,
			Key:   KeyUInt8,
		}
		var buf Buffer
		col.EncodeColumn(&buf)

		var dec ColLowCardinality
		require.NoError(t, dec.DecodeColumn(buf.Reader(), col.Rows()))
	})
	t.Run("InvalidVersion", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(2)
		var dec ColLowCardinality
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidMeta", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(0)
		var dec ColLowCardinality
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidKeyType", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(cardinalityUpdateAll | int64(KeyUInt64+1))
		var dec ColLowCardinality
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
}
