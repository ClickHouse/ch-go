package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColNullableOf(t *testing.T) {
	col := &ColNullableOf[string]{
		Values: new(ColStr),
	}
	v := []Nullable[string]{
		NewNullable("foo"),
		Null[string](),
		NewNullable("bar"),
		NewNullable("baz"),
	}
	col.AppendArr(v)

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_nullable_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := &ColNullableOf[string]{Values: new(ColStr)}

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		for i, s := range v {
			assert.Equal(t, s, col.Row(i))
		}
		assert.Equal(t, ColumnType("Nullable(String)"), dec.Type())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := &ColNullableOf[string]{Values: new(ColStr)}
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := &ColNullableOf[string]{Values: new(ColStr)}
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}
