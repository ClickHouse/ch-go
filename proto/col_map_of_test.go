package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestColMapOfGolden(t *testing.T) {
	v := ColMapOf[string, string]{
		Keys: &ColStr{}, Values: &ColStr{},
	}
	require.Equal(t, ColumnType("Map(String, String)"), v.Type())
	v.Append(map[string]string{
		"foo": "bar",
	})
	v.Append(map[string]string{
		"like": "100",
	})
	var buf Buffer
	v.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		var buf Buffer
		v.EncodeColumn(&buf)
		gold.Bytes(t, buf.Buf, "col_map_of_str_str")
	})
}

func TestColMapOf(t *testing.T) {
	v := ColMapOf[string, string]{
		Keys: &ColStr{}, Values: &ColStr{},
	}
	require.Equal(t, ColumnType("Map(String, String)"), v.Type())
	v.Append(map[string]string{
		"foo": "bar",
		"baz": "hello",
	})
	v.Append(map[string]string{
		"like":    "100",
		"dislike": "200",
		"result":  "1000 - 7",
	})
	const rows = 2

	var buf Buffer
	v.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := &ColMapOf[string, string]{
			Keys: &ColStr{}, Values: &ColStr{},
		}
		require.NoError(t, dec.DecodeColumn(r, rows))
		for i := 0; i < rows; i++ {
			require.Equal(t, v.Row(i), v.Row(i))
		}
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := &ColMapOf[string, string]{
			Keys: &ColStr{}, Values: &ColStr{},
		}
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := &ColMapOf[string, string]{
			Keys: &ColStr{}, Values: &ColStr{},
		}
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}
