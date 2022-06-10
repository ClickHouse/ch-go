package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

// testColumn tests column implementation.
func testColumn[T any](t *testing.T, name string, f func() ColumnOf[T], values ...T) {
	data := f()

	for _, v := range values {
		data.Append(v)
	}
	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "column_of_"+name)
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := f()
		require.NoError(t, dec.DecodeColumn(r, len(values)))
		require.Equal(t, data, dec)
	})
}

func TestColumnOfString(t *testing.T) {
	testColumn(t, "str", func() ColumnOf[string] { return new(ColStr) }, "foo", "bar", "baz")
}

func TestColArrFrom(t *testing.T) {
	var data ColStr
	arr := data.Array()
	arr.Append([]string{"foo", "bar"})
	t.Logf("%T %+v", arr.Data, arr.Data)

	_ = ArrayOf[string](new(ColStr))

	arrArr := ArrayOf[[]string](data.Array())
	arrArr.Append([][]string{
		{"foo", "bar"},
		{"baz"},
	})
	t.Log(arrArr.Type())
	_ = arrArr
}

func TestColArrOfStr(t *testing.T) {
	col := (&ColStr{}).Array()
	col.Append([]string{"foo", "bar", "foo", "foo", "baz"})
	col.Append([]string{"foo", "baz"})

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_of_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := (&ColStr{}).Array()

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		require.Equal(t, ColumnType("Array(String)"), dec.Type())
		require.Equal(t, []string{"foo", "bar", "foo", "foo", "baz"}, dec.Row(0))
		require.Equal(t, []string{"foo", "baz"}, dec.Row(1))
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := (&ColStr{}).Array()
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := (&ColStr{}).Array()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}

func TestArrOfLowCordStr(t *testing.T) {
	col := ArrayOf[string](new(ColStr).LowCardinality())
	col.Append([]string{"foo", "bar", "foo", "foo", "baz"})
	col.Append([]string{"foo", "baz"})

	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_of_low_cord_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := ArrayOf[string](new(ColStr).LowCardinality())

		require.NoError(t, dec.DecodeColumn(r, col.Rows()))
		require.Equal(t, col.Rows(), dec.Rows())
		require.Equal(t, ColumnType("Array(LowCardinality(String))"), dec.Type())
		require.Equal(t, []string{"foo", "bar", "foo", "foo", "baz"}, dec.Row(0))
		require.Equal(t, []string{"foo", "baz"}, dec.Row(1))
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := ArrayOf[string](new(ColStr).LowCardinality())
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := ArrayOf[string](new(ColStr).LowCardinality())
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}
