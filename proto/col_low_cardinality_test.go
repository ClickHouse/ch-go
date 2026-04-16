package proto

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestLowCardinalityOf(t *testing.T) {
	v := NewLowCardinality[string](new(ColStr))

	require.NoError(t, v.Prepare())
	require.Equal(t, ColumnType("LowCardinality(String)"), v.Type())
}

func TestLowCardinalityOfStr(t *testing.T) {
	col := (&ColStr{}).LowCardinality()
	v := []string{"foo", "bar", "foo", "foo", "baz"}
	col.AppendArr(v)

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
		for i, s := range v {
			assert.Equal(t, s, col.Row(i))
		}
		assert.Equal(t, ColumnType("LowCardinality(String)"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := (&ColStr{}).LowCardinality()
		require.ErrorIs(t, dec.DecodeColumn(r, col.Rows()), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := (&ColStr{}).LowCardinality()
		requireNoShortRead(t, buf.Buf, colAware(dec, col.Rows()))
	})
}

func TestArrLowCardinalityStr(t *testing.T) {
	// Array(LowCardinality(String))
	data := [][]string{
		{"foo", "bar", "baz"},
		{"foo"},
		{"bar", "bar"},
		{"foo", "foo"},
		{"bar", "bar", "bar", "bar"},
	}
	col := new(ColStr).LowCardinality().Array()
	rows := len(data)
	for _, v := range data {
		col.Append(v)
	}
	require.NoError(t, col.Prepare())

	var buf Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_low_cardinality_u8_str")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)
		dec := new(ColStr).LowCardinality().Array()
		require.NoError(t, dec.DecodeColumn(r, rows))
		requireEqual[[]string](t, col, dec)
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnType("Array(LowCardinality(String))"), dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		dec := new(ColStr).LowCardinality().Array()
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := new(ColStr).LowCardinality().Array()
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
	t.Run("WriteColumn", checkWriteColumn(col))
}

func TestColLowCardinalityReuse(t *testing.T) {
	t.Run("ResetBetweenPrepare", func(t *testing.T) {
		col := new(ColStr).LowCardinality()

		// First use
		col.Append("hello")
		col.Append("world")
		require.NoError(t, col.Prepare())
		require.Equal(t, 2, col.Rows())

		// Reset and reuse
		col.Reset()

		// Second use with different values
		col.Append("foo")
		col.Append("bar")
		require.NoError(t, col.Prepare())
		require.Equal(t, 2, col.Rows())

		// Verify the actual values are correct after reuse
		require.Equal(t, "foo", col.Row(0))
		require.Equal(t, "bar", col.Row(1))

		// Verify round-trip encoding/decoding
		var buf Buffer
		col.EncodeColumn(&buf)

		dec := new(ColStr).LowCardinality()
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 2))
		require.Equal(t, 2, dec.Rows())
		require.Equal(t, "foo", dec.Row(0))
		require.Equal(t, "bar", dec.Row(1))
	})
	t.Run("PrepareIdempotent", func(t *testing.T) {
		// Calling Prepare() twice without Reset() must produce correct encoding.
		col := new(ColStr).LowCardinality()
		col.Append("hello")
		col.Append("world")
		require.NoError(t, col.Prepare())

		// Call Prepare() again on the same data without Reset().
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)

		dec := new(ColStr).LowCardinality()
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 2))
		require.Equal(t, "hello", dec.Row(0))
		require.Equal(t, "world", dec.Row(1))
	})
	t.Run("AppendAfterPrepare", func(t *testing.T) {
		// Append more values after Prepare(), then Prepare() again.
		col := new(ColStr).LowCardinality()
		col.Append("a")
		require.NoError(t, col.Prepare())

		col.Append("b")
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)

		dec := new(ColStr).LowCardinality()
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 2))
		require.Equal(t, "a", dec.Row(0))
		require.Equal(t, "b", dec.Row(1))
	})
}

// encodeLowCardinalityBlock encodes a LowCardinality(String) block with a
// specific dictionary order and key sequence.
func encodeLowCardinalityBlock(dict []string, keys []uint8) []byte {
	var buf Buffer
	buf.PutInt64(cardinalityUpdateAll | int64(KeyUInt8))
	buf.PutInt64(int64(len(dict)))
	var index ColStr
	for _, s := range dict {
		index.Append(s)
	}
	index.EncodeColumn(&buf)
	buf.PutInt64(int64(len(keys)))
	k := ColUInt8(keys)
	k.EncodeColumn(&buf)
	return buf.Buf
}

func TestColLowCardinalityDecodePrepareCycle(t *testing.T) {
	// Regression test: reusing a column across decode→Prepare→encode cycles
	// must produce correct output even when the decoded dictionary order
	// differs from the value encounter order.
	block1 := encodeLowCardinalityBlock(
		[]string{"A", "B", "C"},
		[]uint8{0, 1, 2},
	)
	block2 := encodeLowCardinalityBlock(
		[]string{"A", "B", "C"},
		[]uint8{2, 0, 1, 2},
	)

	col := new(ColStr).LowCardinality()

	// Cycle 1: populates c.kv (makes it non-nil).
	require.NoError(t, col.DecodeColumn(NewReader(bytes.NewReader(block1)), 3))
	require.NoError(t, col.Prepare())

	// Cycle 2: different key ordering.
	col.Reset()
	require.NoError(t, col.DecodeColumn(NewReader(bytes.NewReader(block2)), 4))
	require.Equal(t, "C", col.Row(0))
	require.Equal(t, "A", col.Row(1))
	require.Equal(t, "B", col.Row(2))
	require.Equal(t, "C", col.Row(3))

	require.NoError(t, col.Prepare())
	var out Buffer
	col.EncodeColumn(&out)

	dec := new(ColStr).LowCardinality()
	require.NoError(t, dec.DecodeColumn(out.Reader(), 4))
	require.Equal(t, "C", dec.Row(0))
	require.Equal(t, "A", dec.Row(1))
	require.Equal(t, "B", dec.Row(2))
	require.Equal(t, "C", dec.Row(3))
}

func TestColLowCardinality_DecodeColumn(t *testing.T) {
	t.Run("Str", func(t *testing.T) {
		const rows = 25
		values := []string{
			"neo",
			"trinity",
			"morpheus",
		}
		col := new(ColStr).LowCardinality()
		for i := 0; i < rows; i++ {
			col.Append(values[i%len(values)])
		}
		require.NoError(t, col.Prepare())

		var buf Buffer
		col.EncodeColumn(&buf)
		t.Run("Golden", func(t *testing.T) {
			gold.Bytes(t, buf.Buf, "col_low_cardinality_i_str_k_8")
		})
		t.Run("Ok", func(t *testing.T) {
			br := bytes.NewReader(buf.Buf)
			r := NewReader(br)
			dec := new(ColStr).LowCardinality()
			require.NoError(t, dec.DecodeColumn(r, rows))
			requireEqual[string](t, col, dec)
			dec.Reset()
			require.Equal(t, 0, dec.Rows())
			require.Equal(t, ColumnTypeLowCardinality.Sub(ColumnTypeString), dec.Type())
		})
		t.Run("EOF", func(t *testing.T) {
			r := NewReader(bytes.NewReader(nil))
			dec := new(ColStr).LowCardinality()
			require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
		})
		t.Run("NoShortRead", func(t *testing.T) {
			dec := new(ColStr).LowCardinality()
			requireNoShortRead(t, buf.Buf, colAware(dec, rows))
		})
	})
	t.Run("Blank", func(t *testing.T) {
		// Blank columns (i.e. row count is zero) are not encoded.
		col := new(ColStr).LowCardinality()
		var buf Buffer
		require.NoError(t, col.Prepare())
		col.EncodeColumn(&buf)

		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), col.Rows()))
	})
	t.Run("InvalidVersion", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(2)
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidMeta", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(0)
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
	t.Run("InvalidKeyType", func(t *testing.T) {
		var buf Buffer
		buf.PutInt64(1)
		buf.PutInt64(cardinalityUpdateAll | int64(KeyUInt64+1))
		var dec ColLowCardinality[string]
		require.NoError(t, dec.DecodeColumn(buf.Reader(), 0))
		require.Error(t, dec.DecodeColumn(buf.Reader(), 1))
	})
}

func TestColLowCardinality_PrepareKeyBoundary256(t *testing.T) {
	cases := []struct {
		name string
		rows int
		key  CardinalityKey
	}{
		{name: "255", rows: 255, key: KeyUInt8},
		{name: "256", rows: 256, key: KeyUInt8},
		{name: "257", rows: 257, key: KeyUInt16},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := new(ColStr).LowCardinality()
			col.AppendArr(makeLCStrings(1, tc.rows))
			require.NoError(t, col.Prepare())
			require.Equal(t, tc.key, col.key)
		})
	}
}

func makeLCStrings(start, count int) []string {
	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, fmt.Sprintf("lg-%06d", start+i))
	}
	return values
}
