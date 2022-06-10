package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func (c *ColInt8) ArrAppend(arr *ColArr, data []int8) {
	*c = append(*c, data...)
	arr.Offsets = append(arr.Offsets, uint64(len(*c)))
}

func (c ColInt8) ArrForEach(arr *ColArr, f func(i int, data []int8) error) error {
	for i, end := range arr.Offsets {
		var start int
		if i > 0 {
			start = int(arr.Offsets[i-1])
		}
		if err := f(i, c[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func TestColArr_DecodeColumn(t *testing.T) {
	var data ColInt8
	arr := ColArr{
		Data: &data,
	}

	const rows = 5
	var values [][]int8
	for i := 0; i < rows; i++ {
		var v []int8
		for j := 0; j < i+2; j++ {
			v = append(v, 10+int8(j*2)+int8(3*i))
		}
		values = append(values, v)
	}
	for _, v := range values {
		data.ArrAppend(&arr, v)
	}

	var buf Buffer
	arr.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_int8_manual")
	})
	t.Run("ColumnType", func(t *testing.T) {
		require.Equal(t, "Array(Int8)", arr.Type().String())
	})

	var outData ColInt8
	out := ColArr{
		Data: &outData,
	}
	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)
	require.NoError(t, out.DecodeColumn(r, rows))
	require.Equal(t, rows, out.Rows())

	assert.Equal(t, data, outData)
	require.NoError(t, outData.ArrForEach(&out, func(i int, data []int8) error {
		t.Logf("%v", data)
		assert.Equal(t, values[i], data, "[%d] mismatch", i)
		return nil
	}))
}
