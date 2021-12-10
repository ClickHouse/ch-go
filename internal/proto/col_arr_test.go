package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
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
	col := ColArr{
		Data: &data,
	}
	const rows = 4
	for i := 0; i < rows; i++ {
		data.ArrAppend(&col, []int8{1, int8(i)})
	}

	var buf Buffer
	col.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_arr_int8")
	})

	var outData ColInt8
	out := ColArr{
		Data: &outData,
	}
	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)
	require.NoError(t, out.DecodeColumn(r, rows))
	require.NoError(t, outData.ArrForEach(&out, func(i int, data []int8) error {
		return nil
	}))
}
