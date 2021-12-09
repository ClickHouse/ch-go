package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColStr_EncodeColumn(t *testing.T) {
	var data ColStr

	input := []string{
		"foo",
		"bar",
		"ClickHouse",
		"one",
		"",
		"1",
	}
	rows := len(input)
	for _, s := range input {
		data.Append(s)
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColStr
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)

		t.Run("ForEach", func(t *testing.T) {
			var output []string
			f := func(i int, s string) error {
				output = append(output, s)
				return nil
			}
			require.NoError(t, dec.ForEach(f))
			require.Equal(t, input, output)
		})
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColStr
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
	})
}
