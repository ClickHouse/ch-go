package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
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

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_str")
	})
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

func BenchmarkColStr_DecodeColumn(b *testing.B) {
	const rows = 1_000
	var data ColStr
	for i := 0; i < rows; i++ {
		data.Append("ClickHouse не тормозит")
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	var dec ColStr
	if err := dec.DecodeColumn(r, rows); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		br.Reset(buf.Buf)
		r.s.Reset(br)
		dec.Reset()

		if err := dec.DecodeColumn(r, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColStr_EncodeColumn(b *testing.B) {
	const rows = 1_000
	var data ColStr
	for i := 0; i < rows; i++ {
		data.Append("ClickHouse не тормозит")
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		data.EncodeColumn(&buf)
	}
}
