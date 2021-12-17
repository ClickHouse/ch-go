package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestBool(t *testing.T) {
	values := []bool{
		false, true, false, false,
	}
	var b Buffer
	for _, v := range values {
		b.PutBool(v)
	}

	r := b.Reader()
	for _, v := range values {
		got, err := r.Bool()
		require.NoError(t, err)
		require.Equal(t, v, got)
	}
}

func TestNewRawBool(t *testing.T) {
	raw := ColUInt8{1, 0, 1, 0, 1, 1, 1, 0}
	a := NewRawBool(&raw)
	require.Equal(t, ColumnTypeBool, a.Type())
}

func TestColBool_Raw_DecodeColumn(t *testing.T) {
	const rows = 8
	values := ColBool{true, false, true, false, false, false, false, true}
	var dataRaw ColUInt8
	for i := 0; i < rows; i++ {
		if values[i] {
			dataRaw = append(dataRaw, 1)
		} else {
			dataRaw = append(dataRaw, 0)
		}
	}
	data := NewRawBool(&dataRaw)

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_bool_raw")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		dec := NewRawBool(&ColUInt8{})
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeBool, dec.Type())
	})
	t.Run("OkBool", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColBool
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, values, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeBool, dec.Type())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		dec := NewRawBool(&ColUInt8{})
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		dec := NewRawBool(&ColUInt8{})
		requireNoShortRead(t, buf.Buf, colAware(dec, rows))
	})
}

func BenchmarkColBool_Raw_DecodeColumn(b *testing.B) {
	const rows = 50_000
	var data ColUInt8
	for i := 0; i < rows; i++ {
		data = append(data, uint8(i%2))
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	dec := NewRawBool(&ColUInt8{})
	if err := dec.DecodeColumn(r, rows); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		br.Reset(buf.Buf)
		r.raw.Reset(br)
		dec.Reset()

		if err := dec.DecodeColumn(r, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColBool_Raw_EncodeColumn(b *testing.B) {
	const rows = 50_000
	var data ColUInt8
	for i := 0; i < rows; i++ {
		data = append(data, uint8(i%2))
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
