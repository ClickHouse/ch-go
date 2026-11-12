package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestColUUID_DecodeColumn(t *testing.T) {
	const rows = 50
	var data ColUUID
	for i := 0; i < rows; i++ {
		data = append(data, uuid.UUID{byte(i)})
	}

	var buf Buffer
	data.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_uuid")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColUUID
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeUUID, dec.Type())
	})
	t.Run("EOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColUUID
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.EOF)
	})
}

func TestColUUID_EncodeAfterPrefix(t *testing.T) {
	v := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	for _, prefix := range [][]byte{{1, 2, 3}, {1, 2, 3, 4, 5, 6, 7, 8}} {
		var b Buffer
		b.PutRaw(prefix)
		ColUUID{v}.EncodeColumn(&b)
		require.Equal(t, prefix, b.Buf[:len(prefix)])

		var got ColUUID
		require.NoError(t, got.DecodeColumn(NewReader(bytes.NewReader(b.Buf[len(prefix):])), 1))
		require.Equal(t, v, got[0])
	}
}

func BenchmarkColUUID_DecodeColumn(b *testing.B) {
	const rows = 1_000
	var data ColUUID
	for i := 0; i < rows; i++ {
		data = append(data, uuid.UUID{byte(i)})
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	var dec ColUUID
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

func BenchmarkColUUID_EncodeColumn(b *testing.B) {
	const rows = 1_000
	var data ColUUID
	for i := 0; i < rows; i++ {
		data = append(data, uuid.UUID{byte(i)})
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
