package proto

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"inet.af/netaddr"

	"github.com/go-faster/ch/internal/gold"
)

func TestColIPv4_EncodeColumn(t *testing.T) {
	const rows = 20
	var data ColIPv4
	for i := 0; i < rows; i++ {
		data = append(data, IPv4(i))
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_ipv4")
	})
	t.Run("NetAddr", func(t *testing.T) {
		input := []netaddr.IP{
			netaddr.MustParseIP("127.0.0.1"),
			netaddr.MustParseIP("127.0.0.2"),
			netaddr.MustParseIP("127.0.0.3"),
		}
		var d ColIPv4
		for _, v := range input {
			d = append(d, ToIPv4(v))
		}
		var netBuf Buffer
		d.EncodeColumn(&netBuf)
		t.Run("Golden", func(t *testing.T) {
			gold.Bytes(t, netBuf.Buf, "col_ipv4_netaddr")
		})
		t.Run("Decode", func(t *testing.T) {
			br := bytes.NewReader(netBuf.Buf)
			r := NewReader(br)

			var dec ColIPv4
			require.NoError(t, dec.DecodeColumn(r, len(input)))
			var output []netaddr.IP
			for _, v := range dec {
				output = append(output, v.ToIP())
			}
			require.Equal(t, input, output)
		})
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := NewReader(br)

		var dec ColIPv4
		require.NoError(t, dec.DecodeColumn(r, rows))
		require.Equal(t, data, dec)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, ColumnTypeIPv4, dec.Type())
	})
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))

		var dec ColIPv4
		require.ErrorIs(t, dec.DecodeColumn(r, rows), io.ErrUnexpectedEOF)
	})
}

func BenchmarkColIPv4_EncodeColumn(b *testing.B) {
	const rows = 50_000
	var data ColIPv4
	for i := 0; i < rows; i++ {
		data = append(data, IPv4(i))
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

func BenchmarkColIPv4_DecodeColumn(b *testing.B) {
	const rows = 50_000
	var data ColIPv4
	for i := 0; i < rows; i++ {
		data = append(data, IPv4(i))
	}

	var buf Buffer
	data.EncodeColumn(&buf)

	br := bytes.NewReader(buf.Buf)
	r := NewReader(br)

	var dec ColIPv4
	if err := dec.DecodeColumn(r, rows); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(buf.Buf)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		data.EncodeColumn(&buf)
	}
}
