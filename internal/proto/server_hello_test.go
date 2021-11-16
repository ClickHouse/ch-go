package proto

import (
	"bytes"
	"testing"
)

func BenchmarkServerHello_Decode(b *testing.B) {
	var raw Buffer
	raw.PutString("ClickHouse server")
	raw.PutInt(21)
	raw.PutInt(11)
	raw.PutInt(54450)

	buf := new(bytes.Reader)

	r := NewReader(buf)

	b.Run("Struct", func(b *testing.B) {
		b.SetBytes(int64(len(raw.Buf)))
		b.ReportAllocs()
		var serverHello ServerHello
		for i := 0; i < b.N; i++ {
			buf.Reset(raw.Buf)
			r.s.Reset(buf)

			if err := serverHello.Decode(r); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Raw", func(b *testing.B) {
		b.SetBytes(int64(len(raw.Buf)))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf.Reset(raw.Buf)
			r.s.Reset(buf)

			name, err := r.StrRaw()
			if err != nil {
				b.Fatal(err)
			}

			major, err := r.Int()
			if err != nil {
				b.Fatal(err)
			}
			minor, err := r.Int()
			if err != nil {
				b.Fatal(err)
			}
			revision, err := r.Int()
			if err != nil {
				b.Fatal(err)
			}

			_ = name
			_ = major
			_ = minor
			_ = revision
		}
	})
}
