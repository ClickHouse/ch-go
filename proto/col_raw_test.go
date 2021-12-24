package proto

import (
	"bytes"
	"testing"
)

func BenchmarkColRaw_EncodeColumn(b *testing.B) {
	buf := new(Buffer)
	v := ColRaw{
		Data: make([]byte, 1024),
	}

	b.ReportAllocs()
	b.SetBytes(1024)

	for i := 0; i < b.N; i++ {
		buf.Reset()
		v.EncodeColumn(buf)
	}
}

func BenchmarkColRaw_DecodeColumn(b *testing.B) {
	const (
		rows = 1_000
		size = 64
		data = size * rows
	)

	raw := make([]byte, data)
	br := bytes.NewReader(raw)
	r := NewReader(br)

	b.ReportAllocs()
	b.SetBytes(data)

	dec := ColRaw{
		T:    ColumnTypeUInt64,
		Size: size,
	}
	for i := 0; i < b.N; i++ {
		br.Reset(raw)
		r.raw.Reset(br)
		dec.Reset()

		if err := dec.DecodeColumn(r, rows); err != nil {
			b.Fatal(err)
		}
	}
}
