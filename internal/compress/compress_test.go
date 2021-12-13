package compress

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompress(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}

	w := NewWriter()
	require.NoError(t, w.Compress(data))

	r := NewReader(bytes.NewReader(w.Data))

	out := make([]byte, len(data))
	_, err := io.ReadFull(r, out)
	require.NoError(t, err)
}

func BenchmarkWriter_Compress(b *testing.B) {
	// Highly compressible data.
	data := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 1800)

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))

	w := NewWriter()

	for i := 0; i < b.N; i++ {
		if err := w.Compress(data); err != nil {
			b.Fatal(err)
		}
	}
}

func randData(n int) []byte {
	s := rand.NewSource(10)
	r := rand.New(s)
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		panic(err)
	}
	return buf
}

func BenchmarkReader_Read(b *testing.B) {
	// Not compressible data.
	data := randData(1024 * 20)

	w := NewWriter()
	if err := w.Compress(data); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(w.Data)))

	out := make([]byte, len(data))

	br := bytes.NewReader(data)
	r := NewReader(br)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		br.Reset(w.Data)
		if _, err := io.ReadFull(r, out); err != nil {
			b.Fatal(err)
		}
	}
}
