package compress

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/go-faster/city"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func TestCompress(t *testing.T) {
	data := []byte(strings.Repeat("Hello!\n", 25))
	gold.Bytes(t, data, "data_raw")

	for i := range MethodValues() {
		m := MethodValues()[i]
		t.Run(m.String(), func(t *testing.T) {
			w := NewWriter(LevelZero, m)
			require.NoError(t, w.Compress(data))

			gold.Bytes(t, w.Data, "data_compressed_"+strings.ToLower(m.String()))

			br := bytes.NewReader(nil)
			r := NewReader(br)
			out := make([]byte, len(data))

			for i := 0; i < 10; i++ {
				br.Reset(w.Data)
				_, err := io.ReadFull(r, out)
				require.NoError(t, err)
				require.Equal(t, data, out)
			}

			t.Run("NoShortRead", func(t *testing.T) {
				for i := 0; i < len(w.Data); i++ {
					b := w.Data[:i]
					r := NewReader(bytes.NewReader(b))
					_, err := io.ReadFull(r, out)
					require.Error(t, err)
				}
			})
			t.Run("CheckHash", func(t *testing.T) {
				// Corrupt bytes of data or checksum.
				var ref city.U128
				for i := 0; i < len(w.Data); i++ {
					b := append([]byte{}, w.Data...) // clone
					b[i]++
					r := NewReader(bytes.NewReader(b))
					_, err := io.ReadFull(r, out)
					require.Error(t, err)
					if i > headerSize {
						var badData *CorruptedDataErr
						require.ErrorAs(t, err, &badData)
						if ref.High == 0 && ref.Low == 0 {
							// Pick first reference.
							ref = badData.Reference
						}
						require.Equal(t, FormatU128(ref), FormatU128(badData.Reference))
						require.Equal(t, ref, badData.Reference)
					}
				}
			})
		})
	}
}

func BenchmarkWriter_Compress(b *testing.B) {
	// Highly compressible data.
	data := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 1800)

	for i := range MethodValues() {
		m := MethodValues()[i]
		b.Run(m.String(), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))

			w := NewWriter(LevelZero, m)

			// First round to warmup.
			if err := w.Compress(data); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.Compress(data); err != nil {
					b.Fatal(err)
				}
			}
		})
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

	for i := range MethodValues() {
		m := MethodValues()[i]
		b.Run(m.String(), func(b *testing.B) {
			w := NewWriter(LevelZero, m)
			if err := w.Compress(data); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(w.Data)))

			out := make([]byte, len(data))

			br := bytes.NewReader(nil)
			r := NewReader(br)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				br.Reset(w.Data)
				if _, err := io.ReadFull(r, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestZSTDCompressionLevel(t *testing.T) {
	data := bytes.Repeat([]byte("compression level test: repeated strings and numbers 0123456789\n"), 2048)
	for _, tt := range []struct {
		level  Level
		preset zstd.EncoderLevel
	}{
		{0, zstd.SpeedDefault},
		{1, zstd.SpeedFastest},
		{2, zstd.SpeedFastest},
		{3, zstd.SpeedDefault},
		{5, zstd.SpeedDefault},
		{6, zstd.SpeedBetterCompression},
		{9, zstd.SpeedBetterCompression},
		{10, zstd.SpeedBestCompression},
		{22, zstd.SpeedBestCompression},
		{23, zstd.SpeedDefault},
		{Level(math.MaxUint32), zstd.SpeedDefault},
	} {
		t.Run(fmt.Sprint(tt.level), func(t *testing.T) {
			reference, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(tt.preset), zstd.WithEncoderConcurrency(1), zstd.WithLowerEncoderMem(true))
			require.NoError(t, err)
			defer reference.Close()
			expected := reference.EncodeAll(data, nil)
			writer := NewWriter(tt.level, ZSTD)
			defer writer.zstd.Close()
			require.NoError(t, writer.Compress(data))
			require.Equal(t, expected, writer.Data[headerSize:])
			decoded := make([]byte, len(data))
			_, err = io.ReadFull(NewReader(bytes.NewReader(writer.Data)), decoded)
			require.NoError(t, err)
			require.Equal(t, data, decoded)
		})
	}
}

func TestCompressReusedDirtyBuffer(t *testing.T) {
	inputs := [][]byte{randData(1 << 20), []byte("short"), nil, randData(2<<20 + 123), make([]byte, 1<<20)}
	for _, method := range MethodValues() {
		t.Run(method.String(), func(t *testing.T) {
			writer := NewWriter(LevelZero, method)
			for _, input := range inputs {
				storage := writer.Data[:cap(writer.Data)]
				for i := range storage {
					storage[i] = 0xa5
				}
				require.NoError(t, writer.Compress(input))
				reference := NewWriter(LevelZero, method)
				require.NoError(t, reference.Compress(input))
				require.Equal(t, reference.Data, writer.Data)
				if len(input) > 0 {
					decoded := make([]byte, len(input))
					_, err := io.ReadFull(NewReader(bytes.NewReader(writer.Data)), decoded)
					require.NoError(t, err)
					require.Equal(t, input, decoded)
				}
			}
		})
	}
}
