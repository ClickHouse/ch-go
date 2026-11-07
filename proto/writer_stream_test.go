package proto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamingWriter(t *testing.T) {
	var dst bytes.Buffer
	var scratch Buffer
	writer := NewStreamingWriter(&dst, &scratch)
	writer.ChainBuffer(func(b *Buffer) { b.PutRaw([]byte("header")) })
	payload := []byte("data")
	writer.ChainWrite(payload)
	clear(payload)
	writer.ChainBuffer(func(b *Buffer) { b.PutRaw([]byte("tail")) })
	n, err := writer.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(14), n)
	require.Equal(t, "headerdatatail", dst.String())
}

type failedColumnWriter struct {
	calls int
	err   error
}

func (w *failedColumnWriter) Write([]byte) (int, error) { w.calls++; return 0, w.err }

func TestStreamingWriterErrors(t *testing.T) {
	for _, failure := range []error{nil, errors.New("sink failed")} {
		dst := &failedColumnWriter{err: failure}
		writer := NewStreamingWriter(dst, new(Buffer))
		writer.ChainWrite([]byte("first"))
		writer.ChainWrite([]byte("second"))
		writer.ChainBuffer(func(*Buffer) { t.Fatal("callback after failure") })
		_, err := writer.Flush()
		if failure == nil {
			failure = io.ErrShortWrite
		}
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, dst.calls)
	}
}

func TestStreamingStringBounded(t *testing.T) {
	var column ColStr
	column.Append("")
	column.Append(string(bytes.Repeat([]byte("x"), 5<<20)))
	for range 100000 {
		column.Append("short")
	}
	var expected Buffer
	column.EncodeColumn(&expected)
	var dst bytes.Buffer
	var scratch Buffer
	writer := NewStreamingWriter(&dst, &scratch)
	column.WriteColumn(writer)
	_, err := writer.Flush()
	require.NoError(t, err)
	require.Equal(t, expected.Buf, dst.Bytes())
	require.Less(t, cap(scratch.Buf), 3*streamBufferSize)
}

func TestStreamingColumnsBounded(t *testing.T) {
	columns := []ColInput{
		ColNothing(2 << 20), ColUInt64(make([]uint64, 256<<10)),
		ColDate(make([]Date, 1<<20)), ColDate32(make([]Date32, 512<<10)),
		ColDateTime{Data: make([]DateTime, 512<<10)},
		ColDateTime64{Data: make([]DateTime64, 256<<10)},
		ColTime32{Data: make([]Time32, 512<<10)},
		ColTime64{Data: make([]Time64, 256<<10)},
	}
	for _, column := range columns {
		t.Run(string(column.Type()), func(t *testing.T) {
			var expected, scratch Buffer
			var dst bytes.Buffer
			column.EncodeColumn(&expected)
			writer := NewStreamingWriter(&dst, &scratch)
			column.WriteColumn(writer)
			_, err := writer.Flush()
			require.NoError(t, err)
			require.Equal(t, expected.Buf, dst.Bytes())
			require.Less(t, cap(scratch.Buf), 3*streamBufferSize)
		})
	}
}

func BenchmarkStringWriter(b *testing.B) {
	for _, shape := range []struct{ rows, width int }{{1 << 20, 8}, {64 << 10, 128}, {512 << 10, 128}} {
		column := ColStr{Buf: make([]byte, 0, shape.rows*shape.width), Pos: make([]Position, 0, shape.rows)}
		value := bytes.Repeat([]byte("x"), shape.width)
		for range shape.rows {
			column.AppendBytes(value)
		}
		for _, streaming := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/width=%d/streaming=%v", shape.rows, shape.width, streaming), func(b *testing.B) {
				var scratch Buffer
				writer := NewWriter(io.Discard, &scratch)
				if streaming {
					writer = NewStreamingWriter(io.Discard, &scratch)
				}
				column.WriteColumn(writer)
				if _, err := writer.Flush(); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(column.Buf)))
				for b.Loop() {
					column.WriteColumn(writer)
					if _, err := writer.Flush(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestStreamingStringBatchBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name    string
		lengths []int
	}{
		{name: "empty column"},
		{name: "empty strings", lengths: make([]int, 2*streamBufferSize+1)},
		{name: "prefix transitions", lengths: []int{0, 1, 127, 128, 16383, 16384}},
		{name: "batch transitions", lengths: []int{streamBufferSize - 4, 1, streamBufferSize, 0, streamBufferSize + 1, 0, 8}},
		{name: "large first and last", lengths: []int{2 * streamBufferSize, 8, 0, 3 * streamBufferSize}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var column ColStr
			for i, length := range tt.lengths {
				column.AppendBytes(bytes.Repeat([]byte{byte(i)}, length))
			}
			var expected, scratch Buffer
			expected.PutRaw([]byte("header"))
			column.EncodeColumn(&expected)
			var dst bytes.Buffer
			writer := NewStreamingWriter(&dst, &scratch)
			writer.ChainBuffer(func(b *Buffer) { b.PutRaw([]byte("header")) })
			column.WriteColumn(writer)
			_, err := writer.Flush()
			require.NoError(t, err)
			require.Equal(t, expected.Buf, dst.Bytes())
			require.Less(t, cap(scratch.Buf), 3*streamBufferSize)
		})
	}
}

func TestBufferedStringAfterLargeBuffer(t *testing.T) {
	var column ColStr
	for i := range 3 * streamBufferSize / 8 {
		column.Append(fmt.Sprintf("%08d", i))
	}
	column.AppendBytes(bytes.Repeat([]byte("L"), 2*streamBufferSize))
	column.Append("tail")
	prefix := bytes.Repeat([]byte("p"), 2*streamBufferSize+7)

	var expected Buffer
	expected.PutRaw(prefix)
	column.EncodeColumn(&expected)

	var dst bytes.Buffer
	writer := NewWriter(&dst, new(Buffer))
	writer.ChainBuffer(func(b *Buffer) { b.PutRaw(prefix) })
	column.WriteColumn(writer)
	_, err := writer.Flush()
	require.NoError(t, err)
	require.Equal(t, expected.Buf, dst.Bytes())
}

func TestStreamingStringBatchFailure(t *testing.T) {
	var column ColStr
	for range 2 * streamBufferSize {
		column.Append("x")
	}
	sink := &failedColumnWriter{err: errors.New("sink failed")}
	writer := NewStreamingWriter(sink, new(Buffer))
	column.WriteColumn(writer)
	_, err := writer.Flush()
	require.ErrorIs(t, err, sink.err)
	require.Equal(t, 1, sink.calls)
}
