package compress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamWriter(t *testing.T) {
	for _, method := range []Method{None, LZ4, LZ4HC, ZSTD} {
		for _, random := range []bool{false, true} {
			t.Run(method.String()+map[bool]string{false: "/zeros", true: "/random"}[random], func(t *testing.T) {
				data := make([]byte, 3*BlockSize+123)
				if random {
					_, err := rand.New(rand.NewSource(42)).Read(data)
					require.NoError(t, err)
				}
				for _, chunk := range []int{37, BlockSize, len(data)} {
					var dst bytes.Buffer
					stream := NewStreamWriter(&dst, LevelZero, method, 0)
					for start := 0; start < len(data); start += chunk {
						n, err := stream.Write(data[start:min(start+chunk, len(data))])
						require.NoError(t, err)
						require.Equal(t, min(chunk, len(data)-start), n)
					}
					require.NoError(t, stream.Flush())
					size := dst.Len()
					require.NoError(t, stream.Flush())
					require.Equal(t, size, dst.Len())
					frames := 0
					for payload := dst.Bytes(); len(payload) > 0; frames++ {
						raw := int(binary.LittleEndian.Uint32(payload[hDataSize:]))
						require.LessOrEqual(t, raw, BlockSize)
						frameSize := checksumSize + int(binary.LittleEndian.Uint32(payload[hRawSize:]))
						require.LessOrEqual(t, frameSize, len(payload))
						payload = payload[frameSize:]
					}
					require.Equal(t, 4, frames)
					got, err := io.ReadAll(io.LimitReader(NewReader(bytes.NewReader(dst.Bytes())), int64(len(data))))
					require.NoError(t, err)
					require.Equal(t, data, got)
					require.LessOrEqual(t, cap(stream.buffer), BlockSize)
					require.Less(t, cap(stream.codec.Data), 2*BlockSize)
				}
			})
		}
	}
}

type failingStreamSink struct {
	err   error
	calls int
}

func (s *failingStreamSink) Write(p []byte) (int, error) { s.calls++; return len(p) - 1, s.err }

func TestStreamWriterErrorsAndReset(t *testing.T) {
	for _, failure := range []error{nil, errors.New("write failed")} {
		sink := &failingStreamSink{err: failure}
		stream := NewStreamWriter(sink, LevelZero, LZ4, 0)
		_, err := stream.Write(make([]byte, BlockSize))
		if failure == nil {
			failure = io.ErrShortWrite
		}
		require.ErrorIs(t, err, failure)
		_, err = stream.Write([]byte("more"))
		require.ErrorIs(t, err, failure)
		require.ErrorIs(t, stream.Flush(), failure)
		require.Equal(t, 1, sink.calls)
		var dst bytes.Buffer
		stream.Reset(&dst)
		_, err = stream.Write([]byte("reused"))
		require.NoError(t, err)
		require.NoError(t, stream.Flush())
		got, err := io.ReadAll(io.LimitReader(NewReader(&dst), 6))
		require.NoError(t, err)
		require.Equal(t, "reused", string(got))
	}
}

func TestStreamWriterNilDestination(t *testing.T) {
	stream := NewStreamWriter(nil, LevelZero, LZ4, 0)
	for range 2 {
		require.ErrorIs(t, stream.Flush(), errNilStreamDestination)
		for _, size := range []int{0, 1, BlockSize} {
			n, err := stream.Write(make([]byte, size))
			require.Zero(t, n)
			require.ErrorIs(t, err, errNilStreamDestination)
		}
		var dst bytes.Buffer
		stream.Reset(&dst)
		n, err := stream.Write([]byte("reused"))
		require.NoError(t, err)
		require.Equal(t, 6, n)
		require.NoError(t, stream.Flush())
		got, err := io.ReadAll(io.LimitReader(NewReader(&dst), 6))
		require.NoError(t, err)
		require.Equal(t, "reused", string(got))
		stream.Reset(nil)
	}
}

type failingFrameSink struct {
	calls  int
	failAt int
	err    error
}

func (s *failingFrameSink) Write(p []byte) (int, error) {
	s.calls++
	if s.calls == s.failAt {
		return len(p) / 2, s.err
	}
	return len(p), nil
}

func TestStreamWriterFailedFrameCount(t *testing.T) {
	for _, failure := range []error{nil, errors.New("sink failed")} {
		for _, buffered := range []int{0, 1} {
			for _, failAt := range []int{1, 2} {
				t.Run(fmt.Sprintf("buffered=%d/failAt=%d/error=%v", buffered, failAt, failure), func(t *testing.T) {
					sink := &failingFrameSink{failAt: failAt, err: failure}
					stream := NewStreamWriter(sink, LevelZero, LZ4, 0)
					n, err := stream.Write(make([]byte, buffered))
					require.NoError(t, err)
					require.Equal(t, buffered, n)
					n, err = stream.Write(make([]byte, 3*BlockSize))
					expectedErr := failure
					if expectedErr == nil {
						expectedErr = io.ErrShortWrite
					}
					require.ErrorIs(t, err, expectedErr)
					expectedCount := 0
					if failAt == 2 {
						expectedCount = BlockSize - buffered
					}
					require.Equal(t, expectedCount, n)
					require.Equal(t, failAt, sink.calls)
					n, err = stream.Write([]byte("more"))
					require.Zero(t, n)
					require.ErrorIs(t, err, expectedErr)
					require.ErrorIs(t, stream.Flush(), expectedErr)
					require.Equal(t, failAt, sink.calls)
				})
			}
		}
	}
}

func TestStreamWriterFrameSize(t *testing.T) {
	for _, size := range []int{0, 64 << 10, 4 << 20, 8 << 20} {
		frameSize := size
		if frameSize == 0 {
			frameSize = BlockSize
		}
		data := randData(2*frameSize + 123)
		for _, method := range []Method{None, LZ4, ZSTD} {
			t.Run(fmt.Sprintf("%s/%d", method, size), func(t *testing.T) {
				var dst bytes.Buffer
				stream := NewStreamWriter(&dst, LevelZero, method, size)
				for _, part := range [][]byte{data[:17], data[17:]} {
					n, err := stream.Write(part)
					require.NoError(t, err)
					require.Equal(t, len(part), n)
				}
				require.NoError(t, stream.Flush())
				remaining := len(data)
				frames := 0
				for payload := dst.Bytes(); len(payload) > 0; frames++ {
					raw := int(binary.LittleEndian.Uint32(payload[hDataSize:]))
					require.Equal(t, min(frameSize, remaining), raw)
					remaining -= raw
					frameBytes := checksumSize + int(binary.LittleEndian.Uint32(payload[hRawSize:]))
					require.LessOrEqual(t, frameBytes, len(payload))
					payload = payload[frameBytes:]
				}
				require.Zero(t, remaining)
				require.Equal(t, 3, frames)
				decoded := make([]byte, len(data))
				_, err := io.ReadFull(NewReader(bytes.NewReader(dst.Bytes())), decoded)
				require.NoError(t, err)
				require.Equal(t, data, decoded)
				require.Equal(t, frameSize, cap(stream.buffer))
				first := bytes.Clone(dst.Bytes())
				dst.Reset()
				stream.Reset(&dst)
				_, err = stream.Write(data)
				require.NoError(t, err)
				require.NoError(t, stream.Flush())
				require.Equal(t, first, dst.Bytes())
			})
		}
	}
}

func TestStreamWriterInvalidFrameSize(t *testing.T) {
	for _, size := range []int{-1, MaxFrameSize + 1} {
		stream := NewStreamWriter(io.Discard, LevelZero, LZ4, size)
		for range 2 {
			n, err := stream.Write([]byte("data"))
			require.Zero(t, n)
			require.ErrorContains(t, err, "compression frame size")
			require.Error(t, stream.Flush())
			require.Nil(t, stream.buffer)
			stream.Reset(io.Discard)
		}
	}
	for _, size := range []int{0, 1, MaxFrameSize} {
		stream := NewStreamWriter(io.Discard, LevelZero, LZ4, size)
		require.NoError(t, stream.Flush())
	}
}
