package ch

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

func testBlockEncoder(method compress.Method, enabled bool) *BlockEncoder {
	mode := proto.CompressionDisabled
	if enabled {
		mode = proto.CompressionEnabled
	}
	return (&Client{protocolVersion: proto.Version, compression: mode, compressionMethod: method}).NewBlockEncoder()
}

func TestPreparedBlockOwnership(t *testing.T) {
	for _, method := range []compress.Method{compress.None, compress.LZ4, compress.LZ4HC, compress.ZSTD} {
		t.Run(method.String(), func(t *testing.T) {
			var stringsColumn proto.ColStr
			stringsColumn.Append("first")
			stringsColumn.Append(string(bytes.Repeat([]byte("payload"), 400000)))
			numbers := proto.ColUInt64{3, 7}
			input := proto.Input{{Name: "s", Data: &stringsColumn}, {Name: "n", Data: &numbers}}
			var expected proto.Buffer
			block := proto.Block{Columns: 2, Rows: 2, Info: proto.BlockInfo{BucketNum: -1}}
			require.NoError(t, block.EncodeBlock(&expected, proto.Version, input))
			encoder := testBlockEncoder(method, true)
			prepared, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			stringsColumn.Reset()
			numbers.Reset()
			stringsColumn.Append("replacement")
			numbers.Append(123)
			_, err = encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			decoded, err := io.ReadAll(io.LimitReader(compress.NewReader(bytes.NewReader(prepared.data)), int64(len(expected.Buf))))
			require.NoError(t, err)
			require.Equal(t, expected.Buf, decoded)
			require.Equal(t, 2, prepared.Rows())
			require.Equal(t, len(prepared.data), prepared.Size())
			require.GreaterOrEqual(t, prepared.Capacity(), prepared.Size())
			require.Less(t, cap(encoder.buffer.Buf), 256<<10)
		})
	}
}

func TestBlockEncoderUncompressedAndCancellation(t *testing.T) {
	values := proto.ColUInt64{1, 2}
	input := proto.Input{{Name: "n", Data: &values}}
	encoder := testBlockEncoder(compress.None, false)
	prepared, err := encoder.Encode(t.Context(), input)
	require.NoError(t, err)
	var expected proto.Buffer
	require.NoError(t, (proto.Block{Columns: 1, Rows: 2, Info: proto.BlockInfo{BucketNum: -1}}).EncodeBlock(&expected, proto.Version, input))
	require.Equal(t, expected.Buf, prepared.data)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = encoder.Encode(ctx, input)
	require.ErrorIs(t, err, context.Canceled)
	_, err = encoder.Encode(t.Context(), nil)
	require.Error(t, err)
	_, err = encoder.Encode(t.Context(), input)
	require.NoError(t, err)
}

func TestPreparedInputValidation(t *testing.T) {
	client := &Client{protocolVersion: proto.Version, compression: proto.CompressionEnabled}
	require.ErrorContains(t, client.Do(t.Context(), Query{Input: proto.Input{{}}, OnPreparedInput: func(context.Context) (*PreparedBlock, error) { return nil, nil }}), "mutually exclusive")
	for _, value := range []*PreparedBlock{nil, {}, {rows: 1, data: []byte{1}, version: proto.Version - 1, compression: proto.CompressionEnabled}, {rows: 1, data: []byte{1}, version: proto.Version, compression: proto.CompressionDisabled}} {
		err := client.sendPreparedInput(t.Context(), func(context.Context) (*PreparedBlock, error) { return value, nil })
		require.Error(t, err)
	}
	err := client.sendPreparedInput(t.Context(), func(context.Context) (*PreparedBlock, error) { return &PreparedBlock{}, io.EOF })
	require.ErrorContains(t, err, "EOF")
	failure := errors.New("source failed")
	err = client.sendPreparedInput(t.Context(), func(context.Context) (*PreparedBlock, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
}

func TestClientStreamingPreparedInput(t *testing.T) {
	for _, options := range []Options{
		{Compression: CompressionLZ4},
		{Compression: CompressionZSTD},
		{Compression: CompressionZSTD, CompressionLevel: 1},
		{Compression: CompressionLZ4, CompressionFrameSize: 4 << 20},
		{Compression: CompressionZSTD, CompressionLevel: 1, CompressionFrameSize: 4 << 20},
		{Compression: CompressionZSTD, CompressionFrameSize: 8 << 20},
		{Compression: CompressionNone},
		{Compression: CompressionDisabled},
	} {
		t.Run(fmt.Sprintf("%s/level=%d/frame=%d", options.Compression, options.CompressionLevel, options.CompressionFrameSize), func(t *testing.T) {
			client := ConnOpt(t, options)
			encoder := client.NewBlockEncoder()
			var values proto.ColStr
			payloadSize := max(3000000, options.CompressionFrameSize+123)
			values.AppendBytes(bytes.Repeat([]byte("x"), payloadSize))
			values.Append("")
			input := proto.Input{{Name: "s", Data: &values}}
			prepared, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			if options.Compression != CompressionDisabled {
				frameSize := options.CompressionFrameSize
				if frameSize == 0 {
					frameSize = compress.BlockSize
				}
				require.Equal(t, uint32(frameSize), binary.LittleEndian.Uint32(prepared.data[21:25]))
			}
			values.Reset()
			require.NoError(t, client.Do(t.Context(), Query{Body: "CREATE TABLE streamed (s String) ENGINE=Memory"}))
			sent := 0
			require.NoError(t, client.Do(t.Context(), Query{Body: "INSERT INTO streamed VALUES", OnPreparedInput: func(context.Context) (*PreparedBlock, error) {
				if sent == 3 {
					return nil, io.EOF
				}
				sent++
				return prepared, nil
			}}))
			values.AppendBytes(bytes.Repeat([]byte("x"), payloadSize))
			values.Append("")
			require.NoError(t, client.Do(t.Context(), Query{Body: "INSERT INTO streamed VALUES", Input: input}))
			values.Reset()
			var count, size proto.ColUInt64
			require.NoError(t, client.Do(t.Context(), Query{Body: "SELECT count(),sum(length(s)) FROM streamed", Result: proto.Results{{Name: "count()", Data: &count}, {Name: "sum(length(s))", Data: &size}}}))
			require.Equal(t, uint64(8), count[0])
			require.Equal(t, uint64(4*payloadSize), size[0])
			// Normal and prepared input share the same frame encoder, including external data.
			numbers := proto.ColUInt64{11, 22}
			var result proto.ColUInt64
			require.NoError(t, client.Do(t.Context(), Query{Body: "SELECT n FROM ext", ExternalTable: "ext", ExternalData: proto.Input{{Name: "n", Data: &numbers}}, Result: proto.Results{{Name: "n", Data: &result}}}))
			require.Equal(t, numbers, result)
		})
	}
}

type cancelBlockWriter struct {
	cancel context.CancelFunc
	calls  int
}

func (w *cancelBlockWriter) Write(p []byte) (int, error) {
	w.calls++
	w.cancel()
	return len(p), nil
}

func TestBlockEncoderCancellationDuringWrite(t *testing.T) {
	values := proto.ColUInt8(make([]uint8, 3*compress.BlockSize))
	input := proto.Input{{Name: "n", Data: &values}}
	encoder := testBlockEncoder(compress.LZ4, true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dst := &cancelBlockWriter{cancel: cancel}
	require.ErrorIs(t, encoder.write(blockContextWriter{ctx: ctx, dst: dst}, input), context.Canceled)
	require.Equal(t, 1, dst.calls)
	_, err := encoder.Encode(t.Context(), input)
	require.NoError(t, err)
}

type benchmarkBlockSink struct {
	bytes int
}

func (s *benchmarkBlockSink) Write(p []byte) (int, error) {
	s.bytes += len(p)
	return len(p), nil
}

// BenchmarkBlockCompression compares warmed, reused whole-block and streaming
// encoders with LZ4 and ZSTD, using synthetic 128-byte strings and eight UInt64
// columns at 4 MiB and 64 MiB input sizes. It reports encoded wire size as well as
// CPU time and allocations. Input construction and initial codec/buffer growth are
// outside the timed loop; output goes to a counting sink, so network and server
// costs are excluded. Steady-state allocations do not measure retained encoder
// memory. Cold-start allocation figures should be reported separately.
func BenchmarkBlockCompression(b *testing.B) {
	for _, size := range []int{4 << 20, 64 << 20} {
		for _, shape := range []string{"String128", "UInt64x8"} {
			var input proto.Input
			switch shape {
			case "String128":
				values := proto.ColStr{Buf: make([]byte, 0, size), Pos: make([]proto.Position, 0, size/128)}
				value := bytes.Repeat([]byte("abcdefgh"), 16)
				for range size / 128 {
					values.AppendBytes(value)
				}
				input = proto.Input{{Name: "s", Data: &values}}
			case "UInt64x8":
				for column := range 8 {
					values := make(proto.ColUInt64, size/(8*8))
					for row := range values {
						values[row] = uint64(row%4096 + column)
					}
					input = append(input, proto.InputColumn{Name: fmt.Sprintf("n%d", column), Data: &values})
				}
			}
			for _, method := range []compress.Method{compress.LZ4, compress.ZSTD} {
				for _, streaming := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/%dMiB/%s/streaming=%v", shape, size>>20, method, streaming), func(b *testing.B) {
						var sink benchmarkBlockSink
						var encode func() error
						if streaming {
							encoder := testBlockEncoder(method, true)
							encode = func() error { return encoder.write(blockContextWriter{ctx: b.Context(), dst: &sink}, input) }
						} else {
							var buffer proto.Buffer
							codec := compress.NewWriter(0, method)
							block := proto.Block{Columns: len(input), Rows: input[0].Data.Rows(), Info: proto.BlockInfo{BucketNum: -1}}
							encode = func() error {
								buffer.Reset()
								if err := block.EncodeBlock(&buffer, proto.Version, input); err != nil {
									return err
								}
								if err := codec.Compress(buffer.Buf); err != nil {
									return err
								}
								buffer.Buf = append(buffer.Buf[:0], codec.Data...)
								_, err := sink.Write(buffer.Buf)
								return err
							}
						}
						// Warm both codec state and reusable buffers before measuring steady state.
						if err := encode(); err != nil {
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.SetBytes(int64(size))
						for b.Loop() {
							sink.bytes = 0
							if err := encode(); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(sink.bytes), "wire-B/op")
					})
				}
			}
		}
	}
}

func TestBlockEncoderMissingColumnData(t *testing.T) {
	values := proto.ColUInt64{1}
	for _, enabled := range []bool{false, true} {
		for _, tt := range []struct {
			name  string
			input proto.Input
		}{
			{name: "first", input: proto.Input{{Name: "missing"}}},
			{name: "later", input: proto.Input{{Name: "n", Data: &values}, {Name: "missing"}}},
		} {
			t.Run(fmt.Sprintf("compression=%v/%s", enabled, tt.name), func(t *testing.T) {
				encoder := testBlockEncoder(compress.LZ4, enabled)
				block, err := encoder.Encode(t.Context(), tt.input)
				require.Nil(t, block)
				require.ErrorContains(t, err, "prepared input must contain rows and columns")
				require.ErrorContains(t, err, `column "missing" has no data`)
				block, err = encoder.Encode(t.Context(), proto.Input{{Name: "n", Data: &values}})
				require.NoError(t, err)
				require.Equal(t, 1, block.Rows())
			})
		}
	}
}

func BenchmarkPreparedBlockEncode(b *testing.B) {
	for _, shape := range []struct{ rows, width int }{{1 << 20, 8}, {512 << 10, 128}} {
		values := proto.ColStr{Buf: make([]byte, 0, shape.rows*shape.width), Pos: make([]proto.Position, 0, shape.rows)}
		value := bytes.Repeat([]byte("x"), shape.width)
		for row := range shape.rows {
			value[0] = byte(row)
			values.AppendBytes(value)
		}
		input := proto.Input{{Name: "s", Data: &values}}
		b.Run(fmt.Sprintf("rows=%d/width=%d", shape.rows, shape.width), func(b *testing.B) {
			encoder := testBlockEncoder(compress.LZ4, true)
			block, err := encoder.Encode(b.Context(), input)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				block, err = encoder.Encode(b.Context(), input)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(block.Size()), "payload-B")
			b.ReportMetric(float64(block.Capacity()), "capacity-B")
		})
	}
}

func TestBlockEncoderOutputSizeHint(t *testing.T) {
	payload := make([]byte, 2*compress.BlockSize+123)
	_, err := rand.New(rand.NewSource(42)).Read(payload)
	require.NoError(t, err)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("compression=%v", enabled), func(t *testing.T) {
			encoder := testBlockEncoder(compress.LZ4, enabled)
			var values proto.ColStr
			values.AppendBytes(payload)
			input := proto.Input{{Name: "s", Data: &values}}
			first, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			second, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			require.Equal(t, first.data, second.data)
			require.NotSame(t, &first.data[0], &second.data[0])
			require.Less(t, second.Capacity()-second.Size(), second.Size()/8+8192)

			mismatched := proto.ColUInt64{1, 2}
			failed, err := encoder.Encode(t.Context(), append(input, proto.InputColumn{Name: "n", Data: &mismatched}))
			require.Error(t, err)
			require.Nil(t, failed)
			require.Equal(t, second.Size(), encoder.lastBlockSize)

			values.Reset()
			values.Append("small")
			small, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			require.Less(t, small.Capacity(), 1024)
			next, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			require.Equal(t, small.data, next.data)
			require.Less(t, next.Capacity(), 1024)
			require.Equal(t, first.data, second.data)
		})
	}
}

func TestClientInvalidCompressionFrameSize(t *testing.T) {
	for _, size := range []int{-1, compress.MaxFrameSize + 1} {
		options := Options{Compression: CompressionLZ4, CompressionFrameSize: size}
		client, err := Dial(t.Context(), options)
		require.Nil(t, client)
		require.ErrorContains(t, err, "compression frame size")
		client, err = Connect(t.Context(), nil, options)
		require.Nil(t, client)
		require.ErrorContains(t, err, "compression frame size")
	}
}

type blockBatchConn struct {
	net.Conn
	output    bytes.Buffer
	writes    int
	deadlines int
	failure   error
}

func (c *blockBatchConn) Write(p []byte) (int, error) {
	c.writes++
	if c.failure != nil {
		return 0, c.failure
	}
	return c.output.Write(p)
}

func (c *blockBatchConn) SetWriteDeadline(time.Time) error {
	c.deadlines++
	return nil
}

func TestBlockClientWriterBatchesFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var buffer proto.Buffer
	conn := &blockBatchConn{}
	client := &Client{conn: conn, writer: proto.NewWriter(conn, &buffer), lg: zap.NewNop()}
	client.writer.ChainBuffer(func(b *proto.Buffer) { b.PutRaw([]byte("header")) })
	output := blockClientWriter{ctx: ctx, client: client}
	var expected bytes.Buffer
	expected.WriteString("header")
	for i := range 70 {
		frame := bytes.Repeat([]byte{byte(i + 1)}, 8<<10)
		expected.Write(frame)
		n, err := output.Write(frame)
		require.NoError(t, err)
		require.Equal(t, len(frame), n)
		clear(frame)
		if i == 0 {
			require.Zero(t, conn.writes)
		}
	}
	require.NoError(t, client.flush(ctx))
	require.Equal(t, expected.Bytes(), conn.output.Bytes())
	require.Equal(t, 3, conn.writes)
	require.Equal(t, 6, conn.deadlines)
	require.LessOrEqual(t, cap(buffer.Buf), 2*compressedWriteBufferSize)
}

func TestBlockClientWriterLargeFrame(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var buffer proto.Buffer
	conn := &blockBatchConn{}
	client := &Client{conn: conn, writer: proto.NewWriter(conn, &buffer), lg: zap.NewNop()}
	output := blockClientWriter{ctx: ctx, client: client}
	small := []byte("small frame")
	_, err := output.Write(small)
	require.NoError(t, err)
	large := bytes.Repeat([]byte("x"), 3*compressedWriteBufferSize)
	_, err = output.Write(large)
	require.NoError(t, err)
	expected := append(bytes.Clone(small), large...)
	clear(large)
	require.Equal(t, expected, conn.output.Bytes())
	require.Equal(t, 2, conn.deadlines)
	require.Less(t, cap(buffer.Buf), compressedWriteBufferSize)
}

func TestBlockClientWriterFailure(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprint(direct), func(t *testing.T) {
			var buffer proto.Buffer
			conn := &blockBatchConn{failure: errors.New("write failed")}
			client := &Client{conn: conn, writer: proto.NewWriter(conn, &buffer), lg: zap.NewNop()}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			output := blockClientWriter{ctx: ctx, client: client}
			size := compressedWriteBufferSize
			if !direct {
				_, err := output.Write(make([]byte, size/2))
				require.NoError(t, err)
				size /= 2
			}
			n, err := output.Write(make([]byte, size))
			require.Zero(t, n)
			require.ErrorIs(t, err, conn.failure)
			require.Equal(t, 1, conn.writes)
			cancel()
			_, err = output.Write([]byte("canceled"))
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, conn.writes)
		})
	}
}

func TestBlockEncoderOutputSizeJitter(t *testing.T) {
	const baseSize = 2 << 20
	payload := make([]byte, baseSize*105/100)
	_, err := rand.New(rand.NewSource(42)).Read(payload)
	require.NoError(t, err)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("compression=%v", enabled), func(t *testing.T) {
			encoder := testBlockEncoder(compress.LZ4, enabled)
			values := proto.ColUInt8(payload[:baseSize])
			input := proto.Input{{Name: "n", Data: &values}}
			_, err := encoder.Encode(t.Context(), input)
			require.NoError(t, err)
			for _, percent := range []int{105, 97, 102, 105, 97, 102} {
				values = payload[:baseSize*percent/100]
				block, err := encoder.Encode(t.Context(), input)
				require.NoError(t, err)
				require.Equal(t, len(values), block.Rows())
				require.LessOrEqual(t, block.Capacity()-block.Size(), block.Size()/4+8192)
			}
		})
	}
}

func BenchmarkPreparedBlockEncodeJitter(b *testing.B) {
	const baseSize = 5 << 20
	payload := make([]byte, baseSize*105/100)
	if _, err := rand.New(rand.NewSource(42)).Read(payload); err != nil {
		b.Fatal(err)
	}
	for _, jitter := range []bool{false, true} {
		b.Run(fmt.Sprintf("jitter=%v", jitter), func(b *testing.B) {
			encoder := testBlockEncoder(compress.LZ4, true)
			values := proto.ColUInt8(payload[:baseSize])
			input := proto.Input{{Name: "n", Data: &values}}
			if _, err := encoder.Encode(b.Context(), input); err != nil {
				b.Fatal(err)
			}
			percents := []int{100}
			if jitter {
				percents = []int{105, 97, 102}
			}
			var payloadBytes, capacityBytes uint64
			index := 0
			b.ReportAllocs()
			for b.Loop() {
				values = payload[:baseSize*percents[index%len(percents)]/100]
				index++
				block, err := encoder.Encode(b.Context(), input)
				if err != nil {
					b.Fatal(err)
				}
				payloadBytes += uint64(block.Size())
				capacityBytes += uint64(block.Capacity())
			}
			b.ReportMetric(float64(payloadBytes)/float64(b.N), "payload-B/op")
			b.ReportMetric(float64(capacityBytes)/float64(b.N), "capacity-B/op")
		})
	}
}
