package ch

import (
	"bytes"
	"context"
	"io"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// PreparedBlock owns the serialized Native block, including compression frames
// when enabled. It does not retain input columns and is immutable after encoding.
// It can be reused with clients negotiating the same protocol version and
// compression mode. The codec itself need not match the receiving client's codec.
type PreparedBlock struct {
	data        []byte
	rows        int
	version     int
	compression proto.Compression
}

// Rows returns the number of rows in the block.
func (b *PreparedBlock) Rows() int { return b.rows }

// Size returns the serialized payload size, excluding the Data packet envelope.
func (b *PreparedBlock) Size() int { return len(b.data) }

// Capacity returns the retained payload allocation size for memory accounting.
func (b *PreparedBlock) Capacity() int { return cap(b.data) }

// BlockEncoder serializes blocks independently of query execution. Its scratch
// space is reused across blocks. The previous successful encoded size is used as
// an allocation hint with 12.5% headroom for size variation in the next owned
// output. It is not safe for concurrent calls, but separate encoders can run
// concurrently with each other and with the originating client.
// Column WriteColumn and EncodeState implementations must use bounded writes to
// avoid allocating an entire column in a single ChainBuffer callback.
type BlockEncoder struct {
	version       int
	compression   proto.Compression
	stream        *compress.StreamWriter
	buffer        proto.Buffer
	lastBlockSize int
}

// NewBlockEncoder creates an independent encoder using this connection's
// negotiated protocol and compression settings. Call it after Dial or Connect.
// Prepared input must have fully resolved types; server-side type inference is
// not available after serialization.
func (c *Client) NewBlockEncoder() *BlockEncoder {
	e := &BlockEncoder{version: c.protocolVersion, compression: c.compression}
	if c.compression == proto.CompressionEnabled {
		e.stream = compress.NewStreamWriter(nil, c.compressionLevel, c.compressionMethod, c.compressionFrameSize)
	}
	return e
}

// Encode copies input into an owned block. Input columns can be reset after this
// call returns. An error discards the incomplete block. Retained output memory is
// proportional to encoded size; callers must bound their prepared-block queues.
// Significantly overestimated output capacity is trimmed before returning.
func (e *BlockEncoder) Encode(ctx context.Context, input proto.Input) (*PreparedBlock, error) {
	for _, column := range input {
		if column.Data == nil {
			return nil, errors.Errorf("prepared input must contain rows and columns: column %q has no data", column.Name)
		}
	}
	if len(input) == 0 || input[0].Data.Rows() <= 0 {
		return nil, errors.New("prepared input must contain rows and columns")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var dst bytes.Buffer
	dst.Grow(e.lastBlockSize + e.lastBlockSize/8)
	if err := e.write(blockContextWriter{ctx: ctx, dst: &dst}, input); err != nil {
		return nil, err
	}
	data := dst.Bytes()
	if len(data) < cap(data)/2 && cap(data)-len(data) > 64<<10 {
		data = bytes.Clone(data)
	}
	e.lastBlockSize = len(data)
	return &PreparedBlock{data: data, rows: input[0].Data.Rows(), version: e.version, compression: e.compression}, nil
}

func (e *BlockEncoder) write(output io.Writer, input proto.Input) error {
	if e.stream != nil {
		e.stream.Reset(output)
		defer e.stream.Reset(nil)
		output = e.stream
	}
	e.buffer.Reset()
	writer := proto.NewStreamingWriter(output, &e.buffer)
	if err := inputBlock(input).WriteBlock(writer, e.version, input); err != nil {
		return err
	}
	if _, err := writer.Flush(); err != nil {
		return err
	}
	if e.stream != nil {
		return e.stream.Flush()
	}
	return nil
}

func inputBlock(input []proto.InputColumn) proto.Block {
	b := proto.Block{Columns: len(input)}
	if len(input) > 0 {
		b.Rows = input[0].Data.Rows()
		// TODO(ernado): investigate and document
		b.Info.BucketNum = -1
	}
	return b
}

type blockContextWriter struct {
	ctx context.Context
	dst io.Writer
}

func (w blockContextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.dst.Write(p)
}

const compressedWriteBufferSize = 256 << 10

// Small frames are copied into the client's buffer so codec storage can be reused.
// Large frames are flushed directly. Existing block/query flushes drain partial
// batches and preserve packet ordering through the same writer.
type blockClientWriter struct {
	ctx    context.Context
	client *Client
}

func (w blockClientWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) >= compressedWriteBufferSize {
		w.client.writer.ChainWrite(p)
	} else {
		var pending int
		w.client.writer.ChainBuffer(func(buffer *proto.Buffer) {
			buffer.PutRaw(p)
			pending = len(buffer.Buf)
		})
		if pending < compressedWriteBufferSize {
			return len(p), nil
		}
	}
	if err := w.client.flush(w.ctx); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Client) sendPreparedInput(ctx context.Context, next func(context.Context) (*PreparedBlock, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		block, err := next(ctx)
		if errors.Is(err, io.EOF) {
			if block != nil {
				return errors.New("prepared input must not return a block with EOF")
			}
			return c.encodeBlankBlock(ctx)
		}
		if err != nil {
			return errors.Wrap(err, "prepared input")
		}
		if block == nil || block.rows <= 0 || len(block.data) == 0 {
			return errors.New("empty prepared block")
		}
		if block.version != c.protocolVersion || block.compression != c.compression {
			return errors.New("prepared block protocol or compression mode does not match client")
		}
		c.encodeDataHeader("")
		c.writer.ChainWrite(block.data)
		if err := c.flush(ctx); err != nil {
			return err
		}
		c.metricsInc(ctx, queryMetricsDelta{BlocksSent: 1})
	}
}
