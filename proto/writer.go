package proto

import (
	"io"
	"net"
)

// Writer is a column writer.
//
// It helps to reduce memory footprint by writing column using vector I/O.
type Writer struct {
	conn io.Writer

	buf       *Buffer
	bufOffset int
	needCut   bool

	vec       net.Buffers
	streaming bool
	err       error
	written   int64
}

// NewWriter creates new [Writer].
func NewWriter(conn io.Writer, buf *Buffer) *Writer {
	w := &Writer{
		conn: conn,
		buf:  buf,
		vec:  make(net.Buffers, 0, 16),
	}
	return w
}

// NewStreamingWriter writes chained data as it is supplied, buffering small
// ChainBuffer calls up to 64 KiB. ChainBuffer callbacks must bound their own
// writes; a single callback can exceed that size. Flush reports write errors.
// Unlike NewWriter, it does not retain slices passed to ChainWrite.
func NewStreamingWriter(conn io.Writer, buf *Buffer) *Writer {
	return &Writer{conn: conn, buf: buf, streaming: true}
}

const streamBufferSize = 64 << 10

func (w *Writer) write(data []byte) {
	if w.err != nil || len(data) == 0 {
		return
	}
	n, err := w.conn.Write(data)
	w.written += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.err = err
}

func (w *Writer) flushBuffer() {
	w.write(w.buf.Buf)
	w.buf.Reset()
}

// ChainWrite queues data for Flush, or writes it immediately in streaming mode.
// A buffered writer may retain the slice until Flush; a streaming writer does not.
func (w *Writer) ChainWrite(data []byte) {
	if w.streaming {
		w.flushBuffer()
		w.write(data)
		return
	}
	w.cutBuffer()
	w.vec = append(w.vec, data)
}

// ChainBuffer appends data to the encoding buffer. A streaming writer may flush
// it immediately; Flush sends any remaining data and reports errors. Callbacks
// must not retain the buffer and should bound their writes in streaming mode.
func (w *Writer) ChainBuffer(cb func(*Buffer)) {
	if w.err != nil {
		return
	}
	cb(w.buf)
	if w.streaming && len(w.buf.Buf) >= streamBufferSize {
		w.flushBuffer()
	}
}

func (w *Writer) cutBuffer() {
	newOffset := len(w.buf.Buf)
	data := w.buf.Buf[w.bufOffset:newOffset:newOffset]
	if len(data) == 0 {
		return
	}
	w.bufOffset = newOffset
	w.vec = append(w.vec, data)
}

func (w *Writer) reset() {
	w.bufOffset = 0
	w.needCut = false
	w.buf.Reset()
	// Do not hold references, to avoid memory leaks.
	clear(w.vec)
	w.vec = w.vec[:0]
}

// Flush flushes all data to writer.
func (w *Writer) Flush() (n int64, err error) {
	if w.streaming {
		w.flushBuffer()
		n, w.written = w.written, 0
		return n, w.err
	}
	w.cutBuffer()
	n, err = w.vec.WriteTo(w.conn)
	w.reset()
	return n, err
}

// writeColumnChunks bounds temporary encoding space for columns that cannot be
// written directly from their backing storage (endianness conversion, pure Go).
func writeColumnChunks(w *Writer, rows, width int, encode func(*Buffer, int, int)) {
	step := rows
	if w.streaming {
		step = max(1, streamBufferSize/width)
	}
	for start := 0; start < rows && w.err == nil; start += step {
		end := min(start+step, rows)
		w.ChainBuffer(func(b *Buffer) { encode(b, start, end) })
	}
}
