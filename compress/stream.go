package compress

import (
	"io"

	"github.com/go-faster/errors"
)

// BlockSize is the default uncompressed frame size for StreamWriter.
const BlockSize = 1 << 20

// MaxFrameSize leaves room for incompressible frames within the reader size limit.
const MaxFrameSize = 64 << 20

var errNilStreamDestination = errors.New("compression stream has no destination")

// StreamWriter writes a sequence of ClickHouse compression frames. Flush ends
// the current frame, without closing the destination. Errors are sticky until
// Reset. A StreamWriter must not be used concurrently.
type StreamWriter struct {
	dst       io.Writer
	codec     *Writer
	buffer    []byte
	frameSize int
	err       error
}

// NewStreamWriter creates a bounded compression stream. frameSize sets the maximum
// uncompressed bytes per frame; zero uses BlockSize. Sizes outside [1, MaxFrameSize]
// cause Write and Flush to return an error, including after Reset. With a nil
// destination, Write and Flush return an error until Reset supplies a destination.
func NewStreamWriter(dst io.Writer, level Level, method Method, frameSize int) *StreamWriter {
	if frameSize == 0 {
		frameSize = BlockSize
	}
	w := &StreamWriter{codec: NewWriter(level, method), frameSize: frameSize}
	w.Reset(dst)
	return w
}

// Reset discards pending data and errors, retaining buffers for reuse. A nil
// destination disables writes and flushes until a destination is supplied.
func (w *StreamWriter) Reset(dst io.Writer) {
	w.dst, w.err = dst, nil
	if w.frameSize < 1 || w.frameSize > MaxFrameSize {
		w.err = errors.Errorf("compression frame size must be between 1 and %d bytes", MaxFrameSize)
	} else if dst == nil {
		w.err = errNilStreamDestination
	}
	w.buffer = w.buffer[:0]
}

// Write compresses complete frames and retains at most one incomplete frame.
// The count measures uncompressed input, not bytes written to the destination.
// On failure it excludes input from this call added to the failed frame; bytes
// buffered and reported by earlier calls cannot be retracted.
func (w *StreamWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n := 0
	for len(p) > 0 {
		if len(w.buffer) == 0 && len(p) >= w.frameSize {
			if err := w.frame(p[:w.frameSize]); err != nil {
				return n, err
			}
			p, n = p[w.frameSize:], n+w.frameSize
			continue
		}
		if w.buffer == nil {
			w.buffer = make([]byte, 0, w.frameSize)
		}
		count := min(len(p), w.frameSize-len(w.buffer))
		w.buffer = append(w.buffer, p[:count]...)
		if len(w.buffer) == w.frameSize {
			if err := w.Flush(); err != nil {
				return n, err
			}
		}
		p, n = p[count:], n+count
	}
	return n, nil
}

func (w *StreamWriter) frame(data []byte) error {
	if w.err = w.codec.Compress(data); w.err != nil {
		return w.err
	}
	var n int
	n, w.err = w.dst.Write(w.codec.Data)
	if w.err == nil && n != len(w.codec.Data) {
		w.err = io.ErrShortWrite
	}
	return w.err
}

// Flush emits the final partial frame. Repeated or empty flushes emit no data.
func (w *StreamWriter) Flush() error {
	if w.err != nil {
		return w.err
	}
	if len(w.buffer) == 0 {
		return nil
	}
	if err := w.frame(w.buffer); err != nil {
		return err
	}
	w.buffer = w.buffer[:0]
	return nil
}
