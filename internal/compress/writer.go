package compress

import (
	"github.com/go-faster/city"
	"github.com/go-faster/errors"
	"github.com/pierrec/lz4/v4"
)

// Writer encodes compressed blocks.
type Writer struct {
	Data []byte

	c *lz4.Compressor
}

// Compress buf into Data.
func (w *Writer) Compress(buf []byte) error {
	if len(buf) > maxBlockSize {
		return errors.Errorf("buf size %d > %d (multiple block encoding not implemented)", len(buf), maxBlockSize)
	}

	maxSize := lz4.CompressBlockBound(len(buf))
	w.Data = append(w.Data[:0], make([]byte, maxSize+headerSize)...)
	_ = w.Data[:headerSize]
	w.Data[hMethod] = byte(LZ4)

	n, err := w.c.CompressBlock(buf, w.Data[headerSize:])
	if err != nil {
		return errors.Wrap(err, "block")
	}

	w.Data = w.Data[:n+headerSize]

	bin.PutUint32(w.Data[hDataSize:], uint32(n))
	bin.PutUint32(w.Data[hRawSize:], uint32(len(buf)))
	hash := city.CH128(w.Data[hMethod:])
	bin.PutUint64(w.Data[0:8], hash.Low)
	bin.PutUint64(w.Data[8:16], hash.High)

	return nil
}

func NewWriter() *Writer {
	return &Writer{
		c: &lz4.Compressor{},
	}
}
