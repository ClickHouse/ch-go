package compress

import (
	"io"

	"github.com/go-faster/errors"
	"github.com/pierrec/lz4/v4"
)

// Reader handles compression.
type Reader struct {
	reader io.Reader
	data   []byte
	pos    int64
	raw    []byte
	header [headerSize]byte
}

// readBlock reads next compressed data into raw and decompresses into data.
func (c *Reader) readBlock() error {
	c.pos = 0

	if _, err := io.ReadFull(c.reader, c.header[:]); err != nil {
		return errors.Wrap(err, "header")
	}

	var (
		dataSize = int(bin.Uint32(c.header[hDataSize:])) - dataSizeOffset
		rawSize  = int(bin.Uint32(c.header[hRawSize:]))
	)
	if dataSize < 0 || dataSize > maxDataSize {
		return errors.Errorf("data size should be %d < %d < %d", 0, dataSize, maxDataSize)
	}
	if rawSize < 0 || rawSize > maxBlockSize {
		return errors.Errorf("raw size should be %d < %d < %d", 0, rawSize, maxBlockSize)
	}
	c.data = append(c.data[:0], make([]byte, dataSize)...)
	c.raw = append(c.raw[:0], make([]byte, rawSize)...)

	switch m := Method(c.header[hMethod]); m {
	case LZ4:
		if _, err := io.ReadFull(c.reader, c.raw); err != nil {
			return errors.Wrap(err, "read raw")
		}
		n, err := lz4.UncompressBlock(c.raw, c.data)
		if err != nil {
			return errors.Wrap(err, "lz4")
		}
		c.data = c.data[:n]
	default:
		return errors.Errorf("compression 0x%02x not implemented", m)
	}

	return nil
}

// Read implements io.Reader.
func (c *Reader) Read(p []byte) (n int, err error) {
	if c.pos >= int64(len(c.data)) {
		if err := c.readBlock(); err != nil {
			return 0, errors.Wrap(err, "read next block")
		}
	}
	n = copy(p, c.data[c.pos:])
	c.pos += int64(n)
	return n, nil
}
