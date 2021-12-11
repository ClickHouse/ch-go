// Package compress implements compression support.
package compress

import (
	"encoding/binary"
	"io"

	"github.com/go-faster/errors"
	"github.com/pierrec/lz4/v4"
)

//go:generate go run github.com/dmarkham/enumer -transform snake_upper -type Method -output method_enum.go

type Method byte

const (
	None Method = 0x02
	LZ4  Method = 0x82
	ZSTD Method = 0x90
)

const (
	checksumSize       = 16
	compressHeaderSize = 1 + 4 + 4
	headerSize         = checksumSize + compressHeaderSize
	maxBlockSize       = 1024 * 1024 * 1   // 1MB
	maxDataSize        = 1024 * 1024 * 128 // 128MB
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

	const (
		hDataSize = 17
		hRawSize  = 21
		hMethod   = 16

		dataSizeOffset = 9
	)
	var (
		dataSize = int(binary.LittleEndian.Uint32(c.header[hDataSize:])) - dataSizeOffset
		rawSize  = int(binary.LittleEndian.Uint32(c.header[hRawSize:]))
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
