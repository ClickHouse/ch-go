package compress

import (
	"encoding/binary"
	"math"

	"github.com/go-faster/city"
	"github.com/go-faster/errors"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	LevelZero         Level = 0
	LevelLZ4HCDefault Level = 9
	LevelLZ4HCMax     Level = 12
)

// Writer encodes compressed blocks.
type Writer struct {
	Data []byte

	method Method

	lz4   *lz4.Compressor
	lz4hc *lz4.CompressorHC
	zstd  *zstd.Encoder
}

// Compress buf into Data.
//
// Data is a sequence of compressed blocks, each holding at most
// maxCompressBlockSize bytes of buf, like the blocks of `clickhouse-client`.
func (w *Writer) Compress(buf []byte) error {
	w.Data = w.Data[:0]
	for {
		n := min(len(buf), maxCompressBlockSize)
		if err := w.compressBlock(buf[:n]); err != nil {
			return err
		}
		buf = buf[n:]
		if len(buf) == 0 {
			return nil
		}
	}
}

// compressBlock appends buf to Data as one compressed block.
func (w *Writer) compressBlock(buf []byte) error {
	start := len(w.Data)
	maxSize := lz4.CompressBlockBound(len(buf))
	w.Data = append(w.Data, make([]byte, maxSize+headerSize)...)
	block := w.Data[start:]
	_ = block[:headerSize]
	block[hMethod] = byte(methodTable[w.method])

	var n int

	switch w.method {
	case LZ4:
		compressedSize, err := w.lz4.CompressBlock(buf, block[headerSize:])
		if err != nil {
			return errors.Wrap(err, "block")
		}
		n = compressedSize
	case LZ4HC:
		compressedSize, err := w.lz4hc.CompressBlock(buf, block[headerSize:])
		if err != nil {
			return errors.Wrap(err, "block")
		}
		n = compressedSize
	case ZSTD:
		w.Data = w.zstd.EncodeAll(buf, w.Data[:start+headerSize])
		block = w.Data[start:]
		n = len(block) - headerSize
	case None:
		n = copy(block[headerSize:], buf)
	}

	// security: https://github.com/ClickHouse/ch-go/pull/1041
	if uint64(n)+uint64(compressHeaderSize) > math.MaxUint32 {
		return errors.New("compressed size overflows uint32")
	}

	w.Data = w.Data[:start+n+headerSize]
	block = w.Data[start:]
	binary.LittleEndian.PutUint32(block[hRawSize:], uint32(n+compressHeaderSize))
	binary.LittleEndian.PutUint32(block[hDataSize:], uint32(len(buf)))
	h := city.CH128(block[hMethod:])
	binary.LittleEndian.PutUint64(block[0:8], h.Low)
	binary.LittleEndian.PutUint64(block[8:16], h.High)

	return nil
}

// NewWriter creates a new Writer with the specified compression level that supports the specified method.
func NewWriter(l Level, m Method) *Writer {
	var err error
	var zstdWriter *zstd.Encoder
	var lz4Writer *lz4.Compressor
	var lz4hcWriter *lz4.CompressorHC

	switch m {
	case LZ4:
		lz4Writer = &lz4.Compressor{}
	case LZ4HC:
		levelLZ4HC := l
		if levelLZ4HC == 0 {
			levelLZ4HC = LevelLZ4HCDefault
		} else {
			levelLZ4HC = Level(math.Min(float64(levelLZ4HC), float64(LevelLZ4HCMax)))
		}
		lz4hcWriter = &lz4.CompressorHC{Level: lz4.CompressionLevel(1 << (8 + levelLZ4HC))}
	case ZSTD:
		zstdWriter, err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
			zstd.WithLowerEncoderMem(true),
		)
		if err != nil {
			panic(err)
		}
	default:
	}

	return &Writer{
		method: m,
		lz4:    lz4Writer,
		lz4hc:  lz4hcWriter,
		zstd:   zstdWriter,
	}
}
