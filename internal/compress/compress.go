// Package compress implements compression support.
package compress

import "encoding/binary"

//go:generate go run github.com/dmarkham/enumer -transform snake_upper -type Method -output method_enum.go

// Method is compression codec.
type Method byte

// Possible compression methods.
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

	hRawSize  = 17
	hDataSize = 21
	hMethod   = 16

	rawSizeOffset = 9
)

var bin = binary.LittleEndian
