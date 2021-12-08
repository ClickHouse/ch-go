// Package proto implements ClickHouse wire protocol.
package proto

import "encoding/binary"

// Defaults for ClientHello.
const (
	Minor   = 1
	Major   = 1
	Version = 54429
	Name    = "go-faster/ch"
)

// ClickHouse uses Little Endian.
var bin = binary.LittleEndian
