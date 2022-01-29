package proto

import (
	"github.com/google/uuid"
)

// ColUUID is UUID column.
type ColUUID []uuid.UUID

// Compile-time assertions for ColUUID.
var (
	_ ColInput  = ColUUID{}
	_ ColResult = (*ColUUID)(nil)
	_ Column    = (*ColUUID)(nil)
)

func (c ColUUID) Type() ColumnType { return ColumnTypeUUID }
func (c ColUUID) Rows() int        { return len(c) }
func (c *ColUUID) Reset()          { *c = (*c)[:0] }

// convEndian16 converts 16-byte b between little endian and big endian.
//
// NB: UUID in ClickHouse is represented as 128-bit integer and can't be
// interpreted as FixedString(16) directly, so converting is required.
func convEndian16(b []byte) {
	b[8], b[15] = b[15], b[8]
	b[9], b[14] = b[14], b[9]
	b[10], b[13] = b[13], b[10]
	b[11], b[12] = b[12], b[11]
	b[0], b[7] = b[7], b[0]
	b[1], b[6] = b[6], b[1]
	b[2], b[5] = b[5], b[2]
	b[3], b[4] = b[4], b[3]
}
