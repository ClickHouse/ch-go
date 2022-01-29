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
