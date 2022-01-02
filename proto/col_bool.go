package proto

// ColBool is Bool column.
type ColBool []bool

// Compile-time assertions for ColBool.
var (
	_ ColInput  = ColBool{}
	_ ColResult = (*ColBool)(nil)
	_ Column    = (*ColBool)(nil)
)

// Type returns ColumnType of Bool.
func (ColBool) Type() ColumnType {
	return ColumnTypeBool
}

// Rows returns count of rows in column.
func (c ColBool) Rows() int {
	return len(c)
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *ColBool) Reset() {
	*c = (*c)[:0]
}
