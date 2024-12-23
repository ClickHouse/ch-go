// Code generated by ./cmd/ch-gen-col, DO NOT EDIT.

package proto

// ColUInt16 represents UInt16 column.
type ColUInt16 []uint16

// Compile-time assertions for ColUInt16.
var (
	_ ColInput  = ColUInt16{}
	_ ColResult = (*ColUInt16)(nil)
	_ Column    = (*ColUInt16)(nil)
)

// Rows returns count of rows in column.
func (c ColUInt16) Rows() int {
	return len(c)
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *ColUInt16) Reset() {
	*c = (*c)[:0]
}

// Type returns ColumnType of UInt16.
func (ColUInt16) Type() ColumnType {
	return ColumnTypeUInt16
}

// Row returns i-th row of column.
func (c ColUInt16) Row(i int) uint16 {
	return c[i]
}

// Append uint16 to column.
func (c *ColUInt16) Append(v uint16) {
	*c = append(*c, v)
}

// Append uint16 slice to column.
func (c *ColUInt16) AppendArr(vs []uint16) {
	*c = append(*c, vs...)
}

// LowCardinality returns LowCardinality for UInt16.
func (c *ColUInt16) LowCardinality() *ColLowCardinality[uint16] {
	return &ColLowCardinality[uint16]{
		index: c,
	}
}

// Array is helper that creates Array of uint16.
func (c *ColUInt16) Array() *ColArr[uint16] {
	return &ColArr[uint16]{
		Data: c,
	}
}

// Nullable is helper that creates Nullable(uint16).
func (c *ColUInt16) Nullable() *ColNullable[uint16] {
	return &ColNullable[uint16]{
		Values: c,
	}
}

// NewArrUInt16 returns new Array(UInt16).
func NewArrUInt16() *ColArr[uint16] {
	return &ColArr[uint16]{
		Data: new(ColUInt16),
	}
}
