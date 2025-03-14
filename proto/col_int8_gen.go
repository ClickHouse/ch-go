// Code generated by ./cmd/ch-gen-col, DO NOT EDIT.

package proto

// ColInt8 represents Int8 column.
type ColInt8 []int8

// Compile-time assertions for ColInt8.
var (
	_ ColInput  = ColInt8{}
	_ ColResult = (*ColInt8)(nil)
	_ Column    = (*ColInt8)(nil)
)

// Rows returns count of rows in column.
func (c ColInt8) Rows() int {
	return len(c)
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *ColInt8) Reset() {
	*c = (*c)[:0]
}

// Type returns ColumnType of Int8.
func (ColInt8) Type() ColumnType {
	return ColumnTypeInt8
}

// Row returns i-th row of column.
func (c ColInt8) Row(i int) int8 {
	return c[i]
}

// Append int8 to column.
func (c *ColInt8) Append(v int8) {
	*c = append(*c, v)
}

// Append int8 slice to column.
func (c *ColInt8) AppendArr(vs []int8) {
	*c = append(*c, vs...)
}

// LowCardinality returns LowCardinality for Int8.
func (c *ColInt8) LowCardinality() *ColLowCardinality[int8] {
	return &ColLowCardinality[int8]{
		index: c,
	}
}

// Array is helper that creates Array of int8.
func (c *ColInt8) Array() *ColArr[int8] {
	return &ColArr[int8]{
		Data: c,
	}
}

// Nullable is helper that creates Nullable(int8).
func (c *ColInt8) Nullable() *ColNullable[int8] {
	return &ColNullable[int8]{
		Values: c,
	}
}

// NewArrInt8 returns new Array(Int8).
func NewArrInt8() *ColArr[int8] {
	return &ColArr[int8]{
		Data: new(ColInt8),
	}
}
