package proto

import (
	"github.com/go-faster/errors"
)

const JSONStringSerializationVersion uint64 = 1

// ColJSONStr represents String column.
//
// Use ColJSONBytes for []bytes ColumnOf implementation.
type ColJSONStr struct {
	str ColStr
}

// Append string to column.
func (c *ColJSONStr) Append(v string) {
	c.str.Append(v)
}

// AppendBytes append byte slice as string to column.
func (c *ColJSONStr) AppendBytes(v []byte) {
	c.str.AppendBytes(v)
}

func (c *ColJSONStr) AppendArr(v []string) {
	c.str.AppendArr(v)
}

// Compile-time assertions for ColJSONStr.
var (
	_ ColInput          = ColJSONStr{}
	_ ColResult         = (*ColJSONStr)(nil)
	_ Column            = (*ColJSONStr)(nil)
	_ ColumnOf[string]  = (*ColJSONStr)(nil)
	_ Arrayable[string] = (*ColJSONStr)(nil)
)

// Type returns ColumnType of JSON.
func (ColJSONStr) Type() ColumnType {
	return ColumnTypeJSON
}

// Rows returns count of rows in column.
func (c ColJSONStr) Rows() int {
	return c.str.Rows()
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *ColJSONStr) Reset() {
	c.str.Reset()
}

// EncodeColumn encodes String rows to *Buffer.
func (c ColJSONStr) EncodeColumn(b *Buffer) {
	b.PutUInt64(JSONStringSerializationVersion)

	c.str.EncodeColumn(b)
}

// WriteColumn writes JSON rows to *Writer.
func (c ColJSONStr) WriteColumn(w *Writer) {
	w.ChainBuffer(func(b *Buffer) {
		b.PutUInt64(JSONStringSerializationVersion)
	})

	c.str.WriteColumn(w)
}

// ForEach calls f on each string from column.
func (c ColJSONStr) ForEach(f func(i int, s string) error) error {
	return c.str.ForEach(f)
}

// First returns the first row of the column.
func (c ColJSONStr) First() string {
	return c.str.First()
}

// Row returns row with number i.
func (c ColJSONStr) Row(i int) string {
	return c.str.Row(i)
}

// RowBytes returns row with number i as byte slice.
func (c ColJSONStr) RowBytes(i int) []byte {
	return c.str.RowBytes(i)
}

// ForEachBytes calls f on each string from column as byte slice.
func (c ColJSONStr) ForEachBytes(f func(i int, b []byte) error) error {
	return c.str.ForEachBytes(f)
}

// DecodeColumn decodes String rows from *Reader.
func (c *ColJSONStr) DecodeColumn(r *Reader, rows int) error {
	jsonSerializationVersion, err := r.UInt64()
	if err != nil {
		return errors.Wrap(err, "failed to read json serialization version")
	}

	if jsonSerializationVersion != JSONStringSerializationVersion {
		return errors.Errorf("received invalid JSON string serialization version %d. Setting \"output_format_native_write_json_as_string\" must be enabled.", jsonSerializationVersion)
	}

	return c.str.DecodeColumn(r, rows)
}

// LowCardinality returns LowCardinality(JSON).
func (c *ColJSONStr) LowCardinality() *ColLowCardinality[string] {
	return c.str.LowCardinality()
}

// Array is helper that creates Array(JSON).
func (c *ColJSONStr) Array() *ColArr[string] {
	return c.str.Array()
}

// Nullable is helper that creates Nullable(JSON).
func (c *ColJSONStr) Nullable() *ColNullable[string] {
	return c.str.Nullable()
}

// ColJSONBytes is ColJSONStr wrapper to be ColumnOf for []byte.
type ColJSONBytes struct {
	ColJSONStr
}

// Row returns row with number i.
func (c ColJSONBytes) Row(i int) []byte {
	return c.RowBytes(i)
}

// Append byte slice to column.
func (c *ColJSONBytes) Append(v []byte) {
	c.AppendBytes(v)
}

// AppendArr append slice of byte slices to column.
func (c *ColJSONBytes) AppendArr(v [][]byte) {
	for _, s := range v {
		c.Append(s)
	}
}

// Array is helper that creates Array(JSON).
func (c *ColJSONBytes) Array() *ColArr[[]byte] {
	return &ColArr[[]byte]{
		Data: c,
	}
}

// Nullable is helper that creates Nullable(JSON).
func (c *ColJSONBytes) Nullable() *ColNullable[[]byte] {
	return &ColNullable[[]byte]{
		Values: c,
	}
}
