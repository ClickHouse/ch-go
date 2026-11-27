package proto

import (
	"github.com/go-faster/errors"
)

// ColumnTypeAggregateFunction is the base of the `AggregateFunction(...)` types.
const ColumnTypeAggregateFunction ColumnType = "AggregateFunction"

// ColAggregateFunction is a column of `AggregateFunction(...)` states.
// S is the state of one row, for example [DDSketch].
//
// ClickHouse writes the states one after the other with no length prefix,
// and each aggregate function has its own state layout. The column cannot
// skip or copy a state that it cannot decode, so the state type S must
// match the aggregate function, its parameters and its argument types.
//
// See https://clickhouse.com/docs/sql-reference/data-types/aggregatefunction.
type ColAggregateFunction[S any, P interface {
	*S
	Encoder
	Decoder
}] struct {
	t    ColumnType
	data []S
}

// NewAggregateFunction returns a column of the type t with states of the
// type S. The type t is the full type name as the server prints it:
//
//	proto.NewAggregateFunction[proto.DDSketch]("AggregateFunction(quantilesDD(0.01, 0.5, 0.99), Float64)")
func NewAggregateFunction[S any, P interface {
	*S
	Encoder
	Decoder
}](t ColumnType) *ColAggregateFunction[S, P] {
	return &ColAggregateFunction[S, P]{t: t}
}

// Compile-time assertions for ColAggregateFunction.
var _ ColumnOf[DDSketch] = (*ColAggregateFunction[DDSketch, *DDSketch])(nil)

// Type returns the type name that the column was made with.
func (c ColAggregateFunction[S, P]) Type() ColumnType {
	return c.t
}

// Rows returns count of rows in column.
func (c ColAggregateFunction[S, P]) Rows() int {
	return len(c.data)
}

// Row returns the state of the row i.
func (c ColAggregateFunction[S, P]) Row(i int) S {
	return c.data[i]
}

// Append appends a state to the column.
func (c *ColAggregateFunction[S, P]) Append(v S) {
	c.data = append(c.data, v)
}

// AppendArr appends states to the column.
func (c *ColAggregateFunction[S, P]) AppendArr(vs []S) {
	c.data = append(c.data, vs...)
}

// Reset resets data in row, preserving capacity for efficiency.
func (c *ColAggregateFunction[S, P]) Reset() {
	c.data = c.data[:0]
}

// EncodeColumn encodes ColAggregateFunction rows to *Buffer.
func (c ColAggregateFunction[S, P]) EncodeColumn(b *Buffer) {
	for i := range c.data {
		P(&c.data[i]).Encode(b)
	}
}

// WriteColumn writes ColAggregateFunction rows to *Writer.
func (c ColAggregateFunction[S, P]) WriteColumn(w *Writer) {
	w.ChainBuffer(c.EncodeColumn)
}

// DecodeColumn decodes ColAggregateFunction rows from *Reader.
//
// Each decoded state owns its memory. A state that the caller got from Row
// stays valid after Reset and after the next block.
func (c *ColAggregateFunction[S, P]) DecodeColumn(r *Reader, rows int) error {
	for i := 0; i < rows; i++ {
		var v S
		if err := P(&v).Decode(r); err != nil {
			return errors.Wrapf(err, "state %d", i)
		}
		c.data = append(c.data, v)
	}
	return nil
}
