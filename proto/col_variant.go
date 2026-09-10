package proto

import (
	"sort"

	"github.com/go-faster/errors"
)

// Serialization reference:
// https://github.com/ClickHouse/ClickHouse/blob/2038a36c2b330e33e1c0ad06b0d28fd2fd3676a9/src/DataTypes/Serializations/SerializationVariant.h

// VariantNullDiscriminator marks NULL row of Variant column.
const VariantNullDiscriminator uint8 = 255

// MaxVariants is maximum number of types in Variant column.
const MaxVariants = 255

// Discriminators serialization modes.
const (
	VariantDiscriminatorsBasic   uint64 = 0
	VariantDiscriminatorsCompact uint64 = 1
)

// Granule formats of compact discriminators mode.
const (
	variantGranulePlain   uint8 = 0
	variantGranuleCompact uint8 = 1
)

// ColVariant is Variant(T1, T2, ...) column.
//
// Row has discriminator, an index in Columns, or VariantNullDiscriminator for
// NULL. Values are stored sparsely, Columns[i] holds only rows of variant i.
//
// ClickHouse sorts variant types by name, so Columns must be sorted by Type()
// to match server discriminators, see Prepare.
type ColVariant struct {
	Discriminators ColUInt8
	Columns        []Column

	offsets []int  // index of row i in Columns[Discriminators[i]]
	counts  []int  // rows per column
	mode    uint64 // discriminators mode of decoded state
}

// Compile-time assertions for ColVariant.
var (
	_ ColInput     = (*ColVariant)(nil)
	_ ColResult    = (*ColVariant)(nil)
	_ Column       = (*ColVariant)(nil)
	_ StateEncoder = (*ColVariant)(nil)
	_ StateDecoder = (*ColVariant)(nil)
	_ Inferable    = (*ColVariant)(nil)
	_ Preparable   = (*ColVariant)(nil)
)

// NewVariant constructs Variant(T1, T2, ...) of columns sorted by type.
func NewVariant(columns ...Column) *ColVariant {
	return &ColVariant{
		Columns: columns,
	}
}

// SortColumns sorts columns by type as ClickHouse does for Variant and
// Dynamic, so that their order matches discriminators.
func SortColumns(columns []Column) {
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Type() < columns[j].Type()
	})
}

func (c ColVariant) Type() ColumnType {
	types := make([]ColumnType, len(c.Columns))
	for i, v := range c.Columns {
		types[i] = v.Type()
	}
	return ColumnTypeVariant.Sub(types...)
}

func (c ColVariant) Rows() int {
	return c.Discriminators.Rows()
}

// Discriminator returns discriminator of column with type t.
func (c ColVariant) Discriminator(t ColumnType) (uint8, bool) {
	for i, v := range c.Columns {
		if v.Type() == t {
			return uint8(i), true
		}
	}
	return 0, false
}

// RowDiscriminator returns discriminator of row i.
func (c ColVariant) RowDiscriminator(i int) uint8 {
	return c.Discriminators[i]
}

// RowIsNull reports whether row i is NULL.
func (c ColVariant) RowIsNull(i int) bool {
	return c.Discriminators[i] == VariantNullDiscriminator
}

// RowOffset returns index of row i in Columns[RowDiscriminator(i)].
func (c ColVariant) RowOffset(i int) int {
	return c.offsets[i]
}

// AppendDiscriminator appends row of variant d.
//
// Value itself must be appended to Columns[d] by caller.
//
//	v.AppendDiscriminator(0)
//	ints.Append(42)
func (c *ColVariant) AppendDiscriminator(d uint8) {
	c.ensureCounts()
	c.offsets = append(c.offsets, c.counts[d])
	c.counts[d]++
	c.Discriminators.Append(d)
}

// AppendNull appends NULL row.
func (c *ColVariant) AppendNull() {
	c.offsets = append(c.offsets, 0)
	c.Discriminators.Append(VariantNullDiscriminator)
}

func (c *ColVariant) ensureCounts() {
	if n := len(c.Columns) - len(c.counts); n > 0 {
		c.counts = append(c.counts, make([]int, n)...)
	}
}

func (c ColVariant) EncodeState(b *Buffer) {
	b.PutUInt64(VariantDiscriminatorsBasic)
	for _, v := range c.Columns {
		if s, ok := v.(StateEncoder); ok {
			s.EncodeState(b)
		}
	}
}

func (c *ColVariant) DecodeState(r *Reader) error {
	mode, err := r.UInt64()
	if err != nil {
		return errors.Wrap(err, "mode")
	}
	if mode > VariantDiscriminatorsCompact {
		return errors.Errorf("unknown discriminators mode %d", mode)
	}
	c.mode = mode
	for i, v := range c.Columns {
		if s, ok := v.(StateDecoder); ok {
			if err := s.DecodeState(r); err != nil {
				return errors.Wrapf(err, "[%d]", i)
			}
		}
	}
	return nil
}

func (c ColVariant) EncodeColumn(b *Buffer) {
	c.Discriminators.EncodeColumn(b)
	for _, v := range c.Columns {
		v.EncodeColumn(b)
	}
}

func (c ColVariant) WriteColumn(w *Writer) {
	c.Discriminators.WriteColumn(w)
	for _, v := range c.Columns {
		v.WriteColumn(w)
	}
}

func (c *ColVariant) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	if err := c.decodeDiscriminators(r, rows); err != nil {
		return errors.Wrap(err, "discriminators")
	}
	if err := c.index(); err != nil {
		return errors.Wrap(err, "index")
	}
	for i, v := range c.Columns {
		if err := v.DecodeColumn(r, c.counts[i]); err != nil {
			return errors.Wrapf(err, "[%d]", i)
		}
	}
	return nil
}

func (c *ColVariant) decodeDiscriminators(r *Reader, rows int) error {
	if c.mode == VariantDiscriminatorsCompact {
		return c.decodeCompactDiscriminators(r, rows)
	}
	return c.Discriminators.DecodeColumn(r, rows)
}

// index fills offsets and counts of decoded discriminators.
func (c *ColVariant) index() error {
	c.offsets = c.offsets[:0]
	c.counts = c.counts[:0]
	c.ensureCounts()

	for _, d := range c.Discriminators {
		if d == VariantNullDiscriminator {
			c.offsets = append(c.offsets, 0)
			continue
		}
		if int(d) >= len(c.Columns) {
			return errors.Errorf("discriminator %d of %d variants", d, len(c.Columns))
		}
		c.offsets = append(c.offsets, c.counts[d])
		c.counts[d]++
	}
	return nil
}

func (c *ColVariant) Reset() {
	c.Discriminators.Reset()
	for _, v := range c.Columns {
		v.Reset()
	}
	c.offsets = c.offsets[:0]
	c.counts = c.counts[:0]
}

// Prepare ensures Preparable column propagation and column order.
func (c *ColVariant) Prepare() error {
	if len(c.Columns) > MaxVariants {
		return errors.Errorf("%d variants exceed maximum of %d", len(c.Columns), MaxVariants)
	}
	for i, v := range c.Columns {
		if i > 0 && c.Columns[i-1].Type() >= v.Type() {
			return errors.Errorf("variants [%d] %s and [%d] %s are not sorted by type",
				i-1, c.Columns[i-1].Type(), i, v.Type(),
			)
		}
		if s, ok := v.(Preparable); ok {
			if err := s.Prepare(); err != nil {
				return errors.Wrapf(err, "[%d]", i)
			}
		}
	}
	return nil
}

// Infer ensures Inferable column propagation.
func (c *ColVariant) Infer(t ColumnType) error {
	elems := t.Elems()
	if len(elems) != len(c.Columns) {
		return errors.Errorf("%q has %d variants, got %d columns", t, len(elems), len(c.Columns))
	}
	for i, v := range c.Columns {
		if s, ok := v.(Inferable); ok {
			if err := s.Infer(elems[i]); err != nil {
				return errors.Wrapf(err, "[%d]", i)
			}
		}
	}
	return nil
}

// decodeCompactDiscriminators decodes granules of compact mode.
//
// Native format does not use compact mode: it is enabled by the
// use_compact_variant_discriminators_serialization MergeTree setting and
// affects data parts only. Implemented to keep the protocol whole and to
// read discriminators from raw data parts.
func (c *ColVariant) decodeCompactDiscriminators(r *Reader, rows int) error {
	for row := 0; row < rows; {
		size, err := r.UVarInt()
		if err != nil {
			return errors.Wrap(err, "granule size")
		}
		if err := checkRows(int(size)); err != nil {
			return errors.Wrap(err, "granule size")
		}
		if row+int(size) > rows {
			return errors.Errorf("granule of %d rows exceeds %d rows left", size, rows-row)
		}
		format, err := r.UInt8()
		if err != nil {
			return errors.Wrap(err, "granule format")
		}
		switch format {
		case variantGranuleCompact:
			d, err := r.UInt8()
			if err != nil {
				return errors.Wrap(err, "granule discriminator")
			}
			for i := 0; i < int(size); i++ {
				c.Discriminators.Append(d)
			}
		case variantGranulePlain:
			if err := c.Discriminators.DecodeColumn(r, int(size)); err != nil {
				return errors.Wrap(err, "granule")
			}
		default:
			return errors.Errorf("unknown granule format %d", format)
		}
		row += int(size)
	}
	return nil
}
