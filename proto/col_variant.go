package proto

import (
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
// Row has discriminator, an index in Elements, or VariantNullDiscriminator for
// NULL. Values are stored sparsely, Elements[i] holds only rows of variant i.
//
// ClickHouse sorts variant types by name, so Elements must be sorted by Type()
// and unique to match server discriminators. Prepare returns error otherwise.
type ColVariant struct {
	// Discriminators of rows.
	Discriminators ColUInt8

	// Elements of variants, must be sorted by Type() and unique.
	Elements []Column

	offsets []int  // index of row i in Elements[Discriminators[i]]
	counts  []int  // rows per element
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

// NewVariant constructs Variant(T1, T2, ...).
//
// Elements must be sorted by Type() and unique, see ColVariant.
func NewVariant(elements ...Column) *ColVariant {
	return &ColVariant{
		Elements: elements,
	}
}

func (c ColVariant) Type() ColumnType {
	types := make([]ColumnType, len(c.Elements))
	for i, v := range c.Elements {
		types[i] = v.Type()
	}
	return ColumnTypeVariant.Sub(types...)
}

func (c ColVariant) Rows() int {
	return c.Discriminators.Rows()
}

// ElementDiscriminator returns discriminator of element with type t,
// an index in Elements.
func (c ColVariant) ElementDiscriminator(t ColumnType) (uint8, bool) {
	for i, v := range c.Elements {
		if v.Type() == t {
			return uint8(i), true
		}
	}
	return 0, false
}

// Row returns discriminator of row i and offset of its value in Elements[d].
//
// Discriminator is VariantNullDiscriminator for NULL.
//
//	switch d, off := v.Row(i); d {
//	case ints.Discriminator():
//		ints.Row(off)
//	}
func (c ColVariant) Row(i int) (d uint8, offset int) {
	return c.Discriminators[i], c.offsets[i]
}

// RowDiscriminator returns discriminator of row i.
func (c ColVariant) RowDiscriminator(i int) uint8 {
	return c.Discriminators[i]
}

// RowIsNull reports whether row i is NULL.
func (c ColVariant) RowIsNull(i int) bool {
	return c.Discriminators[i] == VariantNullDiscriminator
}

// RowOffset returns index of row i in Elements[RowDiscriminator(i)].
func (c ColVariant) RowOffset(i int) int {
	return c.offsets[i]
}

// AppendRowDiscriminator appends row of variant d.
//
// Value itself must be appended to Elements[d] by caller.
//
//	v.AppendRowDiscriminator(0)
//	ints.Append(42)
func (c *ColVariant) AppendRowDiscriminator(d uint8) {
	c.ensureCounts()
	c.offsets = append(c.offsets, c.counts[d])
	c.counts[d]++
	c.Discriminators.Append(d)
}

// AppendNullRow appends NULL row.
func (c *ColVariant) AppendNullRow() {
	c.offsets = append(c.offsets, 0)
	c.Discriminators.Append(VariantNullDiscriminator)
}

func (c *ColVariant) ensureCounts() {
	if n := len(c.Elements) - len(c.counts); n > 0 {
		c.counts = append(c.counts, make([]int, n)...)
	}
}

func (c ColVariant) EncodeState(b *Buffer) {
	b.PutUInt64(VariantDiscriminatorsBasic)
	for _, v := range c.Elements {
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
	for i, v := range c.Elements {
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
	for _, v := range c.Elements {
		v.EncodeColumn(b)
	}
}

func (c ColVariant) WriteColumn(w *Writer) {
	c.Discriminators.WriteColumn(w)
	for _, v := range c.Elements {
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
	for i, v := range c.Elements {
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
		if int(d) >= len(c.Elements) {
			return errors.Errorf("discriminator %d of %d variants", d, len(c.Elements))
		}
		c.offsets = append(c.offsets, c.counts[d])
		c.counts[d]++
	}
	return nil
}

func (c *ColVariant) Reset() {
	c.Discriminators.Reset()
	for _, v := range c.Elements {
		v.Reset()
	}
	c.offsets = c.offsets[:0]
	c.counts = c.counts[:0]
}

// Prepare ensures Preparable column propagation and returns error if Elements
// are not sorted by Type() or not unique.
func (c *ColVariant) Prepare() error {
	if len(c.Elements) > MaxVariants {
		return errors.Errorf("%d variants exceed maximum of %d", len(c.Elements), MaxVariants)
	}
	for i, v := range c.Elements {
		if i > 0 && c.Elements[i-1].Type() >= v.Type() {
			return errors.Errorf("variants [%d] %s and [%d] %s must be sorted by type",
				i-1, c.Elements[i-1].Type(), i, v.Type(),
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
	if len(elems) != len(c.Elements) {
		return errors.Errorf("%q has %d variants, got %d", t, len(elems), len(c.Elements))
	}
	for i, v := range c.Elements {
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
