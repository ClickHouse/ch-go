package proto

import "github.com/go-faster/errors"

//go:generate go run github.com/dmarkham/enumer -type CardinalityKey -trimprefix Key -output col_low_cardinality_enum.go

// CardinalityKey is integer type of ColLowCardinality.Keys column.
type CardinalityKey byte

// Possible integer types for ColLowCardinality.Keys.
const (
	KeyUInt8  CardinalityKey = 0
	KeyUInt16 CardinalityKey = 1
	KeyUInt32 CardinalityKey = 2
	KeyUInt64 CardinalityKey = 3
)

// ColLowCardinality contains index and keys columns.
//
// Index column contains unique values, Keys column contains
// sequence of indexes in Index colum that represent actual
// values.
//
// For example, ["Eko", "Eko", "Amadela", "Amadela", "Amadela", "Amadela"] can
// be encoded as:
//	Index: ["Eko", "Amadela"] (String)
//	Keys:  [0, 0, 1, 1, 1, 1] (UInt8)
//
// The CardinalityKey is chosen depending on Index size, i.e. maximum value
// of chosen type should be able to represent any index of Index element.
type ColLowCardinality struct {
	Index Column
	Key   CardinalityKey

	// Keeping all key column variants as fields to reuse
	// memory more efficiently.

	Keys8  ColUInt8
	Keys16 ColUInt16
	Keys32 ColUInt32
	Keys64 ColUInt64
}

const (
	cardinalityHasAdditionalKeys = 1 << 9
	cardinalityUpdateIndex       = 1 << 10
	cardinalityUpdateAll         = cardinalityUpdateIndex | cardinalityHasAdditionalKeys
)

func (c *ColLowCardinality) AppendKey(i int) {
	switch c.Key {
	case KeyUInt8:
		c.Keys8 = append(c.Keys8, uint8(i))
	case KeyUInt16:
		c.Keys16 = append(c.Keys16, uint16(i))
	case KeyUInt32:
		c.Keys32 = append(c.Keys32, uint32(i))
	case KeyUInt64:
		c.Keys64 = append(c.Keys64, uint64(i))
	default:
		panic("invalid key type")
	}
}

func (c *ColLowCardinality) Keys() Column {
	switch c.Key {
	case KeyUInt8:
		return &c.Keys8
	case KeyUInt16:
		return &c.Keys16
	case KeyUInt32:
		return &c.Keys32
	case KeyUInt64:
		return &c.Keys64
	default:
		panic("invalid key type")
	}
}

func (c ColLowCardinality) Type() ColumnType {
	return ColumnTypeLowCardinality.Sub(c.Index.Type())
}

func (c ColLowCardinality) Rows() int {
	return c.Keys().Rows()
}

func (c *ColLowCardinality) DecodeColumn(r *Reader, rows int) error {
	meta, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "meta")
	}
	key := CardinalityKey(meta & 0xf)
	if !key.IsACardinalityKey() {
		return errors.Errorf("invalid low cardinality keys type %d", key)
	}
	c.Key = key
	indexRows, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "index size")
	}
	if indexRows < 0 || indexRows > maxRowsInBlock {
		return errors.Errorf("index size invalid: %d < %d < %d",
			0, indexRows, maxRowsInBlock,
		)
	}
	if err := c.Index.DecodeColumn(r, int(indexRows)); err != nil {
		return errors.Wrap(err, "index column")
	}
	keysRow, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "keys size")
	}
	if err := c.Keys().DecodeColumn(r, int(keysRow)); err != nil {
		return errors.Wrap(err, "keys column")
	}

	return nil
}

func (c *ColLowCardinality) Reset() {
	c.Index.Reset()
	c.Keys8.Reset()
	c.Keys16.Reset()
	c.Keys32.Reset()
	c.Keys64.Reset()
}

func (c ColLowCardinality) EncodeColumn(b *Buffer) {
	// Meta encodes whether reader should update
	// low cardinality metadata and keys column type.
	meta := cardinalityUpdateAll | int(c.Key)
	b.PutInt64(int64(meta))
	b.PutInt64(int64(c.Index.Rows()))
	c.Index.EncodeColumn(b)

	k := c.Keys()
	b.PutInt64(int64(k.Rows()))
	k.EncodeColumn(b)
}
