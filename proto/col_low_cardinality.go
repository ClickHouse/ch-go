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
// Index (i.e. dictionary) column contains unique values, Keys column contains
// sequence of indexes in Index column that represent actual values.
//
// For example, ["Eko", "Eko", "Amadela", "Amadela", "Amadela", "Amadela"] can
// be encoded as:
//	Index: ["Eko", "Amadela"] (String)
//	Keys:  [0, 0, 1, 1, 1, 1] (UInt8)
//
// The CardinalityKey is chosen depending on Index size, i.e. maximum value
// of chosen type should be able to represent any index of Index element.
type ColLowCardinality struct {
	Index Column // dictionary
	Key   CardinalityKey

	// Keeping all key column variants as fields to reuse
	// memory more efficiently.

	Keys8  ColUInt8
	Keys16 ColUInt16
	Keys32 ColUInt32
	Keys64 ColUInt64
}

func (c *ColLowCardinality) DecodeState(r *Reader) error {
	keySerialization, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "version")
	}
	if keySerialization != int64(sharedDictionariesWithAdditionalKeys) {
		return errors.Errorf("got version %d, expected %d",
			keySerialization, sharedDictionariesWithAdditionalKeys,
		)
	}
	if s, ok := c.Index.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "state")
		}
	}
	return nil
}

func (c ColLowCardinality) EncodeState(b *Buffer) {
	// Writing key serialization version.
	b.PutInt64(int64(sharedDictionariesWithAdditionalKeys))
	if s, ok := c.Index.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

// Constants for low cardinality metadata value that is represented as int64
// consisted of bitflags and key type.
//
// https://github.com/ClickHouse/clickhouse-cpp/blob/b10d71eed0532405dfb4dd03aabce869ba68f581/clickhouse/columns/lowcardinality.cpp
//
// NB: shared dictionaries and on-the-fly dictionary update is not supported,
// because it is not currently used in client protocol.
const (
	cardinalityKeyMask = 0b0000_1111_1111 // last byte

	// Need to read dictionary if it wasn't.
	cardinalityNeedGlobalDictionaryBit = 1 << 8
	// Need to read additional keys.
	// Additional keys are stored before indexes as value N and N keys
	// after them.
	cardinalityHasAdditionalKeysBit = 1 << 9
	// Need to update dictionary. It means that previous granule has different dictionary.
	cardinalityNeedUpdateDictionary = 1 << 10

	// cardinalityUpdateAll sets both flags (update index, has additional keys)
	cardinalityUpdateAll = cardinalityHasAdditionalKeysBit | cardinalityNeedUpdateDictionary
)

type keySerializationVersion byte

// sharedDictionariesWithAdditionalKeys is default key serialization.
const sharedDictionariesWithAdditionalKeys keySerializationVersion = 1

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

func (c *ColArr) AppendLowCardinality(data []int) {
	d := c.Data.(*ColLowCardinality)
	for _, v := range data {
		d.AppendKey(v)
	}
	c.Offsets = append(c.Offsets, uint64(d.Keys().Rows()))
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
	if rows == 0 {
		// Skipping entirely of no rows.
		return nil
	}
	meta, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "meta")
	}
	if (meta & cardinalityNeedGlobalDictionaryBit) == 1 {
		return errors.New("global dictionary is not supported")
	}
	if (meta & cardinalityHasAdditionalKeysBit) == 0 {
		return errors.New("additional keys bit is missing")
	}

	key := CardinalityKey(meta & cardinalityKeyMask)
	if !key.IsACardinalityKey() {
		return errors.Errorf("invalid low cardinality keys type %d", key)
	}
	c.Key = key

	indexRows, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "index size")
	}
	if err := checkRows(int(indexRows)); err != nil {
		return errors.Wrap(err, "index size")
	}
	if err := c.Index.DecodeColumn(r, int(indexRows)); err != nil {
		return errors.Wrap(err, "index column")
	}

	keyRows, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "keys size")
	}
	if err := checkRows(int(keyRows)); err != nil {
		return errors.Wrap(err, "index size")
	}
	if err := c.Keys().DecodeColumn(r, int(keyRows)); err != nil {
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
	if c.Rows() == 0 {
		// Skipping encoding entirely.
		return
	}

	// Meta encodes whether reader should update
	// low cardinality metadata and keys column type.
	meta := cardinalityUpdateAll | int64(c.Key)
	b.PutInt64(meta)

	// Writing index (dictionary).
	b.PutInt64(int64(c.Index.Rows()))
	c.Index.EncodeColumn(b)

	// Sequence of values as indexes in dictionary.
	k := c.Keys()
	b.PutInt64(int64(k.Rows()))
	k.EncodeColumn(b)
}
