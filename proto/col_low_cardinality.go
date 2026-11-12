package proto

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/go-faster/errors"
)

// Compile-time assertions for ColLowCardinality.
var (
	_ ColInput  = (*ColLowCardinality[string])(nil)
	_ ColResult = (*ColLowCardinality[string])(nil)
	_ Column    = (*ColLowCardinality[string])(nil)
)

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

// ColLowCardinality is generic LowCardinality(T) column.
//
// ColLowCardinality contains index and keys columns.
//
// Index (i.e. dictionary) column contains unique values, Keys column contains
// sequence of indexes in Index column that represent actual values.
//
// For example, ["Eko", "Eko", "Amadela", "Amadela", "Amadela", "Amadela"] can
// be encoded as:
//
//	Index: ["Eko", "Amadela"] (String)
//	Keys:  [0, 0, 1, 1, 1, 1] (UInt8)
//
// The CardinalityKey is chosen depending on Index size, i.e. maximum value
// of chosen type should be able to represent any index of Index element.
//
// The dictionary is built while appending, so a row costs a key instead of a
// copy of its value, and encoding doesn't hash every row again.
type ColLowCardinality[T comparable] struct {
	index ColumnOf[T]
	// Position in index of every row.
	keys []int
	// Position in index of every value, to deduplicate on append. Nil when it
	// has to be rebuilt from index, e.g. after decoding.
	kv map[T]int
}

// DecodeState implements StateDecoder, ensuring state for index column.
func (c *ColLowCardinality[T]) DecodeState(r *Reader) error {
	keySerialization, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "version")
	}
	if keySerialization != int64(sharedDictionariesWithAdditionalKeys) {
		return errors.Errorf("got version %d, expected %d",
			keySerialization, sharedDictionariesWithAdditionalKeys,
		)
	}
	if s, ok := c.index.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "index state")
		}
	}
	return nil
}

// EncodeState implements StateEncoder, ensuring state for index column.
func (c ColLowCardinality[T]) EncodeState(b *Buffer) {
	// Writing key serialization version.
	b.PutInt64(int64(sharedDictionariesWithAdditionalKeys))
	if s, ok := c.index.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

func (c *ColLowCardinality[T]) DecodeColumn(r *Reader, rows int) error {
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

	indexRows, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "index size")
	}
	if err := checkRows(int(indexRows)); err != nil {
		return errors.Wrap(err, "index size")
	}
	if err := c.index.DecodeColumn(r, int(indexRows)); err != nil {
		return errors.Wrap(err, "index column")
	}

	keyRows, err := r.Int64()
	if err != nil {
		return errors.Wrap(err, "keys size")
	}
	if err := checkRows(int(keyRows)); err != nil {
		return errors.Wrap(err, "index size")
	}
	// Each case reads its keys from the reader buffer, which is nil on error.
	var raw []byte
	c.keys = c.keys[:0]
	switch key {
	case KeyUInt8:
		raw, err = r.ReadRaw(rows)
		for _, k := range raw {
			c.keys = append(c.keys, int(k))
		}
	case KeyUInt16:
		raw, err = r.ReadRaw(2 * rows)
		for i := 0; i < len(raw); i += 2 {
			c.keys = append(c.keys, int(binary.LittleEndian.Uint16(raw[i:])))
		}
	case KeyUInt32:
		raw, err = r.ReadRaw(4 * rows)
		for i := 0; i < len(raw); i += 4 {
			c.keys = append(c.keys, int(binary.LittleEndian.Uint32(raw[i:])))
		}
	case KeyUInt64:
		raw, err = r.ReadRaw(8 * rows)
		for i := 0; i < len(raw); i += 8 {
			c.keys = append(c.keys, int(binary.LittleEndian.Uint64(raw[i:])))
		}
	}
	if err != nil {
		return errors.Wrap(err, "keys")
	}

	for _, idx := range c.keys {
		if int64(idx) >= indexRows || idx < 0 {
			return errors.Errorf("key index out of range [%d] with length %d", idx, indexRows)
		}
	}
	// The decoded index replaced the one kv refers to.
	c.kv = nil

	return nil
}

func (c ColLowCardinality[T]) Type() ColumnType {
	return ColumnTypeLowCardinality.Sub(c.index.Type())
}

func (c *ColLowCardinality[T]) EncodeColumn(b *Buffer) {
	if c.Rows() == 0 {
		// Skipping encoding entirely.
		return
	}

	key := c.keyType()
	// Meta encodes whether reader should update
	// low cardinality metadata and keys column type.
	b.PutInt64(cardinalityUpdateAll | int64(key))

	// Writing index (dictionary).
	b.PutInt64(int64(c.index.Rows()))
	c.index.EncodeColumn(b)

	c.encodeKeys(b, key)
}

func (c *ColLowCardinality[T]) WriteColumn(w *Writer) {
	if c.Rows() == 0 {
		// Skipping encoding entirely.
		return
	}

	key := c.keyType()
	w.ChainBuffer(func(b *Buffer) {
		// Meta encodes whether reader should update
		// low cardinality metadata and keys column type.
		b.PutInt64(cardinalityUpdateAll | int64(key))

		// Writing index (dictionary).
		b.PutInt64(int64(c.index.Rows()))
	})
	c.index.WriteColumn(w)

	w.ChainBuffer(func(b *Buffer) {
		c.encodeKeys(b, key)
	})
}

// keyType is the narrowest key type for the index size.
func (c *ColLowCardinality[T]) keyType() CardinalityKey {
	// n is the `count` of distinct values, not the values itself; stored keys range over [0, n-1].
	// So n == MaxUint8+1 (256) still fits in uint8 (max key 255), and likewise at the wider boundaries.
	switch n := c.index.Rows(); {
	case n <= math.MaxUint8+1:
		return KeyUInt8
	case n <= math.MaxUint16+1:
		return KeyUInt16
	case uint64(n) <= math.MaxUint32+1:
		return KeyUInt32
	default:
		return KeyUInt64
	}
}

// encodeKeys writes the number of keys and the keys as key type.
func (c *ColLowCardinality[T]) encodeKeys(b *Buffer, key CardinalityKey) {
	b.PutInt64(int64(len(c.keys)))
	// Appending to a local slice grown once keeps the loops tight.
	buf := b.Buf
	switch key {
	case KeyUInt8:
		buf = slices.Grow(buf, len(c.keys))
		for _, k := range c.keys {
			buf = append(buf, uint8(k))
		}
	case KeyUInt16:
		buf = slices.Grow(buf, 2*len(c.keys))
		for _, k := range c.keys {
			buf = binary.LittleEndian.AppendUint16(buf, uint16(k))
		}
	case KeyUInt32:
		buf = slices.Grow(buf, 4*len(c.keys))
		for _, k := range c.keys {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(k))
		}
	case KeyUInt64:
		buf = slices.Grow(buf, 8*len(c.keys))
		for _, k := range c.keys {
			buf = binary.LittleEndian.AppendUint64(buf, uint64(k))
		}
	}
	b.Buf = buf
}

func (c *ColLowCardinality[T]) Reset() {
	clear(c.kv)
	c.keys = c.keys[:0]
	c.index.Reset()
}

// Append value to column.
func (c *ColLowCardinality[T]) Append(v T) {
	if c.kv == nil {
		c.kv = make(map[T]int, c.index.Rows())
		for i := range c.index.Rows() {
			c.kv[c.index.Row(i)] = i
		}
	}
	idx, ok := c.kv[v]
	if !ok {
		idx = c.index.Rows()
		c.index.Append(v)
		c.kv[v] = idx
	}
	c.keys = append(c.keys, idx)
}

// AppendArr appends slice to column.
func (c *ColLowCardinality[T]) AppendArr(v []T) {
	for _, e := range v {
		c.Append(e)
	}
}

// Row returns i-th row.
func (c ColLowCardinality[T]) Row(i int) T {
	return c.index.Row(c.keys[i])
}

// Rows returns rows count.
func (c ColLowCardinality[T]) Rows() int {
	return len(c.keys)
}

// Prepare is a no-op kept for compatibility: the dictionary is built while
// appending, and keys are narrowed while encoding.
func (c *ColLowCardinality[T]) Prepare() error { return nil }

// Array is helper that creates Array(ColLowCardinality(T)).
func (c *ColLowCardinality[T]) Array() *ColArr[T] {
	return &ColArr[T]{
		Data: c,
	}
}

// NewLowCardinality creates new LowCardinality column from another column for T.
func NewLowCardinality[T comparable](c ColumnOf[T]) *ColLowCardinality[T] {
	return &ColLowCardinality[T]{
		index: c,
	}
}
