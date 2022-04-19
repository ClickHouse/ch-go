package proto

import (
	"math"

	"github.com/go-faster/errors"
)

// Compile-time assertions for ColLowCardinalityOf.
var (
	_ ColInput  = ColLowCardinalityOf[string]{}
	_ ColResult = (*ColLowCardinalityOf[string])(nil)
	_ Column    = (*ColLowCardinalityOf[string])(nil)
)

// DecodeState implements StateDecoder, ensuring state for index column.
func (c *ColLowCardinalityOf[T]) DecodeState(r *Reader) error {
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
func (c ColLowCardinalityOf[T]) EncodeState(b *Buffer) {
	// Writing key serialization version.
	b.PutInt64(int64(sharedDictionariesWithAdditionalKeys))
	if s, ok := c.index.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

// ColLowCardinalityOf is generic LowCardinality(T) column.
type ColLowCardinalityOf[T comparable] struct {
	Values []T

	index ColumnOf[T]
	key   CardinalityKey

	// Keeping all key column variants as fields to reuse
	// memory more efficiently.

	// Values[T], kv and keys columns adds memory overhead, but simplifies
	// implementation.
	// TODO(ernado): revisit tradeoffs

	keys8  ColUInt8
	keys16 ColUInt16
	keys32 ColUInt32
	keys64 ColUInt64

	kv   map[T]int
	keys []int
}

func (c *ColLowCardinalityOf[T]) DecodeColumn(r *Reader, rows int) error {
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
	c.key = key

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
	switch c.key {
	case KeyUInt8:
		if err := c.keys8.DecodeColumn(r, rows); err != nil {
			return errors.Wrap(err, "keys")
		}
		c.keys = fillValues(c.keys, c.keys8)
	case KeyUInt16:
		if err := c.keys16.DecodeColumn(r, rows); err != nil {
			return errors.Wrap(err, "keys")
		}
		c.keys = fillValues(c.keys, c.keys16)
	case KeyUInt32:
		if err := c.keys32.DecodeColumn(r, rows); err != nil {
			return errors.Wrap(err, "keys")
		}
		c.keys = fillValues(c.keys, c.keys32)
	case KeyUInt64:
		if err := c.keys64.DecodeColumn(r, rows); err != nil {
			return errors.Wrap(err, "keys")
		}
		c.keys = fillValues(c.keys, c.keys64)
	default:
		return errors.Errorf("invalid key format %s", c.key)
	}

	c.Values = c.Values[:0]
	for _, idx := range c.keys {
		c.Values = append(c.Values, c.index.Row(idx))
	}

	return nil
}

func (c ColLowCardinalityOf[T]) Type() ColumnType {
	return ColumnTypeLowCardinality.Sub(c.index.Type())
}

func (c ColLowCardinalityOf[T]) EncodeColumn(b *Buffer) {
	if c.Rows() == 0 {
		// Skipping encoding entirely.
		return
	}

	// Meta encodes whether reader should update
	// low cardinality metadata and keys column type.
	meta := cardinalityUpdateAll | int64(c.key)
	b.PutInt64(meta)

	// Writing index (dictionary).
	b.PutInt64(int64(c.index.Rows()))
	c.index.EncodeColumn(b)

	b.PutInt64(int64(c.Rows()))
	switch c.key {
	case KeyUInt8:
		c.keys8.EncodeColumn(b)
	case KeyUInt16:
		c.keys16.EncodeColumn(b)
	case KeyUInt32:
		c.keys32.EncodeColumn(b)
	case KeyUInt64:
		c.keys64.EncodeColumn(b)
	}
}

func (c *ColLowCardinalityOf[T]) Reset() {
	for k := range c.kv {
		delete(c.kv, k)
	}
	c.keys = c.keys[:0]

	c.keys8 = c.keys8[:0]
	c.keys16 = c.keys16[:0]
	c.keys32 = c.keys32[:0]
	c.keys64 = c.keys64[:0]
	c.Values = c.Values[:0]

	c.index.Reset()
}

type cardinalityKeyValue interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func fillKeys[K cardinalityKeyValue](values []int, keys []K) []K {
	for _, v := range values {
		keys = append(keys, K(v))
	}
	return keys
}

func fillValues[K cardinalityKeyValue](values []int, keys []K) []int {
	for _, v := range keys {
		values = append(values, int(v))
	}
	return values
}

// Append value to column.
func (c *ColLowCardinalityOf[T]) Append(v T) {
	c.Values = append(c.Values, v)
}

// AppendArr appends slice to column.
func (c *ColLowCardinalityOf[T]) AppendArr(v []T) {
	c.Values = append(c.Values, v...)
}

// Row returns i-th row.
func (c ColLowCardinalityOf[T]) Row(i int) T {
	return c.Values[i]
}

// Rows returns rows count.
func (c ColLowCardinalityOf[T]) Rows() int {
	return len(c.Values)
}

// Prepare column for ingestion.
func (c *ColLowCardinalityOf[T]) Prepare() error {
	// Select minimum possible size for key.
	if n := len(c.Values); n < math.MaxUint8 {
		c.key = KeyUInt8
	} else if n < math.MaxUint16 {
		c.key = KeyUInt16
	} else if uint32(n) < math.MaxUint32 {
		c.key = KeyUInt32
	} else {
		c.key = KeyUInt64
	}

	// Allocate keys slice.
	c.keys = append(c.keys[:0], make([]int, len(c.Values))...)
	if c.kv == nil {
		c.kv = map[T]int{}
	}

	// Fill keys with value indexes.
	var last int
	for i, v := range c.Values {
		idx, ok := c.kv[v]
		if !ok {
			c.index.Append(v)
			c.kv[v] = last
			idx = last
			last++
		}
		c.keys[i] = idx
	}

	// Fill key column with key indexes.
	switch c.key {
	case KeyUInt8:
		c.keys8 = fillKeys(c.keys, c.keys8)
	case KeyUInt16:
		c.keys16 = fillKeys(c.keys, c.keys16)
	case KeyUInt32:
		c.keys32 = fillKeys(c.keys, c.keys32)
	case KeyUInt64:
		c.keys64 = fillKeys(c.keys, c.keys64)
	}

	return nil
}

// LowCardinalityOf creates new LowCardinality column from another column for T.
func LowCardinalityOf[T comparable](c ColumnOf[T]) *ColLowCardinalityOf[T] {
	return &ColLowCardinalityOf[T]{
		index: c,
	}
}
