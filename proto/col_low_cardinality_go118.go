package proto

import "math"

type ColLowCardinalityOf[T comparable] struct {
	Values []T

	index ColumnOf[T]
	key   CardinalityKey

	// Keeping all key column variants as fields to reuse
	// memory more efficiently.

	keys8  ColUInt8
	keys16 ColUInt16
	keys32 ColUInt32
	keys64 ColUInt64

	kv   map[T]int
	keys []int
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

// Prepare column for ingestion.
func (c *ColLowCardinalityOf[T]) Prepare() error {
	n := len(c.Values)
	if n < math.MaxUint8 {
		c.key = KeyUInt8
	} else if n < math.MaxUint16 {
		c.key = KeyUInt16
	} else if uint32(n) < math.MaxUint32 {
		c.key = KeyUInt32
	} else {
		c.key = KeyUInt64
	}

	c.keys = append(c.keys[:0], make([]int, len(c.Values))...)
	if c.kv == nil {
		c.kv = map[T]int{}
	}

	var last int
	for i, v := range c.Values {
		idx, ok := c.kv[v]
		if !ok {
			c.kv[v] = last
			last++
		}
		c.keys[i] = idx
	}

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

func LowCardinalityOf[T comparable](c ColumnOf[T]) *ColLowCardinalityOf[T] {
	return &ColLowCardinalityOf[T]{
		index: c,
	}
}

// LowCardinality returns LowCardinality(String).
func (c *ColStr) LowCardinality() *ColLowCardinalityOf[string] {
	return &ColLowCardinalityOf[string]{
		index: c,
	}
}
