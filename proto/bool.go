package proto

const (
	boolTrue  uint8 = 1
	boolFalse uint8 = 0
)

// NewRawBool makes Bool column from ColUInt8 that is faster than ColBool.
//
// Column values should be only zeroes or ones (1 or 0).
// Can be x15-35 faster than ColBool, but ColBool is already pretty fast and can
// handle 2 GB/s per core.
func NewRawBool(raw *ColUInt8) Column {
	return Alias(raw, ColumnTypeBool)
}
