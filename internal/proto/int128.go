package proto

import "math"

// Int128 represents Int128 type.
type Int128 struct {
	Low  uint64 // first 64 bits
	High uint64 // last 64 bits
}

// Int128FromInt creates new Int128 from int.
func Int128FromInt(v int) Int128 {
	var hi uint64
	if v < 0 {
		hi = math.MaxUint64
	}
	return Int128{
		High: hi,
		Low:  uint64(v),
	}
}

// UInt128 represents UInt128 type.
type UInt128 struct {
	Low  uint64 // first 64 bits
	High uint64 // last 64 bits
}

// UInt128FromInt creates new UInt128 from int.
func UInt128FromInt(v int) UInt128 {
	return UInt128(Int128FromInt(v))
}

func binUInt128(b []byte) UInt128 {
	_ = b[:128/8] // bounds check hint to compiler; see golang.org/issue/14808
	return UInt128{
		Low:  bin.Uint64(b[0 : 64/8]),
		High: bin.Uint64(b[64/8 : 128/8]),
	}
}

func binPutUInt128(b []byte, v UInt128) {
	_ = b[:128/8] // bounds check hint to compiler; see golang.org/issue/14808
	bin.PutUint64(b[0:64/8], v.Low)
	bin.PutUint64(b[64/8:128/8], v.High)
}
