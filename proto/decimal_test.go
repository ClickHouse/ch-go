package proto

func Decimal128FromInt(v int) Decimal128 {
	return Decimal128(Int128FromInt(v))
}
