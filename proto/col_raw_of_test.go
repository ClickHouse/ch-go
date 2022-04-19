package proto

import "testing"

func TestColRawOf(t *testing.T) {
	testColumn[[16]byte](t, "byte_arr_16", func() ColumnOf[[16]byte] {
		return &ColRawOf[[16]byte]{}
	}, [16]byte{1: 1}, [16]byte{10: 14})
}
