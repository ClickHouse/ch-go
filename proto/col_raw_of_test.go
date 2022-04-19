//go:build (amd64 || arm64) && !purego

package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColRawOf(t *testing.T) {
	testColumn[[16]byte](t, "byte_arr_16", func() ColumnOf[[16]byte] {
		return &ColRawOf[[16]byte]{}
	}, [16]byte{1: 1}, [16]byte{10: 14})

	require.Equal(t, ColumnType("FixedString(32)"), (ColRawOf[[32]byte]{}).Type())
	require.Equal(t, ColumnType("FixedString(2)"), (ColRawOf[[2]byte]{}).Type())
}
