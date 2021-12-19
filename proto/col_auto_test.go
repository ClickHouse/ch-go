package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColAuto_Infer(t *testing.T) {
	r := AutoResult("foo")
	for _, columnType := range []ColumnType{
		ColumnTypeString,
		ColumnTypeDateTime,
		ColumnTypeInt8,
		ColumnTypeUInt8,
		ColumnTypeUInt32,
		ColumnTypeUInt64,
	} {
		auto := r.Data.(InferColumn)
		require.NoError(t, auto.Infer(columnType))
		require.Equal(t, auto.Type(), columnType)
		auto.Reset()
		require.Equal(t, 0, auto.Rows())
	}
}
