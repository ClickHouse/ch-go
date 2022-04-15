package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColAuto_Infer(t *testing.T) {
	r := AutoResult("foo")
	for _, columnType := range []ColumnType{
		ColumnTypeString,
		ColumnTypeDate,
		ColumnTypeDate32,
		ColumnTypeInt8,
		ColumnTypeInt16,
		ColumnTypeInt32,
		ColumnTypeInt64,
		ColumnTypeInt128,
		ColumnTypeInt256,
		ColumnTypeUInt8,
		ColumnTypeUInt16,
		ColumnTypeUInt32,
		ColumnTypeUInt64,
		ColumnTypeUInt128,
		ColumnTypeUInt256,
		ColumnTypeFloat32,
		ColumnTypeFloat64,
		ColumnTypeIPv4,
		ColumnTypeIPv6,
		ColumnTypeLowCardinality.Sub(ColumnTypeString),
	} {
		auto := r.Data.(InferColumn)
		require.NoError(t, auto.Infer(columnType))
		require.Equal(t, auto.Type(), columnType)
		auto.Reset()
		require.Equal(t, 0, auto.Rows())
	}
}
