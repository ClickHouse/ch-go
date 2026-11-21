package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColAuto_Infer(t *testing.T) {
	for _, columnType := range []ColumnType{
		ColumnTypeString,
		ColumnTypeArray.Sub(ColumnTypeString),
		ColumnTypeArray.Sub(ColumnTypeLowCardinality.Sub(ColumnTypeString)),
		ColumnTypeDate,
		ColumnTypeDate32,
		ColumnTypeInt8,
		ColumnTypeInt16,
		ColumnTypeArray.Sub(ColumnTypeInt16),
		ColumnTypeNullable.Sub(ColumnTypeInt16),
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
		ColumnTypeDateTime.Sub("Europe/Berlin"),
		ColumnTypeDateTime64.Sub("9"),
		"Map(String, String)",
		"Enum8('hello'=1,'world'=2)",
		"Enum16('hello'=-1,'world'=10)",
		"IntervalSecond",
		"IntervalMinute",
		ColumnType(IntervalHour.String()),
		ColumnTypeNothing,
		"Nullable(Nothing)",
		"Array(Nothing)",
		ColumnTypeUUID,
		ColumnTypeArray.Sub(ColumnTypeUUID),
		ColumnTypeNullable.Sub(ColumnTypeUUID),
		ColumnTypeDecimal,
		ColumnTypeDecimal32,
		ColumnTypeDecimal64,
		ColumnTypeDecimal128,
		ColumnTypeDecimal256,
		"Decimal(2)",
		"Decimal(20, 2)",
		"Decimal32(1)",
		"Decimal64(2)",
		"Decimal128(3)",
		"Decimal256(4)",
		"Array(Nullable(Int8))",
		"Nullable(DateTime64(3))",
		"AggregateFunction(quantileDD(0.01, 0.5), UInt64)",
		"AggregateFunction(quantilesDD(0.01, 0.5, 0.99), Float64)",
	} {
		r := AutoResult("foo")
		require.NoError(t, r.Data.(Inferable).Infer(columnType))
		require.Equal(t, columnType, r.Data.Type())
		r.Data.Reset()
		require.Equal(t, 0, r.Data.Rows())
	}
}

func TestColAuto_Infer_AggregateFunction(t *testing.T) {
	for _, columnType := range []ColumnType{
		// The state has a flag byte before the sketch.
		"AggregateFunction(quantilesDD(0.01, 0.5), Nullable(Float64))",
		// The states of other functions have other layouts.
		"AggregateFunction(sum, UInt64)",
		"AggregateFunction(quantilesTDigest(0.5), Float64)",
		"AggregateFunction(1, sumMap, Array(UInt8), Array(UInt8))",
	} {
		r := AutoResult("foo")
		require.EqualError(t, r.Data.(Inferable).Infer(columnType),
			`automatic column inference not supported for "`+string(columnType)+`"`,
		)
	}
}
