package proto

import (
	"strings"
)

type ColumnType string

func (c ColumnType) IsArray() bool {
	return strings.HasPrefix(string(c), string(ColumnTypeArray))
}

const (
	ColumnTypeInt8    ColumnType = "Int8"
	ColumnTypeInt16   ColumnType = "Int16"
	ColumnTypeInt32   ColumnType = "Int32"
	ColumnTypeInt64   ColumnType = "Int64"
	ColumnTypeInt128  ColumnType = "Int128"
	ColumnTypeUInt8   ColumnType = "UInt8"
	ColumnTypeUInt16  ColumnType = "UInt16"
	ColumnTypeUInt32  ColumnType = "UInt32"
	ColumnTypeUInt64  ColumnType = "UInt64"
	ColumnTypeUInt128 ColumnType = "UInt128"
	ColumnTypeFloat32 ColumnType = "Float32"
	ColumnTypeFloat64 ColumnType = "Float64"
	ColumnTypeString  ColumnType = "String"
	ColumnTypeArray   ColumnType = "Array"
)
