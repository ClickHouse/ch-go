package proto

import (
	"fmt"
	"strings"
)

// ColumnType is type of column element.
type ColumnType string

func (c ColumnType) String() string {
	return string(c)
}

func (c ColumnType) Elem() ColumnType {
	if c == "" {
		return ""
	}
	var (
		v     = string(c)
		start = strings.Index(v, "(")
		end   = strings.LastIndex(v, ")")
	)
	if start <= 0 || end <= 0 || end < start {
		// No element.
		return ""
	}
	return c[start+1 : end]
}

// IsArray reports whether ColumnType is composite.
func (c ColumnType) IsArray() bool {
	return strings.HasPrefix(string(c), string(ColumnTypeArray))
}

// Array returns Array(ColumnType).
func (c ColumnType) Array() ColumnType {
	return ColumnType(fmt.Sprintf("%s(%s)", ColumnTypeArray, c))
}

// Common colum type names. Does not represent full set of supported types,
// because ColumnTypeArray is composable; actual type is composite.
//
// For example: Array(Int8) or even Array(Array(String)).
const (
	ColumnTypeNone    ColumnType = ""
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
	ColumnTypeIPv4    ColumnType = "IPv4"
	ColumnTypeIPv6    ColumnType = "IPv6"
)
