package proto

type ColumnType string

const (
	ColumnTypeInt8    ColumnType = "Int8"
	ColumnTypeInt16   ColumnType = "Int16"
	ColumnTypeInt32   ColumnType = "Int32"
	ColumnTypeInt64   ColumnType = "Int64"
	ColumnTypeUInt8   ColumnType = "UInt8"
	ColumnTypeUInt16  ColumnType = "UInt16"
	ColumnTypeUInt32  ColumnType = "UInt32"
	ColumnTypeUInt64  ColumnType = "UInt64"
	ColumnTypeFloat32 ColumnType = "Float32"
	ColumnTypeFloat64 ColumnType = "Float64"
)
