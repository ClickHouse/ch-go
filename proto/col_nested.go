package proto

import (
	"reflect"

	"github.com/go-faster/errors"
)

// ColNested represents Nested(name1 Type1, name2 Type2, ...).
//
// In ClickHouse, Nested types are stored as multiple parallel Array columns.
// For example, Nested(a UInt32, b String) is stored as:
//   - col.a Array(UInt32)
//   - col.b Array(String)
//
// All arrays in a row must have the same length (this is enforced by ClickHouse).
//
// Since Nested is not a wire-level type, ColNested provides helper methods
// to work with the flattened array columns:
//   - InputColumns(prefix) returns []InputColumn for INSERT operations
//   - ResultColumns(prefix) returns []ResultColumn for SELECT operations
type ColNested struct {
	columns []NestedColumn
}

// NestedColumn represents a single column within a Nested type.
type NestedColumn struct {
	Name string // Field name (e.g., "id")
	Data Column // Array column (e.g., *ColArr[uint64])
}

// NewNested creates a new ColNested with the given columns.
//
// Each column's Data should be an Array type. Example:
//
//	nested := NewNested(
//	    NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
//	    NestedColumn{Name: "name", Data: new(ColStr).Array()},
//	)
func NewNested(columns ...NestedColumn) *ColNested {
	return &ColNested{columns: columns}
}

// Columns returns the nested columns.
func (c *ColNested) Columns() []NestedColumn {
	return c.columns
}

// Column returns the column with the given name, or nil if not found.
func (c *ColNested) Column(name string) *NestedColumn {
	for i := range c.columns {
		if c.columns[i].Name == name {
			return &c.columns[i]
		}
	}
	return nil
}

// Type returns the Nested type string, e.g., "Nested(id UInt64, name String)".
func (c *ColNested) Type() ColumnType {
	if len(c.columns) == 0 {
		return "Nested()"
	}
	var types []ColumnType
	for _, col := range c.columns {
		// Extract element type from Array(T) -> T
		elemType := col.Data.Type().Elem()
		types = append(types, ColumnType(col.Name+" "+elemType.String()))
	}
	return ColumnTypeNested.Sub(types...)
}

// Rows returns the number of rows (from the first column).
func (c *ColNested) Rows() int {
	if len(c.columns) == 0 {
		return 0
	}
	return c.columns[0].Data.Rows()
}

// Reset clears all columns.
func (c *ColNested) Reset() {
	for _, col := range c.columns {
		col.Data.Reset()
	}
}

// InputColumns returns []InputColumn for INSERT operations.
// The prefix is added to each column name with a dot separator.
//
// Example:
//
//	nested.InputColumns("n")
//	// Returns: [{Name: "n.id", Data: ...}, {Name: "n.name", Data: ...}]
func (c *ColNested) InputColumns(prefix string) []InputColumn {
	result := make([]InputColumn, len(c.columns))
	for i, col := range c.columns {
		result[i] = InputColumn{
			Name: prefix + "." + col.Name,
			Data: col.Data,
		}
	}
	return result
}

// ResultColumns returns []ResultColumn for SELECT operations.
// The prefix is added to each column name with a dot separator.
//
// Example:
//
//	nested.ResultColumns("n")
//	// Returns: [{Name: "n.id", Data: ...}, {Name: "n.name", Data: ...}]
func (c *ColNested) ResultColumns(prefix string) []ResultColumn {
	result := make([]ResultColumn, len(c.columns))
	for i, col := range c.columns {
		result[i] = ResultColumn{
			Name: prefix + "." + col.Name,
			Data: col.Data,
		}
	}
	return result
}

// Append appends a row to the nested column.
// The values map should contain all column names with their array values.
// All arrays must have the same length.
//
// Example:
//
//	nested.Append(map[string]any{
//	    "id":   []uint64{1, 2, 3},
//	    "name": []string{"a", "b", "c"},
//	})
func (c *ColNested) Append(values map[string]any) error {
	if len(values) != len(c.columns) {
		return errors.Errorf("expected %d columns, got %d", len(c.columns), len(values))
	}

	// First pass: validate all columns exist and arrays have equal length
	expectedLen := -1
	for _, col := range c.columns {
		val, ok := values[col.Name]
		if !ok {
			return errors.Errorf("missing value for column %q", col.Name)
		}

		arrLen := reflectArrayLen(val)
		if arrLen < 0 {
			return errors.Errorf("value for column %q is not a slice", col.Name)
		}

		if expectedLen == -1 {
			expectedLen = arrLen
		} else if arrLen != expectedLen {
			return errors.Errorf("column %q has length %d, expected %d (all arrays must have equal length)",
				col.Name, arrLen, expectedLen)
		}
	}

	// Second pass: append to each column
	for _, col := range c.columns {
		val := values[col.Name]
		if err := appendToArray(col.Data, val); err != nil {
			return errors.Wrapf(err, "append to column %q", col.Name)
		}
	}

	return nil
}

// AppendTyped is a type-safe way to append a row using a struct.
// The struct fields should match the column names (case-sensitive or using tags).
// This is more efficient than Append as it avoids reflection in hot path.
//
// For now, use Append() which uses reflection. Type-safe variants can be
// added for common use cases.

// Infer implements Inferable interface.
// It parses the Nested type and initializes the columns.
func (c *ColNested) Infer(t ColumnType) error {
	if t.Base() != ColumnTypeNested {
		return errors.Errorf("expected Nested type, got %q", t.Base())
	}

	fields, err := ParseNestedFields(string(t.Elem()))
	if err != nil {
		return errors.Wrap(err, "parse nested fields")
	}

	// If columns already exist, validate and update them
	if len(c.columns) > 0 {
		if len(c.columns) != len(fields) {
			return errors.Errorf("column count mismatch: have %d, type has %d",
				len(c.columns), len(fields))
		}
		for i, field := range fields {
			if c.columns[i].Name != field.Name {
				return errors.Errorf("column name mismatch at %d: have %q, type has %q",
					i, c.columns[i].Name, field.Name)
			}
			// Infer the inner type (as Array)
			if infer, ok := c.columns[i].Data.(Inferable); ok {
				arrayType := ColumnTypeArray.Sub(field.Type)
				if err := infer.Infer(arrayType); err != nil {
					return errors.Wrapf(err, "infer column %q", field.Name)
				}
			}
		}
		return nil
	}

	// Create new columns using ColAuto for automatic type inference
	c.columns = make([]NestedColumn, len(fields))
	for i, field := range fields {
		inner := new(ColAuto)
		arrayType := ColumnTypeArray.Sub(field.Type)
		if err := inner.Infer(arrayType); err != nil {
			return errors.Wrapf(err, "infer column %q", field.Name)
		}
		c.columns[i] = NestedColumn{
			Name: field.Name,
			Data: inner.Data,
		}
	}

	return nil
}

// Prepare implements Preparable interface.
func (c *ColNested) Prepare() error {
	for _, col := range c.columns {
		if p, ok := col.Data.(Preparable); ok {
			if err := p.Prepare(); err != nil {
				return errors.Wrapf(err, "prepare column %q", col.Name)
			}
		}
	}
	return nil
}

// DecodeState implements StateDecoder interface.
func (c *ColNested) DecodeState(r *Reader) error {
	for _, col := range c.columns {
		if s, ok := col.Data.(StateDecoder); ok {
			if err := s.DecodeState(r); err != nil {
				return errors.Wrapf(err, "decode state for column %q", col.Name)
			}
		}
	}
	return nil
}

// EncodeState implements StateEncoder interface.
func (c *ColNested) EncodeState(b *Buffer) {
	for _, col := range c.columns {
		if s, ok := col.Data.(StateEncoder); ok {
			s.EncodeState(b)
		}
	}
}

// EncodeColumn is not supported for Nested types.
// Nested columns must be encoded as separate Array columns using InputColumns().
// This method exists only to satisfy the Column interface for type inference.
func (c *ColNested) EncodeColumn(b *Buffer) {
	// Nested types are sent as multiple Array columns, not a single column.
	// Use InputColumns(prefix) to get the flattened columns for INSERT.
	panic("ColNested.EncodeColumn: Nested types must be encoded as separate Array columns using InputColumns()")
}

// WriteColumn is not supported for Nested types.
// Nested columns must be written as separate Array columns using InputColumns().
func (c *ColNested) WriteColumn(w *Writer) {
	// Nested types are sent as multiple Array columns, not a single column.
	// Use InputColumns(prefix) to get the flattened columns for INSERT.
	panic("ColNested.WriteColumn: Nested types must be written as separate Array columns using InputColumns()")
}

// DecodeColumn is not supported for Nested types.
// Nested columns must be decoded as separate Array columns using ResultColumns().
// This method exists only to satisfy the Column interface for type inference.
func (c *ColNested) DecodeColumn(r *Reader, rows int) error {
	// Nested types are received as multiple Array columns, not a single column.
	// Use ResultColumns(prefix) to get the expected columns for SELECT.
	return errors.New("ColNested.DecodeColumn: Nested types must be decoded as separate Array columns using ResultColumns()")
}

// Note: ColNested does NOT encode/decode directly because ClickHouse sends
// Nested as multiple separate Array columns with dot-notation names.
// Use InputColumns() and ResultColumns() to get the flattened columns.
//
// For INSERT: Use InputColumns(prefix) to get []InputColumn
// For SELECT: Use ResultColumns(prefix) to get []ResultColumn

// reflectArrayLen returns the length of a slice/array, or -1 if not a slice.
func reflectArrayLen(v any) int {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return -1
	}
	return rv.Len()
}

// appendToArray appends values to an array column using reflection.
func appendToArray(col Column, values any) error {
	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return errors.New("values must be a slice")
	}

	// Get the underlying array column and use type assertion to call Append
	colVal := reflect.ValueOf(col)

	// Try to find Append method
	appendMethod := colVal.MethodByName("Append")
	if !appendMethod.IsValid() {
		return errors.Errorf("column type %T does not have Append method", col)
	}

	// Call Append with the values
	results := appendMethod.Call([]reflect.Value{rv})
	if len(results) > 0 && !results[0].IsNil() {
		if err, ok := results[0].Interface().(error); ok {
			return err
		}
	}

	return nil
}

// Compile-time assertions for ColNested.
var (
	_ Inferable    = (*ColNested)(nil)
	_ Preparable   = (*ColNested)(nil)
	_ StateEncoder = (*ColNested)(nil)
	_ StateDecoder = (*ColNested)(nil)
)
