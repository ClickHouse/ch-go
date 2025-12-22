package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNestedFields(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []NestedField
		wantErr  bool
		errMsg   string
	}{
		// Basic cases
		{
			name:  "single field",
			input: "a UInt32",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
			},
		},
		{
			name:  "two fields",
			input: "a UInt32, b String",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
			},
		},
		{
			name:  "three fields",
			input: "id UInt64, name String, value Float64",
			expected: []NestedField{
				{Name: "id", Type: "UInt64"},
				{Name: "name", Type: "String"},
				{Name: "value", Type: "Float64"},
			},
		},

		// Nested types (parentheses)
		{
			name:  "array type",
			input: "values Array(String)",
			expected: []NestedField{
				{Name: "values", Type: "Array(String)"},
			},
		},
		{
			name:  "nullable type",
			input: "data Nullable(Int32)",
			expected: []NestedField{
				{Name: "data", Type: "Nullable(Int32)"},
			},
		},
		{
			name:  "array and simple",
			input: "items Array(Int64), count UInt32",
			expected: []NestedField{
				{Name: "items", Type: "Array(Int64)"},
				{Name: "count", Type: "UInt32"},
			},
		},
		{
			name:  "multiple complex types",
			input: "arr Array(String), nullable Nullable(Float64), simple Int8",
			expected: []NestedField{
				{Name: "arr", Type: "Array(String)"},
				{Name: "nullable", Type: "Nullable(Float64)"},
				{Name: "simple", Type: "Int8"},
			},
		},

		// Deeply nested types
		{
			name:  "array of arrays",
			input: "matrix Array(Array(Int8))",
			expected: []NestedField{
				{Name: "matrix", Type: "Array(Array(Int8))"},
			},
		},
		{
			name:  "array of arrays with other field",
			input: "matrix Array(Array(Int8)), label String",
			expected: []NestedField{
				{Name: "matrix", Type: "Array(Array(Int8))"},
				{Name: "label", Type: "String"},
			},
		},
		{
			name:  "map type",
			input: "metadata Map(String, Int32)",
			expected: []NestedField{
				{Name: "metadata", Type: "Map(String, Int32)"},
			},
		},
		{
			name:  "map with array value",
			input: "data Map(String, Array(Int64))",
			expected: []NestedField{
				{Name: "data", Type: "Map(String, Array(Int64))"},
			},
		},
		{
			name:  "low cardinality",
			input: "category LowCardinality(String)",
			expected: []NestedField{
				{Name: "category", Type: "LowCardinality(String)"},
			},
		},
		{
			name:  "tuple type",
			input: "point Tuple(Float64, Float64)",
			expected: []NestedField{
				{Name: "point", Type: "Tuple(Float64, Float64)"},
			},
		},
		{
			name:  "named tuple",
			input: "coords Tuple(x Float64, y Float64)",
			expected: []NestedField{
				{Name: "coords", Type: "Tuple(x Float64, y Float64)"},
			},
		},
		{
			name:  "nullable array",
			input: "values Nullable(Array(Int32))",
			expected: []NestedField{
				{Name: "values", Type: "Nullable(Array(Int32))"},
			},
		},
		{
			name:  "array of nullable",
			input: "values Array(Nullable(Int32))",
			expected: []NestedField{
				{Name: "values", Type: "Array(Nullable(Int32))"},
			},
		},

		// Whitespace variations
		{
			name:  "extra spaces around commas",
			input: "a UInt32 ,  b String ,  c Int64",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
				{Name: "c", Type: "Int64"},
			},
		},
		{
			name:  "leading/trailing whitespace",
			input: "  a UInt32, b String  ",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
			},
		},
		{
			name:  "tabs and newlines",
			input: "a\tUInt32,\nb\tString",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
			},
		},

		// Underscore names
		{
			name:  "underscore in name",
			input: "user_id UInt64, created_at DateTime",
			expected: []NestedField{
				{Name: "user_id", Type: "UInt64"},
				{Name: "created_at", Type: "DateTime"},
			},
		},

		// DateTime with parameters
		{
			name:  "datetime64 with precision",
			input: "ts DateTime64(3)",
			expected: []NestedField{
				{Name: "ts", Type: "DateTime64(3)"},
			},
		},
		{
			name:  "datetime64 with precision and timezone",
			input: "ts DateTime64(3, 'UTC')",
			expected: []NestedField{
				{Name: "ts", Type: "DateTime64(3, 'UTC')"},
			},
		},

		// Fixed string
		{
			name:  "fixed string",
			input: "code FixedString(3)",
			expected: []NestedField{
				{Name: "code", Type: "FixedString(3)"},
			},
		},

		// Decimal
		{
			name:  "decimal with precision and scale",
			input: "price Decimal(10, 2)",
			expected: []NestedField{
				{Name: "price", Type: "Decimal(10, 2)"},
			},
		},

		// Enum
		{
			name:  "enum8",
			input: "status Enum8('active' = 1, 'inactive' = 2)",
			expected: []NestedField{
				{Name: "status", Type: "Enum8('active' = 1, 'inactive' = 2)"},
			},
		},

		// Complex realistic example
		{
			name:  "realistic nested definition",
			input: "id UInt64, name String, tags Array(String), metadata Map(String, String), created_at DateTime64(3)",
			expected: []NestedField{
				{Name: "id", Type: "UInt64"},
				{Name: "name", Type: "String"},
				{Name: "tags", Type: "Array(String)"},
				{Name: "metadata", Type: "Map(String, String)"},
				{Name: "created_at", Type: "DateTime64(3)"},
			},
		},

		// Error cases
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
			errMsg:  "empty nested type definition",
		},
		{
			name:    "whitespace only",
			input:   "   ",
			wantErr: true,
			errMsg:  "empty nested type definition",
		},
		{
			name:    "missing type",
			input:   "fieldname",
			wantErr: true,
			errMsg:  "expected 'name Type' format",
		},
		{
			name:    "unbalanced open paren",
			input:   "a Array(String",
			wantErr: true,
			errMsg:  "unbalanced parentheses",
		},
		{
			name:    "unbalanced close paren",
			input:   "a String)",
			wantErr: true,
			errMsg:  "unbalanced parentheses",
		},
		{
			name:    "empty field between commas",
			input:   "a UInt32, , b String",
			wantErr: true,
			errMsg:  "empty field definition",
		},
		{
			name:  "trailing comma is tolerated",
			input: "a UInt32, b String,",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
			},
		},
		{
			name:    "invalid name with parens",
			input:   "a() UInt32",
			wantErr: true,
			errMsg:  "invalid field name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := ParseNestedFields(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tt.expected), len(fields), "field count mismatch")

			for i, expected := range tt.expected {
				assert.Equal(t, expected.Name, fields[i].Name, "field %d name", i)
				assert.Equal(t, expected.Type, fields[i].Type, "field %d type", i)
			}
		})
	}
}

func TestParseNestedType(t *testing.T) {
	tests := []struct {
		name     string
		input    ColumnType
		expected []NestedField
		wantErr  bool
		errMsg   string
	}{
		{
			name:  "simple nested type",
			input: "Nested(a UInt32, b String)",
			expected: []NestedField{
				{Name: "a", Type: "UInt32"},
				{Name: "b", Type: "String"},
			},
		},
		{
			name:  "nested with array",
			input: "Nested(id UInt64, tags Array(String))",
			expected: []NestedField{
				{Name: "id", Type: "UInt64"},
				{Name: "tags", Type: "Array(String)"},
			},
		},
		{
			name:  "complex nested type",
			input: "Nested(x Array(Array(Int8)), y Map(String, Int32), z Nullable(Float64))",
			expected: []NestedField{
				{Name: "x", Type: "Array(Array(Int8))"},
				{Name: "y", Type: "Map(String, Int32)"},
				{Name: "z", Type: "Nullable(Float64)"},
			},
		},
		{
			name:    "not a nested type",
			input:   "Array(String)",
			wantErr: true,
			errMsg:  "expected Nested type",
		},
		{
			name:    "empty nested",
			input:   "Nested()",
			wantErr: true,
			errMsg:  "empty Nested type definition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := ParseNestedType(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tt.expected), len(fields), "field count mismatch")

			for i, expected := range tt.expected {
				assert.Equal(t, expected.Name, fields[i].Name, "field %d name", i)
				assert.Equal(t, expected.Type, fields[i].Type, "field %d type", i)
			}
		})
	}
}

func TestParseNameTypePair(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected NestedField
		wantErr  bool
	}{
		{
			name:     "simple",
			input:    "id UInt64",
			expected: NestedField{Name: "id", Type: "UInt64"},
		},
		{
			name:     "with params",
			input:    "arr Array(String)",
			expected: NestedField{Name: "arr", Type: "Array(String)"},
		},
		{
			name:     "underscore name",
			input:    "user_id UInt64",
			expected: NestedField{Name: "user_id", Type: "UInt64"},
		},
		{
			name:     "multiple spaces",
			input:    "field    Type",
			expected: NestedField{Name: "field", Type: "Type"},
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "no type",
			input:   "fieldonly",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := parseNameTypePair(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected.Name, field.Name)
			assert.Equal(t, tt.expected.Type, field.Type)
		})
	}
}
