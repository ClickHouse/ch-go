package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColNestedBasic(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		nested := NewNested()
		assert.Equal(t, 0, nested.Rows())
		assert.Equal(t, "Nested()", nested.Type().String())
	})

	t.Run("single column", func(t *testing.T) {
		idCol := new(ColUInt64).Array()
		nested := NewNested(
			NestedColumn{Name: "id", Data: idCol},
		)

		assert.Equal(t, "Nested(id UInt64)", nested.Type().String())
		assert.Equal(t, 0, nested.Rows())

		// Append a row
		idCol.Append([]uint64{1, 2, 3})
		assert.Equal(t, 1, nested.Rows())
	})

	t.Run("multiple columns", func(t *testing.T) {
		idCol := new(ColUInt64).Array()
		nameCol := new(ColStr).Array()
		nested := NewNested(
			NestedColumn{Name: "id", Data: idCol},
			NestedColumn{Name: "name", Data: nameCol},
		)

		assert.Equal(t, "Nested(id UInt64, name String)", nested.Type().String())
	})
}

func TestColNestedType(t *testing.T) {
	tests := []struct {
		name     string
		columns  []NestedColumn
		expected string
	}{
		{
			name:     "empty",
			columns:  nil,
			expected: "Nested()",
		},
		{
			name: "single uint64",
			columns: []NestedColumn{
				{Name: "id", Data: new(ColUInt64).Array()},
			},
			expected: "Nested(id UInt64)",
		},
		{
			name: "uint64 and string",
			columns: []NestedColumn{
				{Name: "id", Data: new(ColUInt64).Array()},
				{Name: "name", Data: new(ColStr).Array()},
			},
			expected: "Nested(id UInt64, name String)",
		},
		{
			name: "various types",
			columns: []NestedColumn{
				{Name: "id", Data: new(ColUInt64).Array()},
				{Name: "name", Data: new(ColStr).Array()},
				{Name: "value", Data: new(ColFloat64).Array()},
				{Name: "flag", Data: new(ColBool).Array()},
			},
			expected: "Nested(id UInt64, name String, value Float64, flag Bool)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nested := NewNested(tt.columns...)
			assert.Equal(t, tt.expected, nested.Type().String())
		})
	}
}

func TestColNestedInputColumns(t *testing.T) {
	idCol := new(ColUInt64).Array()
	nameCol := new(ColStr).Array()
	nested := NewNested(
		NestedColumn{Name: "id", Data: idCol},
		NestedColumn{Name: "name", Data: nameCol},
	)

	inputs := nested.InputColumns("n")

	require.Len(t, inputs, 2)
	assert.Equal(t, "n.id", inputs[0].Name)
	assert.Equal(t, "n.name", inputs[1].Name)
	assert.Same(t, idCol, inputs[0].Data)
	assert.Same(t, nameCol, inputs[1].Data)
}

func TestColNestedResultColumns(t *testing.T) {
	idCol := new(ColUInt64).Array()
	nameCol := new(ColStr).Array()
	nested := NewNested(
		NestedColumn{Name: "id", Data: idCol},
		NestedColumn{Name: "name", Data: nameCol},
	)

	results := nested.ResultColumns("data")

	require.Len(t, results, 2)
	assert.Equal(t, "data.id", results[0].Name)
	assert.Equal(t, "data.name", results[1].Name)
	assert.Same(t, idCol, results[0].Data)
	assert.Same(t, nameCol, results[1].Data)
}

func TestColNestedAppend(t *testing.T) {
	t.Run("valid append", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "name", Data: new(ColStr).Array()},
		)

		err := nested.Append(map[string]any{
			"id":   []uint64{1, 2, 3},
			"name": []string{"a", "b", "c"},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, nested.Rows())

		// Append another row
		err = nested.Append(map[string]any{
			"id":   []uint64{4, 5},
			"name": []string{"d", "e"},
		})
		require.NoError(t, err)
		assert.Equal(t, 2, nested.Rows())
	})

	t.Run("empty arrays", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "name", Data: new(ColStr).Array()},
		)

		err := nested.Append(map[string]any{
			"id":   []uint64{},
			"name": []string{},
		})
		require.NoError(t, err)
		assert.Equal(t, 1, nested.Rows())
	})

	t.Run("mismatched lengths", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "name", Data: new(ColStr).Array()},
		)

		err := nested.Append(map[string]any{
			"id":   []uint64{1, 2, 3},
			"name": []string{"a", "b"}, // different length
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "all arrays must have equal length")
	})

	t.Run("missing column", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "name", Data: new(ColStr).Array()},
		)

		err := nested.Append(map[string]any{
			"id": []uint64{1, 2, 3},
			// missing "name"
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected 2 columns")
	})

	t.Run("wrong column name", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "name", Data: new(ColStr).Array()},
		)

		err := nested.Append(map[string]any{
			"id":    []uint64{1, 2, 3},
			"wrong": []string{"a", "b", "c"}, // wrong column name
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing value for column")
	})

	t.Run("not a slice", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
		)

		err := nested.Append(map[string]any{
			"id": "not a slice",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not a slice")
	})
}

func TestColNestedReset(t *testing.T) {
	idCol := new(ColUInt64).Array()
	nameCol := new(ColStr).Array()
	nested := NewNested(
		NestedColumn{Name: "id", Data: idCol},
		NestedColumn{Name: "name", Data: nameCol},
	)

	// Add some data
	idCol.Append([]uint64{1, 2, 3})
	nameCol.Append([]string{"a", "b", "c"})
	assert.Equal(t, 1, nested.Rows())

	// Reset
	nested.Reset()
	assert.Equal(t, 0, nested.Rows())
	assert.Equal(t, 0, idCol.Rows())
	assert.Equal(t, 0, nameCol.Rows())
}

func TestColNestedColumn(t *testing.T) {
	idCol := new(ColUInt64).Array()
	nameCol := new(ColStr).Array()
	nested := NewNested(
		NestedColumn{Name: "id", Data: idCol},
		NestedColumn{Name: "name", Data: nameCol},
	)

	t.Run("found", func(t *testing.T) {
		col := nested.Column("id")
		require.NotNil(t, col)
		assert.Equal(t, "id", col.Name)
		assert.Same(t, idCol, col.Data)
	})

	t.Run("not found", func(t *testing.T) {
		col := nested.Column("nonexistent")
		assert.Nil(t, col)
	})
}

func TestColNestedInfer(t *testing.T) {
	t.Run("simple types", func(t *testing.T) {
		nested := &ColNested{}
		err := nested.Infer("Nested(id UInt64, name String)")
		require.NoError(t, err)

		require.Len(t, nested.columns, 2)
		assert.Equal(t, "id", nested.columns[0].Name)
		assert.Equal(t, "name", nested.columns[1].Name)

		// Verify types
		assert.Equal(t, "Array(UInt64)", nested.columns[0].Data.Type().String())
		assert.Equal(t, "Array(String)", nested.columns[1].Data.Type().String())
	})

	t.Run("complex types", func(t *testing.T) {
		nested := &ColNested{}
		err := nested.Infer("Nested(value Float64, count UInt32, flag Bool)")
		require.NoError(t, err)

		require.Len(t, nested.columns, 3)
		assert.Equal(t, "value", nested.columns[0].Name)
		assert.Equal(t, "count", nested.columns[1].Name)
		assert.Equal(t, "flag", nested.columns[2].Name)

		// Each nested field becomes Array(T)
		assert.Equal(t, "Array(Float64)", nested.columns[0].Data.Type().String())
		assert.Equal(t, "Array(UInt32)", nested.columns[1].Data.Type().String())
		assert.Equal(t, "Array(Bool)", nested.columns[2].Data.Type().String())
	})

	t.Run("existing columns - matching", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "name", Data: new(ColStr).Array()},
		)

		err := nested.Infer("Nested(id UInt64, name String)")
		require.NoError(t, err)
	})

	t.Run("existing columns - count mismatch", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
		)

		err := nested.Infer("Nested(id UInt64, name String)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column count mismatch")
	})

	t.Run("existing columns - name mismatch", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "wrong", Data: new(ColStr).Array()},
		)

		err := nested.Infer("Nested(id UInt64, name String)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column name mismatch")
	})

	t.Run("not nested type", func(t *testing.T) {
		nested := &ColNested{}
		err := nested.Infer("Array(String)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected Nested type")
	})

	t.Run("empty nested", func(t *testing.T) {
		nested := &ColNested{}
		err := nested.Infer("Nested()")
		require.Error(t, err)
	})
}

func TestColNestedColumns(t *testing.T) {
	idCol := new(ColUInt64).Array()
	nameCol := new(ColStr).Array()
	nested := NewNested(
		NestedColumn{Name: "id", Data: idCol},
		NestedColumn{Name: "name", Data: nameCol},
	)

	cols := nested.Columns()
	require.Len(t, cols, 2)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, "name", cols[1].Name)
}

func TestColNestedPrepare(t *testing.T) {
	// Create nested with columns that implement Preparable
	nested := NewNested(
		NestedColumn{Name: "id", Data: new(ColUInt64).Array()},
	)

	// Prepare should not error for basic types
	err := nested.Prepare()
	require.NoError(t, err)
}

func TestColNestedIntegration(t *testing.T) {
	// Test a full workflow: create, append, get flattened columns
	t.Run("full workflow", func(t *testing.T) {
		nested := NewNested(
			NestedColumn{Name: "user_id", Data: new(ColUInt64).Array()},
			NestedColumn{Name: "event_name", Data: new(ColStr).Array()},
			NestedColumn{Name: "timestamp", Data: new(ColInt64).Array()},
		)

		// Append multiple rows
		require.NoError(t, nested.Append(map[string]any{
			"user_id":    []uint64{1, 2},
			"event_name": []string{"click", "view"},
			"timestamp":  []int64{1000, 2000},
		}))

		require.NoError(t, nested.Append(map[string]any{
			"user_id":    []uint64{3},
			"event_name": []string{"purchase"},
			"timestamp":  []int64{3000},
		}))

		assert.Equal(t, 2, nested.Rows())

		// Get input columns for INSERT
		inputs := nested.InputColumns("events")
		require.Len(t, inputs, 3)
		assert.Equal(t, "events.user_id", inputs[0].Name)
		assert.Equal(t, "events.event_name", inputs[1].Name)
		assert.Equal(t, "events.timestamp", inputs[2].Name)

		// Verify data in columns
		userIdCol := inputs[0].Data.(*ColArr[uint64])
		assert.Equal(t, 2, userIdCol.Rows())
		assert.Equal(t, []uint64{1, 2}, userIdCol.Row(0))
		assert.Equal(t, []uint64{3}, userIdCol.Row(1))
	})
}

func TestColNestedWithInferredTypes(t *testing.T) {
	// Test creating nested via Infer and then using it
	t.Run("infer then use", func(t *testing.T) {
		nested := &ColNested{}
		require.NoError(t, nested.Infer("Nested(id UInt64, name String)"))

		// Now the columns are set up, we can use InputColumns/ResultColumns
		inputs := nested.InputColumns("n")
		require.Len(t, inputs, 2)
		assert.Equal(t, "n.id", inputs[0].Name)
		assert.Equal(t, "n.name", inputs[1].Name)

		results := nested.ResultColumns("n")
		require.Len(t, results, 2)
		assert.Equal(t, "n.id", results[0].Name)
		assert.Equal(t, "n.name", results[1].Name)
	})
}
