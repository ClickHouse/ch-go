package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnType_Elem(t *testing.T) {
	t.Run("Array", func(t *testing.T) {
		v := ColumnTypeInt16.Array()
		assert.Equal(t, ColumnType("Array(Int16)"), v)
		assert.True(t, v.IsArray())
		assert.Equal(t, ColumnTypeInt16, v.Elem())
	})
	t.Run("Simple", func(t *testing.T) {
		assert.Equal(t, ColumnTypeNone, ColumnTypeFloat32.Elem())
		assert.False(t, ColumnTypeInt32.IsArray())
	})
	t.Run("Conflict", func(t *testing.T) {
		t.Run("Compatible", func(t *testing.T) {
			for _, tt := range []struct {
				A, B ColumnType
			}{
				{}, // blank
				{A: ColumnTypeInt32, B: ColumnTypeInt32},
				{A: ColumnTypeDateTime, B: ColumnTypeDateTime},
				{A: ColumnTypeArray.Sub(ColumnTypeInt32), B: ColumnTypeArray.Sub(ColumnTypeInt32)},
				{A: ColumnTypeDateTime.With("Europe/Moscow"), B: ColumnTypeDateTime.With("UTC")},
				{A: ColumnTypeDateTime.With("Europe/Moscow"), B: ColumnTypeDateTime},
			} {
				assert.False(t, tt.A.Conflicts(tt.B),
					"%s ~ %s", tt.A, tt.B,
				)
			}
		})
		t.Run("Incompatible", func(t *testing.T) {
			for _, tt := range []struct {
				A, B ColumnType
			}{
				{A: ColumnTypeInt64}, // blank
				{A: ColumnTypeInt32, B: ColumnTypeInt64},
				{A: ColumnTypeDateTime, B: ColumnTypeInt32},
				{A: ColumnTypeArray.Sub(ColumnTypeInt32), B: ColumnTypeArray.Sub(ColumnTypeInt64)},
			} {
				assert.True(t, tt.A.Conflicts(tt.B),
					"%s !~ %s", tt.A, tt.B,
				)
			}
		})
	})
}
