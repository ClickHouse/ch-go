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
}
