package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColMapOf(t *testing.T) {
	v := ColMapOf[string, string]{
		Keys:   &ColStr{},
		Values: &ColStr{},
	}
	_, _ = v.Get("foo")
	require.Equal(t, ColumnType("Map(String, String)"), v.Type())
}
