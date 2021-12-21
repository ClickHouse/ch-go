package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLowCardinalityOf(t *testing.T) {
	_ = LowCardinalityOf[string](new(ColStr))

	v := (&ColStr{}).LowCardinality()
	v.Values = append(v.Values, "foo", "boo")

	require.NoError(t, v.Prepare())
}
