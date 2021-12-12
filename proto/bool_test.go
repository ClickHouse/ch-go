package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBool(t *testing.T) {
	values := []bool{
		false, true, false, false,
	}
	var b Buffer
	for _, v := range values {
		b.PutBool(v)
	}

	r := b.Reader()
	for _, v := range values {
		got, err := r.Bool()
		require.NoError(t, err)
		require.Equal(t, v, got)
	}
}
