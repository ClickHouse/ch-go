package compress

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompress(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}

	w := NewWriter()
	require.NoError(t, w.Compress(data))

	r := NewReader(bytes.NewReader(w.Data))

	out := make([]byte, len(data))
	_, err := io.ReadFull(r, out)
	require.NoError(t, err)
}
