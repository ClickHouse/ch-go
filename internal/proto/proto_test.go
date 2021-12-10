package proto

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func requireDecode(t testing.TB, buf []byte, code int, v Decoder) {
	t.Helper()

	r := NewReader(bytes.NewReader(buf))
	gotCode, err := r.Int()
	require.NoError(t, err)
	require.Equal(t, code, gotCode, "code mismatch")
	require.NoError(t, v.Decode(r))
}
