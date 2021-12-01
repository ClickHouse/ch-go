package proto

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt32(t *testing.T) {
	v := int32(1000)

	// Encode.
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf, uint32(v))

	// Decode.
	d := int32(binary.LittleEndian.Uint32(buf))
	fmt.Println(d) // 1000

	fmt.Println(hex.Dump(buf))
	fmt.Println(base64.RawStdEncoding.EncodeToString(buf))

	r := NewReader(bytes.NewBuffer(buf))
	i, err := r.Int32()
	require.NoError(t, err)
	require.Equal(t, v, i)
}

func requireDecode(t testing.TB, buf []byte, code int, v Decoder) {
	t.Helper()

	r := NewReader(bytes.NewReader(buf))
	gotCode, err := r.Int()
	require.NoError(t, err)
	require.Equal(t, code, gotCode, "code mismatch")
	require.NoError(t, v.Decode(r))
}
