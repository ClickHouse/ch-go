package proto

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
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
