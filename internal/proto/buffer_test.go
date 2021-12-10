package proto

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/go-faster/ch/internal/gold"
)

func TestBuffer_PutString(t *testing.T) {
	var b Buffer
	s := "Hello, world!"
	b.PutString(s)

	fmt.Println(hex.Dump(b.Buf))
	fmt.Println(base64.RawStdEncoding.EncodeToString(b.Buf))
}

func TestBuffer(t *testing.T) {
	var b Buffer

	b.PutString("Hello, world!")
	b.PutInt(1)
	b.PutInt8(2)
	b.PutInt16(3)
	b.PutInt32(4)
	b.PutInt64(5)
	b.PutUInt8(1)
	b.PutUInt16(2)
	b.PutUInt32(3)
	b.PutUInt64(4)
	b.PutUVarInt(100)
	b.PutUVarInt(200)
	b.PutLen(114)
	b.PutRaw([]byte{1, 2, 3, 4})
	b.PutBool(true)
	b.PutBool(false)
	b.PutByte(1)
	b.PutFloat32(1.12345)
	b.PutFloat64(500.345)

	gold.Bytes(t, b.Buf, "buffer")
}
