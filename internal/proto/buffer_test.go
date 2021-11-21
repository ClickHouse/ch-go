package proto

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestBuffer_PutString(t *testing.T) {
	var b Buffer
	s := "Hello, world!"
	b.PutString(s)

	fmt.Println(hex.Dump(b.Buf))
	fmt.Println(base64.RawStdEncoding.EncodeToString(b.Buf))
}
