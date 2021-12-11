package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/go-faster/ch/internal/gold"
)

func TestClientInfo_EncodeAware(t *testing.T) {
	b := new(Buffer)
	v := queryCreateDatabase.Info
	v.EncodeAware(b, queryProtoVersion)
	gold.Bytes(t, b.Buf, "client_info")

	t.Run("DecodeAware", func(t *testing.T) {
		var i ClientInfo
		r := NewReader(bytes.NewReader(b.Buf))
		assert.NoError(t, i.DecodeAware(r, queryProtoVersion))
		assert.Equal(t, v, i)

		requireNoShortRead(t, b.Buf, aware(&i))
	})
}
