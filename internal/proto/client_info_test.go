package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientInfo_EncodeAware(t *testing.T) {
	b := new(Buffer)
	v := queryCreateDatabase.Info
	v.EncodeAware(b, queryProtoVersion)

	t.Run("DecodeAware", func(t *testing.T) {
		var i ClientInfo
		r := NewReader(bytes.NewReader(b.Buf))
		assert.NoError(t, i.DecodeAware(r, queryProtoVersion))
		assert.Equal(t, v, i)
	})
}
