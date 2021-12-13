package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"inet.af/netaddr"

	"github.com/go-faster/ch/internal/gold"
)

func TestColIPv4_NetAddr(t *testing.T) {
	input := []netaddr.IP{
		netaddr.MustParseIP("127.0.0.1"),
		netaddr.MustParseIP("127.0.0.2"),
		netaddr.MustParseIP("127.0.0.3"),
	}
	var d ColIPv4
	for _, v := range input {
		d = append(d, ToIPv4(v))
	}
	var netBuf Buffer
	d.EncodeColumn(&netBuf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, netBuf.Buf, "col_ipv4_netaddr")
	})
	t.Run("Decode", func(t *testing.T) {
		br := bytes.NewReader(netBuf.Buf)
		r := NewReader(br)

		var dec ColIPv4
		require.NoError(t, dec.DecodeColumn(r, len(input)))
		var output []netaddr.IP
		for _, v := range dec {
			output = append(output, v.ToIP())
		}
		require.Equal(t, input, output)
	})
}
