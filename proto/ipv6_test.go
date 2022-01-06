package proto

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"inet.af/netaddr"

	"github.com/go-faster/ch/internal/gold"
)

func TestIPv6_String(t *testing.T) {
	for _, v := range []netaddr.IP{
		netaddr.MustParseIP("2001:db8:ac10:fe01:feed:babe:cafe:0"),
		netaddr.MustParseIP("2001:4860:4860::8888"),
	} {
		d := ToIPv6(v)
		require.Equal(t, v.String(), d.String())
	}
}

func IPv6FromInt(v int) IPv6 {
	s := IPv6{}
	binary.BigEndian.PutUint64(s[:], uint64(v))
	return s
}

func TestToIPv6(t *testing.T) {
	v := netaddr.MustParseIP("2001:db8:ac10:fe01:feed:babe:cafe:0")
	b := make([]byte, 16)
	binPutIPv6(b, v.As16())
	ip := binIPv6(b)
	require.Equal(t, v, ip.ToIP())
}

func TestColIPv6_NetAddr(t *testing.T) {
	input := []netaddr.IP{
		netaddr.MustParseIP("2001:db8:ac10:fe01:feed:babe:cafe:0"),
		netaddr.MustParseIP("2001:db8:ac10:fe01:feed:babe:cafe:1"),
		netaddr.MustParseIP("2001:db8:ac10:fe01:feed:babe:cafe:2"),
	}
	var d ColIPv6
	for _, v := range input {
		d = append(d, ToIPv6(v))
	}
	var netBuf Buffer
	d.EncodeColumn(&netBuf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, netBuf.Buf, "col_ipv6_netaddr")
	})
	t.Run("Decode", func(t *testing.T) {
		br := bytes.NewReader(netBuf.Buf)
		r := NewReader(br)

		var dec ColIPv6
		require.NoError(t, dec.DecodeColumn(r, len(input)))
		var output []netaddr.IP
		for _, v := range dec {
			output = append(output, v.ToIP())
		}
		require.Equal(t, input, output)
	})
}
