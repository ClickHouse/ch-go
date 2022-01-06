package proto

import (
	"inet.af/netaddr"
)

// IPv6 represents IPv6 address.
//
// Same as FixedString(16) internally in ClickHouse.
type IPv6 [16]byte

func (v IPv6) String() string {
	return v.ToIP().String()
}

// ToIP represents IPv6 as netaddr.IP.
func (v IPv6) ToIP() netaddr.IP {
	return netaddr.IPv6Raw(v)
}

// ToIPv6 represents ip as IPv6.
func ToIPv6(ip netaddr.IP) IPv6 { return ip.As16() }

func binIPv6(b []byte) IPv6       { return *(*[16]byte)(b) }
func binPutIPv6(b []byte, v IPv6) { copy(b, v[:]) }
