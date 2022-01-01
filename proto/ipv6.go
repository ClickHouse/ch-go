package proto

import (
	"encoding/binary"

	"inet.af/netaddr"
)

// IPv6 represents UInt128 address as UInt128 number.
//
// Not using netaddr.IP because UInt128 is more efficient.
//
// Use ToIP helper for convenience.
type IPv6 UInt128

// ToIP represents IPv6 as netaddr.IP.
func (v IPv6) ToIP() netaddr.IP {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], v.High)
	binary.BigEndian.PutUint64(buf[8:16], v.Low)
	return netaddr.IPv6Raw(buf)
}

// ToIPv6 represents ip as IPv6.
func ToIPv6(ip netaddr.IP) IPv6 {
	b := ip.As16()
	return IPv6{
		High: binary.BigEndian.Uint64(b[0:8]),
		Low:  binary.BigEndian.Uint64(b[8:16]),
	}
}

func binIPv6(b []byte) IPv6 {
	_ = b[:16] // bounds check hint to compiler; see golang.org/issue/14808
	// Using BigEndian for IPv6 as per ClickHouse implementation.
	return IPv6{
		High: binary.BigEndian.Uint64(b[0:8]),
		Low:  binary.BigEndian.Uint64(b[8:16]),
	}
}

func binPutIPv6(b []byte, v IPv6) {
	_ = b[:16] // bounds check hint to compiler; see golang.org/issue/14808
	// Using BigEndian for IPv6 as per ClickHouse implementation.
	binary.BigEndian.PutUint64(b[0:8], v.High)
	binary.BigEndian.PutUint64(b[8:16], v.Low)
}
