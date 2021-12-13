package proto

import (
	"encoding/binary"

	"github.com/go-faster/errors"
	"inet.af/netaddr"
)

// IPv4 represents IPv4 address as uint32 number.
//
// Not using netaddr.IP because uint32 is 5 times faster,
// consumes 6 times less memory and better represents IPv4.
//
// Use ToIP helper for convenience.
type IPv4 uint32

// ToIP represents IPv4 as netaddr.IP.
func (v IPv4) ToIP() netaddr.IP {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	return netaddr.IPFrom4(buf)
}

// ToIPv4 represents ip as IPv4. Panics if ip is not ipv4.
func ToIPv4(ip netaddr.IP) IPv4 {
	b := ip.As4()
	return IPv4(binary.BigEndian.Uint32(b[:]))
}

// ColIPv4 is column of IPv4.
type ColIPv4 []IPv4

func (c ColIPv4) Type() ColumnType {
	return ColumnTypeIPv4
}

func (c ColIPv4) Rows() int {
	return len(c)
}

func (c *ColIPv4) DecodeColumn(r *Reader, rows int) error {
	const size = 32 / 8
	data, err := r.ReadRaw(rows * size)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	v := *c
	for i := 0; i < len(data); i += size {
		v = append(v,
			IPv4(bin.Uint32(data[i:i+size])),
		)
	}
	*c = v
	return nil
}

func (c *ColIPv4) Reset() {
	*c = (*c)[:0]
}

func (c ColIPv4) EncodeColumn(b *Buffer) {
	const size = 32 / 8
	offset := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, size*len(c))...)
	for _, v := range c {
		bin.PutUint32(
			b.Buf[offset:offset+size],
			uint32(v),
		)
		offset += size
	}
}
