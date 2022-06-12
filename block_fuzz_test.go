package ch

import (
	"bytes"
	"net/netip"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/internal/gold"
	"github.com/ClickHouse/ch-go/proto"
)

func encodeTestStrBlock() []byte {
	b := &proto.Buffer{}
	d := &proto.ColStr{}
	arr := &proto.ColArr{
		Data: d,
	}
	for _, v := range [][]string{
		{"foo", "bar"},
		{"1", "2", "3", "4"},
		{"", strings.Repeat("123", 3)},
	} {
		d.ArrAppend(arr, v)
	}
	input := []proto.InputColumn{
		{
			Name: "foo",
			Data: arr,
		},
	}
	block := &proto.Block{
		Info:    proto.BlockInfo{BucketNum: -1},
		Columns: 1,
		Rows:    3,
	}

	block.EncodeAware(b, proto.Version)
	for _, col := range input {
		col.EncodeStart(b)
		col.Data.EncodeColumn(b)
	}

	return b.Buf
}

func encodeTestIPv6Block() []byte {
	b := &proto.Buffer{}

	arr := proto.NewArrIPv6()
	for _, v := range [][]string{
		{"100::", "200::"},
		{"300::", "400::", "500::", "600::"},
		{"2001:db8::", "2002::"},
	} {
		var values []proto.IPv6
		for _, s := range v {
			ip := netip.MustParseAddr(s)
			values = append(values, proto.ToIPv6(ip))
		}
		arr.AppendIPv6(values)
	}
	input := []proto.InputColumn{
		{
			Name: "foo",
			Data: arr,
		},
	}
	block := &proto.Block{
		Info:    proto.BlockInfo{BucketNum: -1},
		Columns: 1,
		Rows:    3,
	}

	block.EncodeAware(b, proto.Version)
	for _, col := range input {
		col.EncodeStart(b)
		col.Data.EncodeColumn(b)
	}

	return b.Buf
}

func TestEncodeIPv6Block(t *testing.T) {
	data := encodeTestIPv6Block()
	gold.Bytes(t, data, "test_arr_ipv6_block")

	r := proto.NewReader(bytes.NewReader(data))
	v := proto.Version
	a := proto.NewArrIPv6()
	d := proto.Results{
		{
			Name: "foo",
			Data: a,
		},
	}

	// Skip table name.
	if _, err := r.Str(); err != nil {
		t.Fatal(err)
	}

	var block proto.Block
	if err := block.DecodeBlock(r, v, d); err != nil {
		t.Fatal(err)
	}
	if block.End() {
		return
	}
}

func TestEncodeBlock(t *testing.T) {
	data := encodeTestStrBlock()
	gold.Bytes(t, data, "test_arr_str_block")

	r := proto.NewReader(bytes.NewReader(data))
	v := proto.Version
	d := proto.Results{
		{
			Name: "foo",
			Data: &proto.ColArr{
				Data: &proto.ColStr{},
			},
		},
	}

	// Skip table name.
	if _, err := r.Str(); err != nil {
		t.Fatal(err)
	}

	var block proto.Block
	if err := block.DecodeBlock(r, v, d); err != nil {
		t.Fatal(err)
	}
	if block.End() {
		return
	}
}

func FuzzDecodeBlock(f *testing.F) {
	f.Add(encodeTestStrBlock())

	f.Fuzz(func(t *testing.T, data []byte) {
		r := proto.NewReader(bytes.NewReader(data))
		v := proto.Version
		d := proto.Results{
			{
				Name: "foo",
				Data: &proto.ColArr{
					Data: &proto.ColStr{},
				},
			},
		}

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip(err)
		}

		var block proto.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip(err)
		}
	})
}

func FuzzDecodeArrayIPv6ArrayBlock(f *testing.F) {
	f.Add(encodeTestStrBlock())

	f.Fuzz(func(t *testing.T, data []byte) {
		r := proto.NewReader(bytes.NewReader(data))
		v := proto.Version
		d := proto.Results{
			{
				Name: "foo",
				Data: proto.NewArrIPv6(),
			},
		}

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip(err)
		}

		var block proto.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip(err)
		}
	})
}
