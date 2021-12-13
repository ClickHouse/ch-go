//go:build go1.18

package ch

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-faster/ch/internal/gold"
	"github.com/go-faster/ch/proto"
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
	d := &proto.ColIPv6{}
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

func TestEncodeBlock(t *testing.T) {
	data := encodeTestStrBlock()
	gold.Bytes(t, data, "test_arr_str_block")

	r := proto.NewReader(bytes.NewReader(data))
	v := proto.Version
	d := []proto.ResultColumn{
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
		d := []proto.ResultColumn{
			{
				Name: "foo",
				Data: &proto.ColArr{
					Data: &proto.ColStr{},
				},
			},
		}

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip()
		}

		var block proto.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip()
		}
	})
}
