//go:build go1.18

package ch

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-faster/ch/internal/gold"
	proto2 "github.com/go-faster/ch/proto"
)

func encodeTestStrBlock() []byte {
	b := &proto2.Buffer{}
	d := &proto2.ColStr{}
	arr := &proto2.ColArr{
		Data: d,
	}
	for _, v := range [][]string{
		{"foo", "bar"},
		{"1", "2", "3", "4"},
		{"", strings.Repeat("123", 3)},
	} {
		d.ArrAppend(arr, v)
	}
	input := []proto2.InputColumn{
		{
			Name: "foo",
			Data: arr,
		},
	}
	block := &proto2.Block{
		Info:    proto2.BlockInfo{BucketNum: -1},
		Columns: 1,
		Rows:    3,
	}

	block.EncodeAware(b, proto2.Version)
	for _, col := range input {
		col.EncodeStart(b)
		col.Data.EncodeColumn(b)
	}

	return b.Buf
}

func TestEncodeBlock(t *testing.T) {
	data := encodeTestStrBlock()
	gold.Bytes(t, data, "test_arr_str_block")

	r := proto2.NewReader(bytes.NewReader(data))
	v := proto2.Version
	d := []proto2.ResultColumn{
		{
			Name: "foo",
			Data: &proto2.ColArr{
				Data: &proto2.ColStr{},
			},
		},
	}

	// Skip table name.
	if _, err := r.Str(); err != nil {
		t.Fatal(err)
	}

	var block proto2.Block
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
		r := proto2.NewReader(bytes.NewReader(data))
		v := proto2.Version
		d := []proto2.ResultColumn{
			{
				Name: "foo",
				Data: &proto2.ColArr{
					Data: &proto2.ColStr{},
				},
			},
		}

		// Skip table name.
		if _, err := r.Str(); err != nil {
			t.Skip()
		}

		var block proto2.Block
		if err := block.DecodeBlock(r, v, d); err != nil {
			t.Skip()
		}
	})
}
