package proto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/gold"
)

func TestBlockInfo_Encode(t *testing.T) {
	i := BlockInfo{
		Overflows: true,
		BucketNum: 1056,
	}
	var b Buffer
	b.Encode(i)
	require.Equal(t, []byte{0x1, 0x1, 0x2, 0x20, 0x4, 0x0, 0x0, 0x0}, b.Buf)
	t.Run("Decode", func(t *testing.T) {
		var v BlockInfo
		requireDecode(t, b.Buf, &v)
		require.Equal(t, i, v)
		requireNoShortRead(t, b.Buf, &v)
	})
}

func TestBlock_EncodeAware(t *testing.T) {
	Gold(t, Block{
		Info: BlockInfo{
			Overflows: true,
			BucketNum: 2014,
		},
		Columns: 15,
		Rows:    10,
	})
}

type resultAware struct {
	*Block
	out Result
}

func (c resultAware) Decode(r *Reader) error {
	return c.DecodeBlock(r, Version, c.out)
}

func resAware(v *Block, out Results) Decoder {
	return resultAware{
		Block: v,
		out:   out,
	}
}

func TestBlock_EncodeBlock(t *testing.T) {
	v := Block{
		Info: BlockInfo{
			BucketNum: -1,
		},
		Columns: 2,
		Rows:    5,
	}
	var b Buffer
	require.NoError(t, v.EncodeBlock(&b, Version, []InputColumn{
		{Name: "count", Data: ColInt8{1, 2, 3, 4, 5}},
		{Name: "users", Data: ColUInt64{5467267, 175676, 956105, 18347896, 554714}},
	}))
	gold.Bytes(t, b.Buf, "block_int8_uint64")
	t.Run("Ok", func(t *testing.T) {
		var dec Block
		var (
			countRes ColInt8
			usersRes ColUInt64
		)
		requireDecode(t, b.Buf, resAware(&dec, []ResultColumn{
			{Name: "count", Data: &countRes},
			{Name: "users", Data: &usersRes},
		}))
		require.Equal(t, dec, v)
	})
	t.Run("NoShortRead", func(t *testing.T) {
		var dec Block
		var (
			countRes ColInt8
			usersRes ColUInt64
		)
		requireNoShortRead(t, b.Buf, resAware(&dec, []ResultColumn{
			{Name: "count", Data: &countRes},
			{Name: "users", Data: &usersRes},
		}))
	})
	t.Run("BadColumn", func(t *testing.T) {
		var dec Block
		var (
			countRes ColInt8
			usersRes ColUInt64
		)
		for _, res := range []Results{
			{}, // No columns.
			{
				// Bad name.
				{Name: "counts", Data: &countRes},
				{Name: "users", Data: &usersRes},
			},
			{
				// Bad type.
				{Name: "count", Data: &countRes},
				{Name: "users", Data: new(ColStr)},
			},
		} {
			require.Error(t, dec.DecodeBlock(b.Reader(), Version, res))
		}
	})
}
