package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
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
		require.NoError(t, b.Reader().Decode(&v))
		require.Equal(t, i, v)
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
