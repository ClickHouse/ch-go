package proto

import (
	"fmt"

	"github.com/go-faster/errors"
)

// BlockInfo describes block.
type BlockInfo struct {
	Overflows bool
	BucketNum int
}

func (i BlockInfo) String() string {
	return fmt.Sprintf("overflows: %v, buckets: %d", i.Overflows, i.BucketNum)
}

const endField = 0 // end of field pairs

// fields of BlockInfo.
const (
	blockInfoOverflows = 1
	blockInfoBucketNum = 2
)

// Encode to Buffer.
func (i BlockInfo) Encode(b *Buffer) {
	b.PutUVarInt(blockInfoOverflows)
	b.PutBool(i.Overflows)

	b.PutUVarInt(blockInfoBucketNum)
	b.PutInt32(int32(i.BucketNum))

	b.PutUVarInt(endField)
}

func (i *BlockInfo) Decode(r *Reader) error {
	for {
		f, err := r.Uvarint()
		if err != nil {
			return errors.Wrap(err, "field id")
		}
		switch f {
		case blockInfoOverflows:
			v, err := r.Bool()
			if err != nil {
				return errors.Wrap(err, "overflows")
			}
			i.Overflows = v
		case blockInfoBucketNum:
			v, err := r.Int32()
			if err != nil {
				return errors.Wrap(err, "bucket number")
			}
			i.BucketNum = int(v)
		case endField:
			return nil
		default:
			return errors.Errorf("unknown field %d", f)
		}
	}
}
