package proto

import (
	"fmt"
	"strings"

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
		f, err := r.UVarInt()
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

type RawColumn struct {
	Name string
	Type ColumnType
	Data []byte
}

func (c RawColumn) Encode(b *Buffer) {
	b.PutString(c.Name)
	b.PutString(string(c.Type))
	b.PutRaw(c.Data)
}

type Block struct {
	Info    BlockInfo
	Columns int
	Rows    int
	Data    []RawColumn
}

func (b Block) String() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("[%d]", b.Rows))
	s.WriteRune('(')
	for i, column := range b.Data {
		if i != 0 {
			s.WriteString(", ")
		}
		s.WriteString(column.Name)
		s.WriteString(": ")
		s.WriteString(string(column.Type))
	}
	s.WriteRune(')')
	return s.String()
}

func (b Block) EncodeAware(buf *Buffer, revision int) {
	if FeatureBlockInfo.In(revision) {
		b.Info.Encode(buf)
	}

	buf.PutInt(b.Columns)
	buf.PutInt(b.Rows)

	for _, c := range b.Data {
		buf.PutString(c.Name)
		buf.PutString(string(c.Type))
		buf.PutRaw(c.Data)
	}
}

const (
	maxColumnsInBlock = 1_000_000
	maxRowsInBlock    = 1_000_000
)

func (b *Block) DecodeAware(r *Reader, revision int) error {
	if FeatureBlockInfo.In(revision) {
		if err := b.Info.Decode(r); err != nil {
			return errors.Wrap(err, "info")
		}
	}

	{
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "columns")
		}
		if v > maxColumnsInBlock || v < 0 {
			return errors.Errorf("invalid columns number %d", v)
		}
		b.Columns = v
	}
	{
		v, err := r.Int()
		if err != nil {
			return errors.Wrap(err, "rows")
		}
		if v > maxRowsInBlock || v < 0 {
			return errors.Errorf("invalid columns number %d", v)
		}
		b.Rows = v
	}

	for i := 0; i < b.Columns; i++ {
		columnName, err := r.Str()
		if err != nil {
			return errors.Wrapf(err, "column [%d] name", i)
		}
		columnType, err := r.Str()
		if err != nil {
			return errors.Wrapf(err, "column [%d] type", i)
		}

		// Read fixed size.
		var columnSize int
		switch ColumnType(columnType) {
		case ColumnTypeInt64, ColumnTypeUInt64:
			columnSize = 64 / 8
		case ColumnTypeInt32, ColumnTypeUInt32:
			columnSize = 32 / 8
		case ColumnTypeInt16, ColumnTypeUInt16:
			columnSize = 16 / 8
		case ColumnTypeInt8, ColumnTypeUInt8:
			columnSize = 1
		default:
			return errors.Errorf("unknown type %q", columnType)
		}

		full := columnSize * b.Rows
		data, err := r.ReadRaw(full)
		if err != nil {
			return errors.Wrap(err, "raw column")
		}

		b.Data = append(b.Data, RawColumn{
			Name: columnName,
			Type: ColumnType(columnType),
			Data: data,
		})
	}

	return nil
}
