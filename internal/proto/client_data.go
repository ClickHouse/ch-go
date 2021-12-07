package proto

import (
	"fmt"
	"strings"

	"github.com/go-faster/errors"
)

type RawColumn struct {
	Name string
	Type ColumnType
	Data []byte
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

	// TODO: Write columns and rows data
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

type ClientData struct {
	TableName string
	Block     Block
}

func (c ClientData) EncodeAware(b *Buffer, revision int) {
	ClientCodeData.Encode(b)
	if FeatureTempTables.In(revision) {
		b.PutString(c.TableName)
	}
	c.Block.EncodeAware(b, revision)
}
