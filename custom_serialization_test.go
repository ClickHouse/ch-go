package ch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
)

// Fixtures follow ClickHouse's SerializationSparse and SerializationReplicated
// layouts. Production columns can implement the same hook without materializing
// an ordinary Native column first.
type customNativeColumn struct {
	kind    byte
	typ     proto.ColumnType
	rows    int
	payload []byte
}

func (c customNativeColumn) Type() proto.ColumnType { return c.typ }
func (c customNativeColumn) Rows() int              { return c.rows }
func (c customNativeColumn) EncodeSerialization(b *proto.Buffer, _ int) error {
	b.PutByte(c.kind)
	return nil
}
func (c customNativeColumn) EncodeColumn(b *proto.Buffer) { b.PutRaw(c.payload) }
func (c customNativeColumn) WriteColumn(w *proto.Writer)  { w.ChainWrite(c.payload) }

func sparseNativeFixture(allDefault bool) customNativeColumn {
	var payload proto.Buffer
	if !allDefault {
		payload.PutUVarInt(3)             // Three defaults, then 7.
		payload.PutUVarInt(2)             // Two defaults, then 9.
		payload.PutUVarInt((1 << 62) | 1) // One trailing default.
		payload.PutUInt64(7)
		payload.PutUInt64(9)
	} else {
		payload.PutUVarInt((1 << 62) | 8)
	}
	return customNativeColumn{kind: 1, typ: proto.ColumnTypeUInt64, rows: 8, payload: payload.Buf}
}

func replicatedNativeFixture() customNativeColumn {
	var payload proto.Buffer
	payload.PutUVarInt(8)
	payload.PutByte(1) // UInt8 indexes.
	payload.PutRaw([]byte{0, 1, 0, 1, 1, 0, 0, 1})
	payload.PutUVarInt(2)
	payload.PutUInt64(7)
	payload.PutUInt64(9)
	return customNativeColumn{kind: 4, typ: proto.ColumnTypeUInt64, rows: 8, payload: payload.Buf}
}

func TestPreparedCustomSerialization(t *testing.T) {
	for _, column := range []customNativeColumn{sparseNativeFixture(false), sparseNativeFixture(true), replicatedNativeFixture()} {
		for _, method := range []compress.Method{compress.None, compress.LZ4, compress.ZSTD} {
			t.Run(fmt.Sprintf("kind=%d/method=%s/bytes=%d", column.kind, method, len(column.payload)), func(t *testing.T) {
				input := proto.Input{{Name: "v", Data: column}}
				var expected proto.Buffer
				require.NoError(t, (proto.Block{Rows: column.rows, Columns: 1, Info: proto.BlockInfo{BucketNum: -1}}).EncodeBlock(&expected, proto.Version, input))
				encoder := testBlockEncoder(method, method != compress.None)
				prepared, err := encoder.Encode(t.Context(), input)
				require.NoError(t, err)
				data := prepared.data
				if method != compress.None {
					data, err = io.ReadAll(io.LimitReader(compress.NewReader(bytes.NewReader(data)), int64(len(expected.Buf))))
					require.NoError(t, err)
				}
				require.Equal(t, expected.Buf, data)
				encoder.version = proto.FeatureCustomSerialization.Version() - 1
				_, err = encoder.Encode(t.Context(), input)
				require.ErrorContains(t, err, "requires custom serialization")
			})
		}
	}
}

func TestClientCustomSerialization(t *testing.T) {
	for _, compression := range []Compression{CompressionDisabled, CompressionLZ4, CompressionZSTD} {
		t.Run(compression.String(), func(t *testing.T) {
			// The async queue appends compact columns into ordinary columns without
			// materializing them. See the server path linked in README's Custom Native serialization section.
			client := ConnOpt(t, Options{
				Compression: compression,
				Settings:    []Setting{{Key: "async_insert", Value: "0", Important: true}},
			})
			SkipNoFeature(t, client, proto.FeatureCustomSerialization)
			fixtures := []struct {
				name   string
				column customNativeColumn
				want   proto.ColUInt64
			}{
				{"sparse", sparseNativeFixture(false), proto.ColUInt64{0, 0, 0, 7, 0, 0, 9, 0}},
				{"all_default", sparseNativeFixture(true), proto.ColUInt64{0, 0, 0, 0, 0, 0, 0, 0}},
				{"replicated", replicatedNativeFixture(), proto.ColUInt64{7, 9, 7, 9, 9, 7, 7, 9}},
			}
			for _, fixture := range fixtures {
				t.Run(fixture.name, func(t *testing.T) {
					if fixture.column.kind == 4 && client.ServerInfo().Revision < 54482 {
						t.Skip("server predates replicated serialization")
					}
					require.NoError(t, client.Do(t.Context(), Query{Body: "CREATE TABLE " + fixture.name + " (id UInt64, v UInt64) ENGINE=Memory"}))
					ids := proto.ColUInt64{0, 1, 2, 3, 4, 5, 6, 7}
					input := proto.Input{{Name: "id", Data: ids}, {Name: "v", Data: fixture.column}}
					body := "INSERT INTO " + fixture.name + " VALUES"
					require.NoError(t, client.Do(t.Context(), Query{Body: body, Input: input}))
					encoder := client.NewBlockEncoder()
					prepared, err := encoder.Encode(t.Context(), input)
					require.NoError(t, err)
					sent := false
					require.NoError(t, client.Do(t.Context(), Query{Body: body, OnPreparedInput: func(context.Context) (*PreparedBlock, error) {
						if sent {
							return nil, io.EOF
						}
						sent = true
						return prepared, nil
					}}))
					var actual proto.ColUInt64
					require.NoError(t, client.Do(t.Context(), Query{Body: "SELECT v FROM " + fixture.name + " ORDER BY id", Result: proto.Results{{Name: "v", Data: &actual}}}))
					var expected proto.ColUInt64
					for _, value := range fixture.want {
						expected = append(expected, value, value)
					}
					require.Equal(t, expected, actual)
				})
			}
		})
	}
}
