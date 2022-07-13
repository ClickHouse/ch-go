package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func newCol[T any, C proto.ColumnOf[T]](c C, v ...T) C {
	for _, vv := range v {
		c.Append(vv)
	}
	return c
}

func TestNamedTuples(t *testing.T) {
	conn := ConnOpt(t, Options{
		Settings: []Setting{
			{
				Key:       "allow_experimental_object_type",
				Value:     "1",
				Important: true,
			},
		},
	})
	if v := conn.ServerInfo(); (v.Major < 22) || (v.Major == 22 && v.Minor < 5) {
		t.Skip("Skipping (not supported)")
	}
	ctx := context.Background()
	require.NoError(t, conn.Do(ctx, Query{
		Body: "CREATE TABLE named_tuples (`1` Tuple(`s` String, `i` Int64)) ENGINE = Memory",
	}))
	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO named_tuples VALUES",
		Input: proto.Input{
			{
				Name: "1",
				Data: proto.ColTuple{
					proto.ColNamed[string]{
						ColumnOf: newCol[string](new(proto.ColStr), "foo", "bar", "baz"),
						Name:     "s",
					},
					proto.ColNamed[int64]{
						ColumnOf: newCol[int64](new(proto.ColInt64), 1, 2, 3),
						Name:     "i",
					},
				},
			},
		},
	}))
	var (
		strData = proto.ColNamed[string]{
			ColumnOf: new(proto.ColStr),
			Name:     "s",
		}
		intData = proto.ColNamed[int64]{
			ColumnOf: new(proto.ColInt64),
			Name:     "i",
		}
	)
	require.NoError(t, conn.Do(ctx, Query{
		Body: "SELECT * FROM named_tuples",
		Result: proto.Results{
			{Name: "1", Data: proto.ColTuple{strData, intData}},
		},
	}))
}
