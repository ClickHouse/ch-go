//go:build (amd64 || arm64 || riscv64) && !purego

package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestClient_Query_Raw(t *testing.T) {
	ctx := context.Background()
	t.Parallel()

	t.Run("InsertMapOfFixedStrArrayStr", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		createTable := Query{
			Body: "CREATE TABLE test_table (v Map(FixedString(16), Array(String))) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		type K = [16]byte
		data := &proto.ColMap[K, []string]{
			Keys:   new(proto.ColRawOf[K]),
			Values: new(proto.ColStr).Array(),
		}
		data.AppendArr([]map[K][]string{
			{
				K{1: 123, 2: 4}: []string{"Hello", "World"},
			},
			{
				K{1: 124, 2: 3}: []string{"Hi"},
				K{1: 124, 2: 1}: []string{"Hello"},
			},
		})

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		gotData := &proto.ColMap[K, []string]{
			Keys:   new(proto.ColRawOf[K]),
			Values: new(proto.ColStr).Array(),
		}
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, data.Rows(), gotData.Rows())
		for i := 0; i < data.Rows(); i++ {
			require.Equal(t, data.Row(i), gotData.Row(i))
		}
	})
}
