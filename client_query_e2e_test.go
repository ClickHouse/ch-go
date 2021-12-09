package ch

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/proto"
)

func TestClient_Query(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	t.Run("Insert", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		// Create table, no data fetch.
		createTable := Query{
			Body: "CREATE TABLE test_table (id UInt8) ENGINE = MergeTree ORDER BY id",
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

		data := proto.ColUInt8{1, 2, 3, 4}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "id", Data: &data},
			},
		}
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		var gotData proto.ColUInt8
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: []proto.ResultColumn{
				{Name: "id", Data: &gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Len(t, data, 4)
		require.Equal(t, data, gotData)
	})
	t.Run("SelectOne", func(t *testing.T) {
		t.Parallel()
		// Select single row.
		var data proto.ColUInt8
		selectOne := Query{
			Body: "SELECT 1 AS one",
			Result: []proto.ResultColumn{
				{
					Name: "one",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectOne))
		require.Len(t, data, 1)
		require.Equal(t, byte(1), data[0])
	})
	t.Run("SelectRand", func(t *testing.T) {
		t.Parallel()
		const numbers = 15_249_611
		var (
			data  proto.ColUInt32
			total int
		)
		selectRand := Query{
			Body: fmt.Sprintf("SELECT rand() as v FROM numbers(%d)", numbers),
			OnData: func(ctx context.Context) error {
				total += len(data)
				return nil
			},
			Result: []proto.ResultColumn{
				{
					Name: "v",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectRand))
		require.Equal(t, numbers, total)
	})
}
