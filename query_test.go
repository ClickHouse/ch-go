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
	t.Run("InsertArr", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		require.NoError(t, conn.Query(ctx, Query{
			Body: "CREATE TABLE test_array_table (id UInt8, v Array(String)) ENGINE = MergeTree ORDER BY id",
		}), "create table")

		values := [][]string{
			{"foo", "bar", "Baz"},
			{"Hello", "World!"},
			{"ClickHouse", "", "Goes", "", "Fasta!"},
		}

		var data proto.ColStr
		arr := proto.ColArr{Data: &data}
		for _, v := range values {
			data.ArrAppend(&arr, v)
		}

		insertArr := Query{
			Body: "INSERT INTO test_array_table VALUES",
			Input: []proto.InputColumn{
				{Name: "id", Data: proto.ColUInt8{1, 2, 3}},
				{Name: "v", Data: &arr},
			},
		}
		require.NoError(t, conn.Query(ctx, insertArr), "insert")

		var gotData proto.ColStr
		gotArr := proto.ColArr{Data: &gotData}
		selectArr := Query{
			Body: "SELECT v FROM test_array_table",
			Result: []proto.ResultColumn{
				{Name: "v", Data: &gotArr},
			},
		}
		require.NoError(t, conn.Query(ctx, selectArr), "select")
		require.Equal(t, data, gotData)
		require.Equal(t, arr.Offsets, gotArr.Offsets)
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
	t.Run("SelectInt128", func(t *testing.T) {
		t.Parallel()
		var (
			signed   proto.ColInt128
			unsigned proto.ColUInt128
		)
		selectOne := Query{
			Body: "SELECT toInt128(-109331) as signed, toUInt128(4012) as unsigned",
			Result: []proto.ResultColumn{
				{Name: "signed", Data: &signed},
				{Name: "unsigned", Data: &unsigned},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectOne))
		require.Len(t, signed, 1)
		require.Len(t, unsigned, 1)

		expectedSigned := proto.ColInt128{proto.Int128FromInt(-109331)}
		require.Equal(t, expectedSigned, signed)
		expectedUnsigned := proto.ColUInt128{proto.UInt128FromInt(4012)}
		require.Equal(t, expectedUnsigned, unsigned)
	})
	t.Run("Exception", func(t *testing.T) {
		t.Parallel()
		drop := Query{Body: "DROP TABLE _3_"}
		err := Conn(t).Query(ctx, drop)
		ex, ok := AsException(err)
		t.Logf("%#v", ex)
		require.True(t, ok)
		require.True(t, IsException(err))
		require.True(t, IsErr(err, proto.ErrUnknownTable))
	})
	t.Run("SelectStr", func(t *testing.T) {
		t.Parallel()
		// Select single string row.
		var data proto.ColStr
		selectStr := Query{
			Body: "SELECT 'foo' AS s",
			Result: []proto.ResultColumn{
				{
					Name: "s",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectStr))
		require.Equal(t, 1, data.Rows())
		require.Equal(t, "foo", data.First())
	})
	t.Run("SelectArr", func(t *testing.T) {
		t.Parallel()
		var data proto.ColUInt8
		arr := proto.ColArr{
			Data: &data,
		}
		selectArr := Query{
			Body: "SELECT [1, 2, 3] AS arr",
			Result: []proto.ResultColumn{
				{
					Name: "arr",
					Data: &arr,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectArr))
		require.Equal(t, 1, arr.Rows())
		require.Equal(t, 3, data.Rows())
		require.Equal(t, proto.ColUInt8{1, 2, 3}, data)
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
