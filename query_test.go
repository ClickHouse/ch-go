package ch

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"inet.af/netaddr"

	"github.com/go-faster/ch/proto"
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
	t.Run("InsertStream", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		// Create table, no data fetch.
		createTable := Query{
			Body: "CREATE TABLE test_table (id UInt8) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

		const (
			blocks = 5 // total blocks
			size   = 4 // rows in single blocks
		)
		var (
			data  proto.ColUInt8
			total int
		)
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "id", Data: &data},
			},
			OnInput: func(ctx context.Context) error {
				data = append(data[:0], uint8(total), 2, 3, 4)
				total++
				if total > blocks {
					return io.EOF
				}
				return nil
			},
		}
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		var (
			gotTotal int
			gotData  proto.ColUInt8
		)
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: []proto.ResultColumn{
				{Name: "id", Data: &gotData},
			},
			OnResult: func(ctx context.Context, b proto.Block) error {
				gotTotal += len(gotData)
				return nil
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Equal(t, blocks*size, gotTotal)
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
	t.Run("SelectIPv4", func(t *testing.T) {
		t.Parallel()
		var data proto.ColIPv4
		selectArr := Query{
			Body: "SELECT toIPv4('127.1.1.5') AS ip",
			Result: []proto.ResultColumn{
				{
					Name: "ip",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectArr))
		require.Equal(t, 1, data.Rows())
		t.Logf("%v %s", data[0], data[0].ToIP())
		require.Equal(t, netaddr.MustParseIP("127.1.1.5"), data[0].ToIP())
	})
	t.Run("SelectIPv6", func(t *testing.T) {
		t.Parallel()
		var data proto.ColIPv6
		selectArr := Query{
			Body: "SELECT toIPv6('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D') AS ip",
			Result: []proto.ResultColumn{
				{
					Name: "ip",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectArr))
		require.Equal(t, 1, data.Rows())
		t.Logf("%v %s", data[0], data[0].ToIP())
		expected := netaddr.MustParseIP("2001:db8:ac10:fe01:feed:babe:cafe:f00d")
		require.Equal(t, expected, data[0].ToIP())
	})
	t.Run("SelectDateTime", func(t *testing.T) {
		t.Parallel()
		const (
			tz = "Europe/Moscow"
			dt = "2019-01-01 00:00:00"
		)
		var data proto.ColDateTime
		selectArr := Query{
			Body: fmt.Sprintf("SELECT toDateTime('%s', '%s') as time", dt, tz),
			Result: []proto.ResultColumn{
				{
					Name: "time",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Query(ctx, selectArr))
		require.Equal(t, 1, data.Rows())
		loc, err := time.LoadLocation(tz)
		require.NoError(t, err)
		exp, err := time.ParseInLocation("2006-01-02 15:04:05", dt, loc)
		v := data[0].Time().In(loc)
		require.NoError(t, err)
		require.True(t, exp.Equal(v))
		t.Logf("%s %d", v, v.Unix())
	})
	t.Run("InsertDateTime", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		// Create table, no data fetch.
		createTable := Query{
			Body: "CREATE TABLE test_table (d DateTime) ENGINE = MergeTree ORDER BY d",
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

		data := proto.ColDateTime{1546290000}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "d", Data: &data},
			},
		}
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		var gotData proto.ColDateTime
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: []proto.ResultColumn{
				{Name: "d", Data: &gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Len(t, data, 1)
		require.Equal(t, data, gotData)
	})
	t.Run("InsertDateTime64", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		p := proto.PrecisionNano
		createTable := Query{
			Body: fmt.Sprintf("CREATE TABLE test_table (d DateTime64(%d)) ENGINE = MergeTree ORDER BY d", p),
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

		data := proto.ColDateTime64{
			proto.DateTime64(time.Unix(1546290000, 0).UnixNano()),
		}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "d", Data: data.Wrap(p)},
			},
		}
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		var gotData proto.ColDateTime64
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: []proto.ResultColumn{
				{Name: "d", Data: &gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Len(t, data, 1)
		require.Equal(t, data, gotData)
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
			OnResult: func(ctx context.Context, b proto.Block) error {
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

func TestClientCompression(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := func(t *testing.T) *Client {
		return ConnOpt(t, Options{
			Compression: CompressionLZ4,
		})
	}
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
		require.NoError(t, conn(t).Query(ctx, selectStr))
		require.Equal(t, 1, data.Rows())
		require.Equal(t, "foo", data.First())
	})
}
