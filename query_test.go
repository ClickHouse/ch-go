package ch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/proto"
)

func requireEqual[T any](t *testing.T, a, b proto.ColumnOf[T]) {
	t.Helper()
	require.Equal(t, a.Rows(), b.Rows(), "rows count should match")
	for i := 0; i < a.Rows(); i++ {
		require.Equalf(t, a.Row(i), b.Row(i), "[%d]", i)
	}
}

func TestWithTotals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)
	var n proto.ColUInt64
	var c proto.ColUInt64

	var data []uint64
	query := Query{
		Body: `
			SELECT
				number AS n,
				COUNT() AS c
			FROM (
				SELECT number FROM system.numbers LIMIT 100
			) GROUP BY n WITH TOTALS
		`,
		Result: proto.Results{
			{Name: "n", Data: &n},
			{Name: "c", Data: &c},
		},
		OnResult: func(ctx context.Context, b proto.Block) error {
			data = append(data, c...)
			return nil
		},
	}
	require.NoError(t, conn.Do(ctx, query))
	require.Equal(t, 101, len(data))
	require.Equal(t, uint64(100), data[100])
}

func TestDateTimeOverflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)
	var data proto.ColDateTime
	query := Query{
		Body: "SELECT toDateTime('2061-02-01 00:00:00') as v",
		Result: proto.Results{
			{Name: "v", Data: &data},
		},
	}
	require.NoError(t, conn.Do(ctx, query))
	require.Equal(t, 1, data.Rows())
	require.Equal(t, "2061-02-01 00:00:00", data.Row(0).Format("2006-01-02 15:04:05"))
}

func TestProtoVersion(t *testing.T) {
	t.Skip("Long")
	t.Parallel()
	for ver := 54451; ver <= proto.Version; ver++ {
		v := ver
		t.Run(fmt.Sprintf("%d", v), func(t *testing.T) {
			t.Parallel()
			t.Run("Select", func(t *testing.T) {
				ctx := context.Background()
				conn := ConnOpt(t, Options{
					ProtocolVersion: v,
				})
				var data proto.ColUInt8
				query := Query{
					Body: "SELECT 1",
					Result: proto.Results{
						{Name: "1", Data: &data},
					},
				}
				require.NoError(t, conn.Do(ctx, query))
				require.Equal(t, 1, data.Rows())
			})
			t.Run("Insert", func(t *testing.T) {
				ctx := context.Background()
				conn := ConnOpt(t, Options{
					ProtocolVersion: v,
				})
				require.NoError(t, conn.Do(ctx, Query{
					Body: "CREATE TABLE IF NOT EXISTS test_table (id UInt64) ENGINE = Null",
				}))
				query := Query{
					Body: "INSERT INTO test_table VALUES",
					Input: proto.Input{
						{Name: "id", Data: proto.ColUInt64{1, 2, 3, 4}},
					},
				}
				require.NoError(t, conn.Do(ctx, query))
			})
		})
	}
}

func TestClient_Query(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	t.Run("Insert", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (id UInt8) ENGINE = MergeTree ORDER BY id",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := proto.ColUInt8{1, 2, 3, 4}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "id", Data: &data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var gotData proto.ColUInt8
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "id", Data: &gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Len(t, data, 4)
		require.Equal(t, data, gotData)
	})
	t.Run("InsertHelper", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (id UInt8) ENGINE = MergeTree ORDER BY id",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := proto.ColUInt8{1, 2, 3, 4}
		input := proto.Input{
			{Name: "id", Data: &data},
		}
		insertQuery := Query{
			Body:  input.Into("test_table"),
			Input: input,
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var gotData proto.ColUInt8
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "id", Data: &gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Len(t, data, 4)
		require.Equal(t, data, gotData)
	})
	t.Run("InsertEnum8", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (v Enum8('foo' = 1, 'bar' = 2)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := proto.ColEnum{
			Values: []string{"foo", "bar"},
		}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: &data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var gotData proto.ColEnum
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: &gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, data.Values, gotData.Values)
		t.Log(gotData.Values)
	})
	t.Run("InsertEnum16", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (v Enum16('foo' = 1, 'bar' = 2)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := proto.ColEnum{
			Values: []string{"foo", "bar"},
		}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: &data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var gotData proto.ColEnum
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: &gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, data.Values, gotData.Values)
		t.Log(gotData.Values)
	})
	t.Run("InsertTuple", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (v Tuple(String, Int64)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		const rows = 50
		var (
			dataStr proto.ColStr
			dataInt proto.ColInt64
		)
		for i := 0; i < rows; i++ {
			dataStr.Append(fmt.Sprintf("<%d>", i))
			dataInt = append(dataInt, int64(i))
		}

		data := proto.ColTuple{&dataStr, &dataInt}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: &data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		gotData := proto.ColTuple{new(proto.ColStr), new(proto.ColInt64)}
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, rows, gotData.Rows())
		require.Equal(t, data, gotData)
	})
	t.Run("InsertStream", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (id UInt8) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

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
				if total >= blocks {
					return io.EOF
				}
				return nil
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var (
			gotTotal int
			gotData  proto.ColUInt8
		)
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "id", Data: &gotData},
			},
			OnResult: func(ctx context.Context, b proto.Block) error {
				gotTotal += len(gotData)
				return nil
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, blocks*size, gotTotal)
	})
	t.Run("InsertArr", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		require.NoError(t, conn.Do(ctx, Query{
			Body: "CREATE TABLE test_array_table (id UInt8, v Array(String)) ENGINE = MergeTree ORDER BY id",
		}), "create table")

		values := [][]string{
			{"foo", "bar", "Baz"},
			{"Hello", "World!"},
			{"ClickHouse", "", "Goes", "", "Fasta!"},
		}

		arr := new(proto.ColStr).Array()
		for _, v := range values {
			arr.Append(v)
		}

		insertArr := Query{
			Body: "INSERT INTO test_array_table VALUES",
			Input: []proto.InputColumn{
				{Name: "id", Data: proto.ColUInt8{1, 2, 3}},
				{Name: "v", Data: arr},
			},
		}
		require.NoError(t, conn.Do(ctx, insertArr), "insert")

		gotArr := new(proto.ColStr).Array()
		selectArr := Query{
			Body: "SELECT v FROM test_array_table",
			Result: proto.Results{
				{Name: "v", Data: gotArr},
			},
		}
		require.NoError(t, conn.Do(ctx, selectArr), "select")
		requireEqual[[]string](t, arr, gotArr)
		require.Equal(t, arr.Offsets, gotArr.Offsets)
	})
	t.Run("SelectOne", func(t *testing.T) {
		t.Parallel()
		// Select single row.
		var data proto.ColUInt8
		selectOne := Query{
			Body: "SELECT 1 AS one",
			Result: proto.Results{
				{
					Name: "one",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectOne))
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
			Result: proto.Results{
				{Name: "signed", Data: &signed},
				{Name: "unsigned", Data: &unsigned},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectOne))
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
		err := Conn(t).Do(ctx, drop)
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
		require.NoError(t, Conn(t).Do(ctx, Query{
			Body: "SELECT 'foo' AS s",
			Result: proto.Results{
				{
					Name: "s",
					Data: &data,
				},
			},
		}))
		require.Equal(t, 1, data.Rows())
		require.Equal(t, "foo", data.First())
	})
	t.Run("SelectArr", func(t *testing.T) {
		t.Parallel()
		arr := new(proto.ColUInt8).Array()
		require.NoError(t, Conn(t).Do(ctx, Query{
			Body: "SELECT [1, 2, 3] AS arr",
			Result: proto.Results{
				{Name: "arr", Data: arr},
			},
		}))
		require.Equal(t, 1, arr.Rows())
		require.Equal(t, 3, len(arr.Row(0)))
		require.Equal(t, []uint8{1, 2, 3}, arr.Row(0))
	})
	t.Run("SelectIPv4", func(t *testing.T) {
		t.Parallel()
		var data proto.ColIPv4
		require.NoError(t, Conn(t).Do(ctx, Query{
			Body: "SELECT toIPv4('127.1.1.5') AS ip",
			Result: proto.Results{
				{Name: "ip", Data: &data},
			},
		}))
		require.Equal(t, 1, data.Rows())
		t.Logf("%v %s", data[0], data[0].ToIP())
		require.Equal(t, netip.MustParseAddr("127.1.1.5"), data[0].ToIP())
	})
	t.Run("SelectIPv6", func(t *testing.T) {
		t.Parallel()
		var data proto.ColIPv6
		selectArr := Query{
			Body: "SELECT toIPv6('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D') AS ip",
			Result: proto.Results{
				{Name: "ip", Data: &data},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectArr))
		require.Equal(t, 1, data.Rows())
		t.Logf("%v %s", data[0], data[0].ToIP())
		expected := netip.MustParseAddr("2001:db8:ac10:fe01:feed:babe:cafe:f00d")
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
			Result: proto.Results{
				{Name: "time", Data: &data},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectArr))
		require.Equal(t, 1, data.Rows())
		loc, err := time.LoadLocation(tz)
		require.NoError(t, err)
		exp, err := time.ParseInLocation("2006-01-02 15:04:05", dt, loc)
		v := data.Row(0).In(loc)
		require.NoError(t, err)
		require.True(t, exp.Equal(v))
		t.Logf("%s %d", v, v.Unix())
	})
	t.Run("UUID", func(t *testing.T) {
		t.Parallel()
		v := uuid.MustParse(`9e1cf0cf-4b82-4237-a6ed-6ad549907fb0`)
		var data proto.ColUUID
		require.NoError(t, Conn(t).Do(ctx, Query{
			Body: fmt.Sprintf(`SELECT '%s'::UUID as v`, v),
			Result: proto.Results{
				{Name: "v", Data: &data},
			},
		}))
		require.Equal(t, 1, data.Rows())
		require.Equal(t, v, data[0])
	})
	t.Run("IPv4", func(t *testing.T) {
		t.Parallel()
		var data proto.ColIPv4
		require.NoError(t, Conn(t).Do(ctx, Query{
			Body: `SELECT toIPv4('10.10.0.1') as v`,
			Result: proto.Results{
				{Name: "v", Data: &data},
			},
		}))
		require.Equal(t, 1, data.Rows())
		require.Equal(t, netip.MustParseAddr("10.10.0.1"), data[0].ToIP())
	})
	t.Run("InsertDateTime", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		createTable := Query{
			Body: "CREATE TABLE test_table (d DateTime) ENGINE = MergeTree ORDER BY d",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := proto.ColDateTime{Data: []proto.DateTime{1546290000}}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "d", Data: &data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var gotData proto.ColDateTime
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "d", Data: &gotData},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, 1, data.Rows())
		require.Equal(t, data, gotData)
	})
	t.Run("InsertDateTime64", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		p := proto.PrecisionNano
		createTable := Query{
			Body: fmt.Sprintf("CREATE TABLE test_table (d DateTime64(%d)) ENGINE = MergeTree ORDER BY d", p),
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := proto.ColDateTime64{
			Data: []proto.DateTime64{
				proto.DateTime64(time.Unix(1546290000, 0).UnixNano()),
			},
		}
		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "d", Data: data.WithPrecision(p)},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		t.Run("Read", func(t *testing.T) {
			var gotData proto.ColDateTime64
			selectData := Query{
				Body: "SELECT * FROM test_table",
				Result: proto.Results{
					{Name: "d", Data: &gotData},
				},
			}
			require.NoError(t, conn.Do(ctx, selectData), "select")
			require.Equal(t, 1, data.Rows())
			require.Equal(t, data, gotData)
		})
	})
	t.Run("ArrayFixedStr", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		require.NoError(t, conn.Do(ctx, Query{
			Body: "CREATE TABLE test_table (v Array(FixedString(10))) ENGINE = Memory",
		}), "create table")

		v := (&proto.ColFixedStr{Size: 10}).Array()

		v.Append([][]byte{
			bytes.Repeat([]byte("a"), 10),
			bytes.Repeat([]byte("b"), 10),
			bytes.Repeat([]byte("c"), 10),
		})
		v.Append([][]byte{
			bytes.Repeat([]byte("d"), 10),
			bytes.Repeat([]byte("e"), 10),
			bytes.Repeat([]byte("f"), 10),
		})

		require.NoError(t, conn.Do(ctx, Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: v},
			},
		}), "insert")
	})
	t.Run("ArrayLowCardinality", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		require.NoError(t, conn.Do(ctx, Query{
			Body: "CREATE TABLE test_table (v Array(LowCardinality(String))) ENGINE = Memory",
		}), "create table")

		v := new(proto.ColStr).LowCardinality().Array()
		v.Append([]string{"foo", "bar"})
		v.Append([]string{"baz"})

		require.NoError(t, conn.Do(ctx, Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: v},
			},
		}), "insert")

		gotData := new(proto.ColStr).LowCardinality().Array()
		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}), "select")

		assert.Equal(t, []string{"foo", "bar"}, gotData.Row(0))
		assert.Equal(t, []string{"baz"}, gotData.Row(1))
	})
	t.Run("InsertLowCardinalityString", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		createTable := Query{
			Body: "CREATE TABLE test_table (v LowCardinality(String)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		expected := []string{
			"One", "Two", "One", "Two", "One", "Two", "Two", "Two", "One", "One",
		}
		data := new(proto.ColStr).LowCardinality()
		data.AppendArr(expected)

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		gotData := new(proto.ColStr).LowCardinality()
		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}), "select")
		requireEqual[string](t, data, gotData)
	})
	t.Run("InsertArrayLowCardinalityString", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		require.NoError(t, conn.Do(ctx, Query{
			Body: "CREATE TABLE test_table (v Array(LowCardinality(String))) ENGINE = Memory",
		}), "create table")

		data := [][]string{
			{"foo", "bar", "baz"},
			{"foo"},
			{"bar", "bar"},
			{"foo", "foo"},
			{"bar", "bar", "bar", "bar"},
		}
		col := new(proto.ColStr).LowCardinality().Array()
		for _, v := range data {
			col.Append(v)
		}

		require.NoError(t, conn.Do(ctx, Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: col},
			},
		}), "insert")

		gotData := new(proto.ColStr).LowCardinality().Array()
		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}), "select")
		requireEqual[[]string](t, col, gotData)
	})
	t.Run("SelectArray", func(t *testing.T) {
		t.Parallel()
		arr := new(proto.ColUInt8).Array()
		selectArr := Query{
			Body: "SELECT [1, 2, 3, 4]::Array(UInt8) as v",
			Result: proto.Results{
				{Name: "v", Data: arr},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectArr))
		require.Equal(t, []uint8{1, 2, 3, 4}, arr.Row(0))
	})
	t.Run("SelectArrayOf", func(t *testing.T) {
		t.Parallel()
		arr := new(proto.ColStr).Array()
		selectArr := Query{
			Body:   "SELECT ['foo', 'bar', 'baz']::Array(String) as v",
			Result: arr.Results("v"),
		}
		require.NoError(t, Conn(t).Do(ctx, selectArr))
		require.Equal(t, 1, arr.Rows())
		require.Equal(t, []string{"foo", "bar", "baz"}, arr.Row(0))
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
			Result: proto.Results{
				{
					Name: "v",
					Data: &data,
				},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectRand))
		require.Equal(t, numbers, total)
	})
	t.Run("InsertMapOfStringInt64", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		createTable := Query{
			Body: "CREATE TABLE test_table (v Map(String, Int64)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := &proto.ColMap[string, int64]{
			Keys:   new(proto.ColStr),
			Values: new(proto.ColInt64),
		}
		data.AppendArr([]map[string]int64{
			{
				"foo": 1,
				"bar": 100,
			},
			{
				"clickhouse_1": -7,
				"clickhouse_2": 130,
				"clickhouse_3": 110,
			},
		})

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		gotData := &proto.ColMap[string, int64]{
			Keys:   new(proto.ColStr),
			Values: new(proto.ColInt64),
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
	t.Run("InsertNullableString", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		createTable := Query{
			Body: "CREATE TABLE test_table (v Nullable(String)) ENGINE = Memory",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		data := &proto.ColNullable[string]{
			Values: new(proto.ColStr),
		}
		data.AppendArr([]proto.Nullable[string]{
			proto.Null[string](),
			proto.NewNullable("hello"),
			proto.NewNullable("world"),
			proto.Null[string](),
			proto.Null[string](),
			proto.NewNullable("end"),
		})

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: data},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		gotData := &proto.ColNullable[string]{
			Values: new(proto.ColStr),
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
	t.Run("InsertDecimal32", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)
		createTable := Query{
			Body: "CREATE TABLE test_table (v Decimal32(2)) ENGINE = Memory",
		}
		require.NoError(t, conn.Do(ctx, createTable), "create table")

		var data proto.ColDecimal32
		data.Append(1234)
		data.Append(5678)

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: proto.Alias(&data, "Decimal(9, 2)")},
			},
		}
		require.NoError(t, conn.Do(ctx, insertQuery), "insert")

		var gotData proto.ColDecimal32
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: proto.Alias(&gotData, "Decimal(9, 2)")},
			},
		}
		require.NoError(t, conn.Do(ctx, selectData), "select")
		require.Equal(t, data.Rows(), gotData.Rows())
		for i := 0; i < data.Rows(); i++ {
			require.Equal(t, data.Row(i), gotData.Row(i))
		}
	})
	t.Run("InsertGeoPoint", func(t *testing.T) {
		t.Parallel()
		conn := ConnOpt(t, Options{
			Settings: []Setting{
				// https://clickhouse.com/docs/en/sql-reference/data-types/geo/
				SettingInt("allow_experimental_geo_types", 1),
			},
		})
		require.NoError(t, conn.Do(ctx, Query{
			Body: "CREATE TABLE test_table (v Point) ENGINE = Memory",
		}), "create table")

		data := new(proto.ColPoint)
		data.AppendArr([]proto.Point{
			{X: 1, Y: 0},
			{X: 0, Y: 1},
			{X: 1, Y: 1},
			{X: 0, Y: 0},
			{X: 0, Y: -1},
		})
		require.NoError(t, conn.Do(ctx, Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: data},
			},
		}), "insert")

		gotData := new(proto.ColPoint)
		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}), "select")

		require.Equal(t, data.Rows(), gotData.Rows())
		for i := 0; i < data.Rows(); i++ {
			require.Equal(t, data.Row(i), gotData.Row(i))
		}
	})
	t.Run("SelectInterval", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		data := new(proto.ColInterval)
		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT toIntervalWeek(1) AS w",
			Result: proto.Results{
				{Name: "w", Data: data},
			},
		}), "select table")
		require.Equal(t, proto.Interval{Scale: proto.IntervalWeek, Value: 1}, data.Row(0))
	})
	t.Run("SelectNothing", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		data := proto.NewColNullable[proto.Nothing](new(proto.ColNothing))
		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT NULL as w",
			Result: proto.Results{
				{Name: "w", Data: data},
			},
		}), "select table")
		require.False(t, data.Row(0).Set)
	})
	t.Run("NotUTF8", func(t *testing.T) {
		// https://github.com/ClickHouse/ch-go/issues/226
		t.Parallel()
		conn := Conn(t)
		data := &proto.ColUInt8{}

		err := conn.Do(ctx, Query{
			Body: "SELECT 错误 as w",
			Result: proto.Results{
				{Name: "w", Data: data},
			},
		})
		require.True(t, IsErr(err, proto.ErrSyntaxError), "%v", err)
		require.Equal(t, 0, data.Rows())

		require.NoError(t, conn.Do(ctx, Query{
			Body: "SELECT 1 as w",
			Result: proto.Results{
				{Name: "w", Data: data},
			},
		}), "select table")
	})
}

func TestClientCompression(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	testCompression := func(c Compression) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()
			conn := func(t *testing.T) *Client {
				return ConnOpt(t, Options{
					Compression: c,
					Settings: []Setting{
						{
							Important: true,
							Key:       "network_compression_method",
							Value:     c.String(),
						},
					},
				})
			}
			t.Run("SelectStr", func(t *testing.T) {
				t.Parallel()
				// Select single string row.
				var data proto.ColStr
				selectStr := Query{
					Body: "SELECT 'foo' AS s",
					Result: proto.Results{
						{
							Name: "s",
							Data: &data,
						},
					},
				}
				require.NoError(t, conn(t).Do(ctx, selectStr))
				require.Equal(t, 1, data.Rows())
				require.Equal(t, "foo", data.First())
			})
			t.Run("Insert", func(t *testing.T) {
				t.Parallel()
				client := conn(t)
				createTable := Query{
					Body: "CREATE TABLE test_table (id UInt8) ENGINE = MergeTree ORDER BY id",
				}
				require.NoError(t, client.Do(ctx, createTable), "create table")

				data := proto.ColUInt8{1, 2, 3, 4}
				insertQuery := Query{
					Body: "INSERT INTO test_table VALUES",
					Input: []proto.InputColumn{
						{Name: "id", Data: &data},
					},
				}
				require.NoError(t, client.Do(ctx, insertQuery), "insert")

				var gotData proto.ColUInt8
				selectData := Query{
					Body: "SELECT * FROM test_table",
					Result: proto.Results{
						{Name: "id", Data: &gotData},
					},
				}
				require.NoError(t, client.Do(ctx, selectData), "select")
				require.Len(t, data, 4)
				require.Equal(t, data, gotData)
			})
			t.Run("InsertBig", func(t *testing.T) {
				t.Parallel()
				client := conn(t)
				createTable := Query{
					Body: "CREATE TABLE test_table_big (v String) ENGINE = TinyLog",
				}
				require.NoError(t, client.Do(ctx, createTable), "create table")

				data := proto.ColStr{}
				s := rand.NewSource(10)
				r := rand.New(s)
				buf := make([]byte, 1024)
				_, err := io.ReadFull(r, buf)
				require.NoError(t, err)
				for i := 0; i < 1200; i++ {
					data.AppendBytes(buf)
				}
				insertQuery := Query{
					Body: "INSERT INTO test_table_big VALUES",
					Input: []proto.InputColumn{
						{Name: "v", Data: &data},
					},
				}
				require.NoError(t, client.Do(ctx, insertQuery), "insert")
			})
		}
	}
	t.Run("LZ4", testCompression(CompressionLZ4))
	t.Run("LZ4HC", testCompression(CompressionLZ4HC))
	t.Run("ZSTD", testCompression(CompressionZSTD))
	t.Run("None", testCompression(CompressionNone))
	t.Run("Disabled", testCompression(CompressionDisabled))
}

func TestClient_ServerLog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := func(t *testing.T) *Client {
		return ConnOpt(t, Options{
			Settings: []Setting{
				{
					Key:       "send_logs_level",
					Value:     "trace",
					Important: true,
				},
			},
		})
	}
	t.Run("Log", func(t *testing.T) {
		t.Parallel()
		// Select single string row.
		var (
			data proto.ColStr
			logs int
		)
		qID := "expected-query-id"
		selectStr := Query{
			Body:    "SELECT 'foo' as s",
			QueryID: qID,
			OnLog: func(ctx context.Context, l Log) error {
				assert.Equal(t, qID, l.QueryID)
				return nil
			},
			OnLogs: func(ctx context.Context, events []Log) error {
				logs += len(events)
				for _, l := range events {
					t.Logf("Log: %s", l.Text)
					assert.Equal(t, qID, l.QueryID)
				}
				return nil
			},
			Result: proto.Results{
				{
					Name: "s",
					Data: &data,
				},
			},
		}
		require.NoError(t, conn(t).Do(ctx, selectStr))
		require.Equal(t, 1, data.Rows())
		require.Equal(t, "foo", data.First())
		if logs == 0 {
			t.Fatal("No log entries received")
		}
	})
}

func TestClient_ExternalData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("Named", func(t *testing.T) {
		t.Parallel()
		var data proto.ColInt64
		selectStr := Query{
			Body:          "SELECT * FROM external",
			ExternalTable: "external",
			ExternalData: []proto.InputColumn{
				{Name: "v", Data: proto.ColInt64{1, 2, 3}},
			},
			Result: proto.Results{
				{Name: "v", Data: &data},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectStr))
		require.Equal(t, 3, data.Rows())
	})
	t.Run("Default", func(t *testing.T) {
		t.Parallel()
		var data proto.ColInt64
		selectStr := Query{
			Body: "SELECT * FROM _data",
			ExternalData: []proto.InputColumn{
				{Name: "v", Data: proto.ColInt64{1, 2, 3}},
			},
			Result: proto.Results{
				{Name: "v", Data: &data},
			},
		}
		require.NoError(t, Conn(t).Do(ctx, selectStr))
		require.Equal(t, 3, data.Rows())
	})
}

func TestClient_ServerProfile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)
	var profiles int
	selectStr := Query{
		Body: "SELECT 1",
		OnProfile: func(ctx context.Context, p proto.Profile) error {
			profiles++
			return nil
		},
		Result: proto.Results{
			proto.AutoResult("1"),
		},
	}
	require.NoError(t, conn.Do(ctx, selectStr))
	t.Logf("%d profile(s)", profiles)
	if profiles == 0 {
		t.Fatal("No profiles")
	}
}

func TestClient_ServerProfileEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)
	if !conn.ServerInfo().Has(proto.FeatureProfileEvents) {
		t.Skip("Profile events not supported")
	}
	var (
		events      int
		eventsBatch int
	)
	selectStr := Query{
		Body: "SELECT 1",
		OnProfileEvent: func(ctx context.Context, p ProfileEvent) error {
			// Deprecated.
			// TODO: remove
			events++
			return nil
		},
		OnProfileEvents: func(ctx context.Context, e []ProfileEvent) error {
			eventsBatch += len(e)
			return nil
		},
		Result: proto.Results{
			proto.AutoResult("1"),
		},
	}
	require.NoError(t, conn.Do(ctx, selectStr))
	t.Logf("%d profile event(s)", events)
	if events == 0 {
		t.Fatal("No profile events")
	}
	require.Equal(t, events, eventsBatch)
}

func TestClient_Query_Bool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)
	if v := conn.server.Revision; v < 54452 {
		t.Skipf("No bool support %v", v)
	}

	require.NoError(t, conn.Do(ctx, Query{
		Body: "CREATE TABLE test_table (v Bool) ENGINE = TinyLog",
	}), "create table")

	data := proto.ColBool{true, true, false, false, true}
	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO test_table VALUES",
		Input: []proto.InputColumn{
			{Name: "v", Data: &data},
		},
	}), "insert")

	var res proto.ColBool
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT v FROM test_table",
		Result: proto.ResultColumn{Data: &res},
	}), "select")
	require.Len(t, data, 5)
	require.Equal(t, data, res)
}

func BenchmarkClient_decodeBlock(b *testing.B) {
	// Encoding block.
	buf := new(proto.Buffer)
	{
		const rows = 65535
		buf.PutString("") // no temp table
		var data proto.ColUInt64
		for i := uint64(0); i < rows; i++ {
			data.Append(i)
		}
		block := proto.Block{
			Info:    proto.BlockInfo{BucketNum: -1},
			Columns: 1,
			Rows:    rows,
		}
		input := []proto.InputColumn{
			{Name: "v", Data: data},
		}
		require.NoError(b, block.EncodeBlock(buf, proto.Version, input))
	}
	var (
		br  = bytes.NewReader(buf.Buf)
		r   = proto.NewReader(br)
		ctx = context.Background()
	)
	c := &Client{
		reader:          r,
		protocolVersion: proto.Version,
		lg:              zap.NewNop(),
	}
	opt := decodeOptions{
		Handler: func(ctx context.Context, b proto.Block) error { return nil },
		Result: proto.Results{
			{Name: "v", Data: new(proto.ColUInt64)},
		},
	}

	b.ResetTimer()
	b.SetBytes(int64(len(buf.Buf)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		br.Reset(buf.Buf)
		if err := c.decodeBlock(ctx, opt); err != nil {
			b.Fatal(err)
		}
	}
}

func discardResult() proto.Result {
	return (&proto.Results{}).Auto()
}

func TestClient_ResultsAuto(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var data proto.Results
	require.NoError(t, Conn(t).Do(ctx, Query{
		Body:   "SELECT number as a, number as b FROM system.numbers LIMIT 10",
		Result: data.Auto(),
	}), "select")

	require.Len(t, data, 2)
	require.Equal(t, 10, data.Rows())
}

func TestClient_discardResult(t *testing.T) {
	t.Parallel()
	require.NoError(t, Conn(t).Do(context.Background(), Query{
		Body:   "SELECT 1",
		Result: discardResult(),
	}), "select")
}

func TestClient_ColInfoInput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var data proto.ColInfoInput
	require.NoError(t, Conn(t).Do(ctx, Query{
		Body:   "SELECT number as a, number as b FROM system.numbers LIMIT 0",
		Result: &data,
	}), "select")
	require.Len(t, data, 2)
}

func TestClient_OpenTelemetryInstrumentation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := ConnOpt(t, Options{
		OpenTelemetryInstrumentation: true,
	})
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT 1 as v",
		Result: discardResult(),
	}), "select")
}

func TestClientInsert(t *testing.T) {
	var (
		body      proto.ColStr
		name      proto.ColStr
		sevText   proto.ColEnum
		sevNumber proto.ColUInt8

		ts  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano) // DateTime64(9)
		arr = new(proto.ColStr).Array()                                   // Array(String)
		now = time.Date(2010, 1, 1, 10, 22, 33, 345678, time.UTC)
	)

	// Append 10 rows to initial data block.
	for i := 0; i < 10; i++ {
		body.AppendBytes([]byte("Hello"))
		ts.Append(now)
		name.Append("name")
		sevText.Values = append(sevText.Values, "INFO")
		sevNumber.Append(10)
		arr.Append([]string{"foo", "bar", "baz"})
	}

	ctx := context.Background()
	conn := Conn(t)
	require.NoError(t, conn.Do(ctx, Query{
		Body: `CREATE TABLE test_table
(
    ts                DateTime64(9),
    severity_text     Enum8('INFO'=1, 'DEBUG'=2),
    severity_number   UInt8,
    body              String,
    name              String,
    arr               Array(String)
) ENGINE = Memory`,
	}))

	// Insert single data block.
	input := proto.Input{
		{Name: "ts", Data: ts},
		{Name: "severity_text", Data: &sevText},
		{Name: "severity_number", Data: sevNumber},
		{Name: "body", Data: body},
		{Name: "name", Data: name},
		{Name: "arr", Data: arr},
	}
	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO test_table VALUES",
		// Or "INSERT INTO test_table (ts, severity_text, severity_number, body, name, arr) VALUES"
		Input: input,
	}))

	// Stream data.
	var blocks int
	require.NoError(t, conn.Do(ctx, Query{
		Body:  "INSERT INTO test_table VALUES",
		Input: input,
		OnInput: func(ctx context.Context) error {
			// On OnInput call, you should fill the input data.
			//
			// NB: You should reset the input columns, they are
			// not reset automatically.
			//
			// That is, we are re-using the same input columns and
			// if we will return nil without doing anything, data will be
			// just duplicated.

			input.Reset() // calls "Reset" on each column

			if blocks >= 10 {
				// Stop streaming.
				//
				// This will also write tailing input data if any,
				// but we just reset the input, so it is currently blank.
				return io.EOF
			}

			// Append new values:
			for i := 0; i < 10; i++ {
				body.AppendBytes([]byte("Hello"))
				ts.Append(now)
				name.Append("name")
				sevText.Values = append(sevText.Values, "INFO")
				sevNumber.Append(10)
				arr.Append([]string{"foo", "bar", "baz"})
			}

			blocks++

			return nil
		},
	}))
}

func TestClientQueryCancellation(t *testing.T) {
	ctx := context.Background()
	server := cht.New(t)
	c, err := Dial(ctx, Options{
		Address: server.TCP,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.Ping(ctx))

	ctx, cancel := context.WithCancel(context.Background())

	// Performing query cancellation.
	var (
		rows int
		data proto.ColUInt64
	)
	require.Error(t, c.Do(ctx, Query{
		Body:   fmt.Sprintf("SELECT number as v FROM system.numbers LIMIT %d", 2_500_000),
		Result: proto.Results{{Name: "v", Data: &data}},
		OnResult: func(_ context.Context, block proto.Block) error {
			rows += block.Rows
			if rows >= 500_000 {
				t.Log("Canceling query")
				cancel()
			}
			return nil
		},
	}))

	// Connection should be closed after query cancellation.
	require.True(t, c.IsClosed())
}
