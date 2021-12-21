package ch

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
			Result: proto.Results{
				{Name: "id", Data: &gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Len(t, data, 4)
		require.Equal(t, data, gotData)
	})
	t.Run("InsertTuple", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		createTable := Query{
			Body: "CREATE TABLE test_table (v Tuple(String, Int64)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

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
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		gotData := proto.ColTuple{new(proto.ColStr), new(proto.ColInt64)}
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Equal(t, rows, gotData.Rows())
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
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
			Result: proto.Results{
				{Name: "d", Data: &gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Len(t, data, 1)
		require.Equal(t, data, gotData)
	})
	t.Run("InsertLowCardinalityString", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		// Create table, no data fetch.
		createTable := Query{
			Body: "CREATE TABLE test_table (v LowCardinality(String)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

		s := &proto.ColStr{}
		data := proto.ColLowCardinality{
			Key:   proto.KeyUInt8,
			Keys8: proto.ColUInt8{0, 1, 0, 1, 0, 1, 1, 1, 0, 0},
			Index: s,
		}
		s.Append("One")
		s.Append("Two")

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: &data},
			},
		}
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		var (
			gotIndex = &proto.ColStr{}
			gotData  = &proto.ColLowCardinality{
				Index: gotIndex,
			}
		)
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Equal(t, data.Rows(), gotData.Rows())
		require.Equal(t, data.Key, gotData.Key)

		expected := []string{
			"One", "Two", "One", "Two", "One", "Two", "Two", "Two", "One", "One",
		}
		for i, j := range gotData.Keys8 {
			got := gotIndex.Row(int(j))
			assert.Equal(t, expected[i], got, "[%d]", i)
		}
	})
	t.Run("InsertMapStringString", func(t *testing.T) {
		t.Parallel()
		conn := Conn(t)

		// Create table, no data fetch.
		createTable := Query{
			Body: "CREATE TABLE test_table (v Map(String, String)) ENGINE = TinyLog",
		}
		require.NoError(t, conn.Query(ctx, createTable), "create table")

		var (
			keys   = &proto.ColStr{}
			values = &proto.ColStr{}
			data   = &proto.ColMap{
				Keys:   keys,
				Values: values,
			}
		)

		for _, v := range []struct {
			Key, Value string
		}{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
			{Key: "key3", Value: "value3"},
		} {
			keys.Append(v.Key)
			values.Append(v.Value)
		}
		data.Offsets = proto.ColUInt64{
			2, // [0:2]
			3, // [2:3]
		}

		insertQuery := Query{
			Body: "INSERT INTO test_table VALUES",
			Input: []proto.InputColumn{
				{Name: "v", Data: data},
			},
		}
		require.NoError(t, conn.Query(ctx, insertQuery), "insert")

		var (
			gotKeys   = &proto.ColStr{}
			gotValues = &proto.ColStr{}
			gotData   = &proto.ColMap{
				Keys:   gotKeys,
				Values: gotValues,
			}
		)
		selectData := Query{
			Body: "SELECT * FROM test_table",
			Result: proto.Results{
				{Name: "v", Data: gotData},
			},
		}
		require.NoError(t, conn.Query(ctx, selectData), "select")
		require.Equal(t, data.Rows(), gotData.Rows())
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
			Result: proto.Results{
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
				require.NoError(t, conn(t).Query(ctx, selectStr))
				require.Equal(t, 1, data.Rows())
				require.Equal(t, "foo", data.First())
			})
			t.Run("Insert", func(t *testing.T) {
				// Create table, no data fetch.
				t.Parallel()
				client := conn(t)
				createTable := Query{
					Body: "CREATE TABLE test_table (id UInt8) ENGINE = MergeTree ORDER BY id",
				}
				require.NoError(t, client.Query(ctx, createTable), "create table")

				data := proto.ColUInt8{1, 2, 3, 4}
				insertQuery := Query{
					Body: "INSERT INTO test_table VALUES",
					Input: []proto.InputColumn{
						{Name: "id", Data: &data},
					},
				}
				require.NoError(t, client.Query(ctx, insertQuery), "insert")

				var gotData proto.ColUInt8
				selectData := Query{
					Body: "SELECT * FROM test_table",
					Result: proto.Results{
						{Name: "id", Data: &gotData},
					},
				}
				require.NoError(t, client.Query(ctx, selectData), "select")
				require.Len(t, data, 4)
				require.Equal(t, data, gotData)
			})
		}
	}
	t.Run("LZ4", testCompression(CompressionLZ4))
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
				t.Logf("Log: %s", l.Text)
				logs++
				assert.Equal(t, qID, l.QueryID)
				return nil
			},
			Result: proto.Results{
				{
					Name: "s",
					Data: &data,
				},
			},
		}
		require.NoError(t, conn(t).Query(ctx, selectStr))
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
		require.NoError(t, Conn(t).Query(ctx, selectStr))
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
		require.NoError(t, Conn(t).Query(ctx, selectStr))
		require.Equal(t, 3, data.Rows())
	})
}

func TestClient_ServerProfile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)
	if !conn.serverInfo().Has(proto.FeatureProfileEvents) {
		t.Skip("Profile events not supported")
	}
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
	require.NoError(t, conn.Query(ctx, selectStr))
	t.Logf("%d profile(s)", profiles)
	if profiles == 0 {
		t.Fatal("No profiles")
	}
}
