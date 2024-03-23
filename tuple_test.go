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
		Body: "CREATE TABLE named_tuples (`1` Tuple(`s` String, `i` Int64, `m` Map(String, Float32))) ENGINE = Memory",
	}))
	const numRows = 3
	testStrs := []string{"foo", "bar", "baz"}
	testInts := []int64{1, 2, 3}
	testMaps := []map[string]float32{
		{
			"key": 42,
			"0":   100.1,
			"3":   0.0,
			"":    -34.90,
		},
		{
			// empty map
		},
		{
			"": 43,
		},
	}
	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO named_tuples VALUES",
		Input: proto.Input{
			{
				Name: "1",
				Data: proto.ColTuple{
					proto.ColNamed[string]{
						ColumnOf: newCol[string](new(proto.ColStr), testStrs...),
						Name:     "s",
					},
					proto.ColNamed[int64]{
						ColumnOf: newCol[int64](new(proto.ColInt64), testInts...),
						Name:     "i",
					},
					proto.ColNamed[map[string]float32]{
						ColumnOf: newCol[map[string]float32](
							proto.NewMap[string, float32](new(proto.ColStr), new(proto.ColFloat32)),
							testMaps...,
						),
						Name: "m",
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
		mapData = proto.Named[map[string]float32](
			proto.NewMap[string, float32](new(proto.ColStr), new(proto.ColFloat32)),
			"m",
		)
	)
	results := proto.Results{
		{Name: "1", Data: proto.ColTuple{strData, intData, mapData}},
	}
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT * FROM named_tuples",
		Result: results,
	}))

	actualStrs := getRows[string](strData, numRows)
	require.EqualValues(t, testStrs, actualStrs)

	actualInts := getRows[int64](intData, numRows)
	require.EqualValues(t, testInts, actualInts)

	actualMaps := getRows[map[string]float32](mapData, numRows)
	require.EqualValues(t, testMaps, actualMaps)
}

func getRows[T any](col proto.ColumnOf[T], numRows int) []T {
	ret := make([]T, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, col.Row(i))
	}
	return ret
}
