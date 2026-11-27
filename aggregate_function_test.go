package ch

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestClient_AggregateFunctionDDSketch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn := Conn(t)

	const (
		parameters = "(0.01, 0.1, 0.5, 0.9)"
		columnType = proto.ColumnType("AggregateFunction(quantilesDD" + parameters + ", Float64)")
	)
	// One sketch for each set of values.
	sets := []string{
		// The bin indexes are far apart: ClickHouse writes the sparse form.
		"[-1000000., -1., 1., 1000., 1000000.]",
		// The bin indexes are near: ClickHouse writes the dense form.
		"[-10.3, -10., 0., 0., 10., 10.1, 10.3, 10.5, 10.7]",
		// No negative values and no zero.
		"[5.]",
	}
	selects := make([]string, len(sets))
	for i, set := range sets {
		selects[i] = fmt.Sprintf("SELECT %d AS id, arrayJoin(%s) AS x", i, set)
	}
	values := strings.Join(selects, " UNION ALL ")

	// ClickHouse makes the states and the quantiles. ch-go decodes the states.
	var (
		ids       proto.ColUInt8
		sketches  = proto.NewAggregateFunction[proto.DDSketch](columnType)
		quantiles = new(proto.ColFloat64).Array()
	)
	require.NoError(t, conn.Do(ctx, Query{
		Body: "SELECT id, quantilesDDState" + parameters + "(x) AS sketch, quantilesDD" + parameters + "(x) AS quantiles" +
			" FROM (" + values + ") GROUP BY id ORDER BY id",
		Result: proto.Results{
			{Name: "id", Data: &ids},
			{Name: "sketch", Data: sketches},
			{Name: "quantiles", Data: quantiles},
		},
	}), "select states")
	require.Equal(t, len(sets), sketches.Rows())

	// ch-go encodes the same states.
	require.NoError(t, conn.Do(ctx, Query{
		Body: "CREATE TABLE test_table (id UInt8, sketch " + columnType.String() + ") ENGINE = MergeTree ORDER BY id",
	}), "create table")
	require.NoError(t, conn.Do(ctx, Query{
		Body: "INSERT INTO test_table VALUES",
		Input: []proto.InputColumn{
			{Name: "id", Data: ids},
			{Name: "sketch", Data: sketches},
		},
	}), "insert")

	// ClickHouse reads the states from ch-go: the states and the quantiles
	// must be the same as the ones that ClickHouse made.
	var got proto.Results
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT sketch, finalizeAggregation(sketch) AS quantiles FROM test_table ORDER BY id",
		Result: got.Auto(),
	}), "select table")
	require.Equal(t, columnType, got[0].Data.Type())
	gotSketches := got[0].Data.(proto.ColumnOf[proto.DDSketch])
	gotQuantiles := got[1].Data.(proto.ColumnOf[[]float64])
	require.Equal(t, len(sets), gotSketches.Rows())
	for i := range sets {
		require.Equal(t, sketches.Row(i), gotSketches.Row(i), "sketch %d", i)
		require.Equal(t, quantiles.Row(i), gotQuantiles.Row(i), "quantiles %d", i)
	}

	// ClickHouse merges the states from ch-go: the quantiles must be the
	// same as the quantiles of all the values.
	var (
		merged = new(proto.ColFloat64).Array()
		all    = new(proto.ColFloat64).Array()
	)
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT quantilesDDMerge" + parameters + "(sketch) AS quantiles FROM test_table",
		Result: proto.Results{{Name: "quantiles", Data: merged}},
	}), "select merged")
	require.NoError(t, conn.Do(ctx, Query{
		Body:   "SELECT quantilesDD" + parameters + "(x) AS quantiles FROM (" + values + ")",
		Result: proto.Results{{Name: "quantiles", Data: all}},
	}), "select all")
	require.Equal(t, all.Row(0), merged.Row(0))
}
