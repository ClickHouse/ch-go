package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/proto"
)

func TestClient_Query(t *testing.T) {
	ctx := context.Background()
	t.Parallel()
	t.Run("CreateTable", func(t *testing.T) {
		t.Parallel()
		// Create table, no data fetch.
		createTable := Query{
			Query: "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree ORDER BY id",
		}
		require.NoError(t, Conn(t).Query(ctx, createTable))
	})
	t.Run("SelectOne", func(t *testing.T) {
		t.Parallel()
		// Select single row.
		var data proto.ColumnUInt8
		selectOne := Query{
			Query: "SELECT 1 AS one",
			Columns: []proto.Column{
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
}
