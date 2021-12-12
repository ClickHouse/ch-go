package cht_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/internal/cht"
	proto2 "github.com/go-faster/ch/proto"
)

func TestConnect(t *testing.T) {
	ctx := context.Background()
	server := cht.Connect(t)

	client, err := ch.Dial(ctx, server.TCP, ch.Options{})
	require.NoError(t, err)

	t.Log("Connected", client.Location())
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	t.Run("CreateTable", func(t *testing.T) {
		// Create table, no data fetch.
		createTable := ch.Query{
			Body: "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree ORDER BY id",
		}
		require.NoError(t, client.Query(ctx, createTable))
	})
	t.Run("SelectOne", func(t *testing.T) {
		// Select single row.
		var data proto2.ColUInt8
		selectOne := ch.Query{
			Body: "SELECT 1 AS one",
			Result: []proto2.ResultColumn{
				{
					Name: "one",
					Data: &data,
				},
			},
		}
		require.NoError(t, client.Query(ctx, selectOne))
		require.Len(t, data, 1)
		require.Equal(t, byte(1), data[0])
	})
}
