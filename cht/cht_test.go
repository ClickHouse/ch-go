package cht_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/cht"
	"github.com/go-faster/ch/internal/ztest"
	"github.com/go-faster/ch/proto"
)

func TestXML(t *testing.T) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(cht.Config{
		RemoteServers: cht.Clusters{
			"alpha": cht.Cluster{
				Secret: "foo",
				Shards: []cht.Shard{
					{
						Weight:              10,
						InternalReplication: true,
						Replicas: []cht.Replica{
							{
								Priority: 1,
								Host:     "localhost",
								Port:     33123,
							},
						},
					},
				},
			},
			"beta": cht.Cluster{
				Secret: "bar",
			},
		},
	}))

	t.Log(buf)
}

func TestConnect(t *testing.T) {
	ctx := context.Background()
	server := cht.New(t, cht.WithLog(ztest.NewLogger(t)))

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
		require.NoError(t, client.Do(ctx, createTable))
	})
	t.Run("SelectOne", func(t *testing.T) {
		// Select single row.
		var data proto.ColUInt8
		selectOne := ch.Query{
			Body: "SELECT 1 AS one",
			Result: proto.Results{
				{
					Name: "one",
					Data: &data,
				},
			},
		}
		require.NoError(t, client.Do(ctx, selectOne))
		require.Len(t, data, 1)
		require.Equal(t, byte(1), data[0])
	})
}

func TestCluster(t *testing.T) {
	cht.Skip(t)
	var (
		alphaPort = cht.Port(t)
		betaPort  = cht.Port(t)
	)
	clusters := cht.Clusters{
		"nexus": cht.Cluster{
			Shards: []cht.Shard{
				{
					Replicas: []cht.Replica{
						{Host: "localhost", Port: alphaPort},
						{Host: "localhost", Port: betaPort},
					},
				},
			},
		},
	}
	var (
		withCluster = cht.WithClusters(clusters)
		lg          = ztest.NewLogger(t)
		servers     = cht.Many(t,
			cht.With(
				cht.WithTCP(alphaPort), withCluster, cht.WithLog(lg.Named("alpha")),
			),
			cht.With(
				cht.WithTCP(betaPort), withCluster, cht.WithLog(lg.Named("beta")),
			),
		)
		alpha = servers[0]
		beta  = servers[1]
		ctx   = context.Background()
	)
	t.Run("Clusters", func(t *testing.T) {
		client, err := ch.Dial(ctx, alpha.TCP, ch.Options{})
		require.NoError(t, err)
		defer client.Close()

		var data proto.Results
		getClusters := ch.Query{
			Body:   "SELECT * FROM system.clusters",
			Result: data.Auto(),
		}
		require.NoError(t, client.Do(ctx, getClusters))
		require.Equal(t, 2, data.Rows())
	})
	t.Run("Beta", func(t *testing.T) {
		client, err := ch.Dial(ctx, beta.TCP, ch.Options{})
		require.NoError(t, err)
		defer client.Close()

		require.NoError(t, client.Ping(ctx))
	})
}
