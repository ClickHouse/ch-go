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

	client, err := ch.Dial(ctx, ch.Options{Address: server.TCP})
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
		ports = cht.Ports(t, 3*3)

		alphaPort         = ports[0]
		alphaKeeperPort   = ports[1]
		alphaInternalPort = ports[2]

		betaPort         = ports[3]
		betaKeeperPort   = ports[4]
		betaInternalPort = ports[5]

		gammaPort         = ports[6]
		gammaKeeperPort   = ports[7]
		gammaInternalPort = ports[8]
	)
	const host = "127.0.0.1"
	clusters := cht.Clusters{
		"nexus": cht.Cluster{
			Shards: []cht.Shard{
				{
					Replicas: []cht.Replica{
						{Host: host, Port: alphaPort},
					},
				},
				{
					Replicas: []cht.Replica{
						{Host: host, Port: betaPort},
					},
				},
				{
					Replicas: []cht.Replica{
						{Host: host, Port: gammaPort},
					},
				},
			},
		},
	}
	var (
		withCluster = cht.WithClusters(clusters)
		lg          = ztest.NewLogger(t)
		nodes       = []cht.ZooKeeperNode{
			{Index: 1, Host: host, Port: alphaKeeperPort},
			{Index: 2, Host: host, Port: betaKeeperPort},
			{Index: 3, Host: host, Port: gammaKeeperPort},
		}
		raft = cht.RaftConfig{
			Servers: []cht.RaftServer{
				{ID: 1, Hostname: host, Port: alphaInternalPort},
				{ID: 2, Hostname: host, Port: betaInternalPort},
				{ID: 3, Hostname: host, Port: gammaInternalPort},
			},
		}
		withZooKeeper = cht.WithZooKeeper(nodes)
		coordination  = cht.CoordinationConfig{
			ElectionTimeoutLowerBoundMs: 50,
			ElectionTimeoutUpperBoundMs: 60,
			HeartBeatIntervalMs:         10,
			DeadSessionCheckPeriodMs:    10,
		}
		servers = cht.Many(t,
			cht.With(
				cht.WithKeeper(cht.KeeperConfig{
					Raft:         raft,
					ServerID:     1,
					TCPPort:      alphaKeeperPort,
					Coordination: coordination,

					LogStoragePath:      t.TempDir(),
					SnapshotStoragePath: t.TempDir(),
				}),
				cht.WithTCP(alphaPort), withCluster, withZooKeeper, cht.WithLog(lg.Named("alpha")),
			),
			cht.With(
				cht.WithKeeper(cht.KeeperConfig{
					Raft:         raft,
					ServerID:     2,
					TCPPort:      betaKeeperPort,
					Coordination: coordination,

					LogStoragePath:      t.TempDir(),
					SnapshotStoragePath: t.TempDir(),
				}),
				cht.WithTCP(betaPort), withCluster, withZooKeeper, cht.WithLog(lg.Named("beta")),
			),
			cht.With(
				cht.WithKeeper(cht.KeeperConfig{
					Raft:         raft,
					ServerID:     3,
					TCPPort:      gammaKeeperPort,
					Coordination: coordination,

					LogStoragePath:      t.TempDir(),
					SnapshotStoragePath: t.TempDir(),
				}),
				cht.WithTCP(gammaPort), withCluster, withZooKeeper, cht.WithLog(lg.Named("gamma")),
			),
		)
		alpha = servers[0]
		beta  = servers[1]
		gamma = servers[2]
		ctx   = context.Background()
	)
	t.Run("Clusters", func(t *testing.T) {
		client, err := ch.Dial(ctx, ch.Options{Address: alpha.TCP})
		require.NoError(t, err)
		defer client.Close()

		var data proto.Results
		getClusters := ch.Query{
			Body:   "SELECT * FROM system.clusters",
			Result: data.Auto(),
		}
		require.NoError(t, client.Do(ctx, getClusters))
		require.Equal(t, 3, data.Rows())
	})
	t.Run("Beta", func(t *testing.T) {
		client, err := ch.Dial(ctx, ch.Options{Address: beta.TCP})
		require.NoError(t, err)
		defer client.Close()

		require.NoError(t, client.Ping(ctx))
	})
	t.Run("Gamma", func(t *testing.T) {
		client, err := ch.Dial(ctx, ch.Options{Address: gamma.TCP})
		require.NoError(t, err)
		defer client.Close()

		require.NoError(t, client.Ping(ctx))
	})
}
