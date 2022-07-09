package cht_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/internal/ztest"
	"github.com/ClickHouse/ch-go/proto"
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
	server := cht.New(t,
		cht.WithLog(ztest.NewLogger(t)),
		cht.WithMaxServerMemoryUsage(1024*1024*256),
	)
	t.Parallel()

	client, err := ch.Dial(ctx, ch.Options{Address: server.TCP})
	require.NoError(t, err)

	t.Log("Connected", client.ServerInfo())
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

func tableMacros(shard, replica int) cht.Map {
	return cht.Map{
		"shard":   fmt.Sprintf("%02d", shard),
		"replica": fmt.Sprintf("%02d", replica),
	}
}

func withTableMacros(shard, replica int) cht.Option {
	return cht.WithMacros(tableMacros(shard, replica))
}

func TestCluster(t *testing.T) {
	{
		ctx := context.Background()
		server := cht.New(t, cht.WithLog(ztest.NewLogger(t)))
		client, err := ch.Dial(ctx, ch.Options{Address: server.TCP})
		require.NoError(t, err)
		if v := client.ServerInfo(); (v.Major < 22) || (v.Major == 22 && v.Minor < 6) {
			t.Skip("Skipping (not supported)")
		}
	}
	var (
		ports = cht.Ports(t, 3*4)

		alphaPort            = ports[0]
		alphaKeeperPort      = ports[1]
		alphaInternalPort    = ports[2]
		alphaInterServerPort = ports[3]

		betaPort            = ports[4]
		betaKeeperPort      = ports[5]
		betaInternalPort    = ports[6]
		betaInterServerPort = ports[7]

		gammaPort            = ports[8]
		gammaKeeperPort      = ports[9]
		gammaInternalPort    = ports[10]
		gammaInterServerPort = ports[11]
	)
	t.Parallel()
	const host = "127.0.0.1"
	clusters := cht.Clusters{
		"nexus": cht.Cluster{
			Shards: []cht.Shard{
				{
					InternalReplication: true,
					Replicas: []cht.Replica{
						{Host: host, Port: alphaPort},
					},
				},
				{
					InternalReplication: true,
					Replicas: []cht.Replica{
						{Host: host, Port: betaPort},
					},
				},
				{
					InternalReplication: true,
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
		withDDL = cht.WithDistributedDDL(cht.DistributedDDL{
			PoolSize: 1,
			Profile:  "default",
			Path:     "/nexus/task_queue/ddl",
		})
		withInterHost = cht.WithInterServerHost(host)
		withZooKeeper = cht.WithZooKeeper(nodes)
		coordination  = cht.CoordinationConfig{
			ElectionTimeoutLowerBoundMs: 250,
			ElectionTimeoutUpperBoundMs: 350,
			HeartBeatIntervalMs:         100,
			DeadSessionCheckPeriodMs:    100,
			OperationTimeoutMs:          200,
		}
		retry = backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond*20), 20)

		withOptions = cht.With(withCluster, withZooKeeper, withDDL, withInterHost)
		servers     = cht.Many(t,
			cht.With(
				cht.WithKeeper(cht.KeeperConfig{
					Raft:         raft,
					ServerID:     1,
					TCPPort:      alphaKeeperPort,
					Coordination: coordination,

					LogStoragePath:      t.TempDir(),
					SnapshotStoragePath: t.TempDir(),
				}),
				withTableMacros(1, 1),
				cht.WithInterServerHTTP(alphaInterServerPort),
				cht.WithTCP(alphaPort), withOptions, cht.WithLog(lg.Named("alpha")),
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
				withTableMacros(2, 1),
				cht.WithInterServerHTTP(betaInterServerPort),
				cht.WithTCP(betaPort), withOptions, cht.WithLog(lg.Named("beta")),
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
				withTableMacros(3, 1),
				cht.WithInterServerHTTP(gammaInterServerPort),
				cht.WithTCP(gammaPort), withOptions, cht.WithLog(lg.Named("gamma")),
			),
		)
		alpha = servers[0]
		beta  = servers[1]
		gamma = servers[2]
		ctx   = context.Background()
	)
	t.Run("Create distributed table", func(t *testing.T) {
		t.Parallel()

		client, err := ch.Dial(ctx, ch.Options{Address: alpha.TCP, Logger: lg.Named("client")})
		require.NoError(t, err)
		defer client.Close()

		var data proto.Results
		require.NoError(t, client.Do(ctx, ch.Query{
			Body:   "SELECT * FROM system.clusters",
			Result: data.Auto(),
		}))
		require.Equal(t, 3, data.Rows())

		do := func(ctx context.Context, t testing.TB, q ch.Query) {
			t.Helper()
			require.NoError(t, backoff.Retry(func() error {
				return client.Do(ctx, q)
			}, retry))
		}
		do(ctx, t, ch.Query{
			Result:   (&proto.Results{}).Auto(),
			OnResult: func(ctx context.Context, block proto.Block) error { return nil },
			Body: `CREATE TABLE IF NOT EXISTS hits ON CLUSTER 'nexus'
(
    EventDate DateTime,
    CounterID UInt32,
    UserID    UInt32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)`,
		})
		do(ctx, t, ch.Query{
			Result:   (&proto.Results{}).Auto(),
			OnResult: func(ctx context.Context, block proto.Block) error { return nil },
			Body: `CREATE TABLE IF NOT EXISTS hits_distributed ON CLUSTER 'nexus' AS hits
ENGINE = Distributed('nexus', default, hits, rand())`,
		})
		t.Run("Insert", func(t *testing.T) {
			for i := 0; i < 20; i++ {
				require.NoError(t, client.Do(ctx, ch.Query{
					Body: `INSERT INTO hits_distributed VALUES`,
					Input: proto.Input{
						{
							Name: "EventDate",
							Data: proto.ColDateTime{
								Data: []proto.DateTime{
									proto.ToDateTime(time.Now()),
								},
							},
						},
						{
							Name: "CounterID",
							Data: proto.ColUInt32{
								10,
							},
						},
						{
							Name: "UserID",
							Data: proto.ColUInt32{
								uint32(i),
							},
						},
					},
				}))
			}
			t.Run("Select", func(t *testing.T) {
				// Waiting until distributed table is fully propagated.
				for i := 0; i < 50; i++ {
					var count proto.ColUInt64
					require.NoError(t, client.Do(ctx, ch.Query{
						Body: `SELECT count(1) as total FROM hits_distributed`,
						Result: proto.Results{
							{Name: "total", Data: &count},
						},
					}))
					if len(count) > 0 && count[0] == 20 {
						t.Log("Got target count")
						return
					}
					time.Sleep(time.Millisecond * 50)
				}
				t.Error("Timed out waiting until target count")
			})
		})
	})
	t.Run("Beta", func(t *testing.T) {
		t.Parallel()

		client, err := ch.Dial(ctx, ch.Options{Address: beta.TCP})
		require.NoError(t, err)
		defer client.Close()

		require.NoError(t, client.Ping(ctx))
	})
	t.Run("Gamma", func(t *testing.T) {
		t.Parallel()

		client, err := ch.Dial(ctx, ch.Options{Address: gamma.TCP})
		require.NoError(t, err)
		defer client.Close()

		require.NoError(t, client.Ping(ctx))
	})
}
