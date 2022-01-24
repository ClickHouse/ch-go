package cht

import (
	"bytes"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/require"
)

func logXML(t testing.TB, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	t.Log(buf)
}

func TestKeeperConfig(t *testing.T) {
	v := KeeperConfig{
		TCPPort:  2181,
		ServerID: 1,

		Raft: RaftConfig{
			Servers: []RaftServer{
				{
					ID:       1,
					Port:     9444,
					Hostname: "zoo1",
				},
			},
		},
	}
	t.Run("Standalone", func(t *testing.T) {
		logXML(t, v)
	})
	t.Run("Embedded", func(t *testing.T) {
		logXML(t, Config{
			Keeper: &v,
			Macros: Map{
				"shard":   "01",
				"replica": "01",
			},
			ZooKeeper: []ZooKeeperNode{
				{Port: 2181, Host: "127.0.0.1"},
			},
		})
	})
}
