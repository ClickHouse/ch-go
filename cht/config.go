package cht

import (
	"encoding/xml"
	"sort"

	"github.com/go-faster/errors"
)

// Logger settings.
type Logger struct {
	Level   string `xml:"level"`
	Console int    `xml:"console,omitempty"`
}

// UsersXML config for ClickHouse.
type UsersXML struct {
	Path string `xml:"path"`
}

// UserDir for ClickHouse.
type UserDir struct {
	UsersXML UsersXML `xml:"users_xml"`
}

type Replica struct {
	Priority int    `xml:"priority,omitempty"`
	Host     string `xml:"host"`
	Port     int    `xml:"port"`
}

type Shard struct {
	XMLName             xml.Name  `xml:"shard"`
	Weight              int       `xml:"weight,omitempty"`
	InternalReplication bool      `xml:"internal_replication,omitempty"`
	Replicas            []Replica `xml:"replica,omitempty"`
}

type Clusters map[string]Cluster

func (c Clusters) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	// Sort for deterministic marshaling.
	var keys []string
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if err := e.EncodeToken(start); err != nil {
		return errors.Wrap(err, "secret end")
	}
	for _, k := range keys {
		if err := e.EncodeElement(c[k], xml.StartElement{
			Name: xml.Name{Local: k},
		}); err != nil {
			return errors.Wrap(err, "secret")
		}
	}
	if err := e.EncodeToken(start.End()); err != nil {
		return errors.Wrap(err, "end")
	}

	return e.Flush()
}

type Cluster struct {
	Secret string  `xml:"secret,omitempty"`
	Shards []Shard `xml:"shard"`
}

// Config for ClickHouse.
type Config struct {
	XMLName xml.Name `xml:"clickhouse"`
	Logger  Logger   `xml:"logger"`
	HTTP    int      `xml:"http_port"`
	TCP     int      `xml:"tcp_port"`
	Host    string   `xml:"host"`

	Path            string  `xml:"path"`
	TempPath        string  `xml:"tmp_path"`
	UserFilesPath   string  `xml:"user_files_path"`
	UserDirectories UserDir `xml:"user_directories"`

	MarkCacheSize int64 `xml:"mark_cache_size"`
	MMAPCacheSize int64 `xml:"mmap_cache_size"`

	RemoteServers Clusters `xml:"remote_servers,omitempty"`
}

type CoordinationConfig struct {
	OperationTimeoutMs int    `xml:"operation_timeout_ms"`
	SessionTimeoutMs   int    `xml:"session_timeout_ms"`
	RaftLogsLevel      string `xml:"raft_logs_level"`
}

type RaftServer struct {
	ID       int    `xml:"id"`
	Hostname string `xml:"hostname"`
	Port     int    `xml:"port"`
}

type RaftConfig struct {
	Servers []RaftServer `xml:"servers"`
}

// KeeperConfig is config for clickhouse-keeper.
//
// https://clickhouse.com/docs/en/operations/clickhouse-keeper/
type KeeperConfig struct {
	XMLName             xml.Name `xml:"keeper_server"`
	TCPPort             int      `xml:"tcp_port"`
	ServerID            int      `xml:"server_id"`
	LogStoragePath      string   `json:"log_storage_path"`
	SnapshotStoragePath string   `json:"snapshot_storage_path"`

	Coordination CoordinationConfig `xml:"coordination_settings"`
	Raft         RaftConfig         `xml:"raft_configuration"`
}
