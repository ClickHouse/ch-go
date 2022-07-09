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

type Map map[string]string

func (m Map) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	// Sort for deterministic marshaling.
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if err := e.EncodeToken(start); err != nil {
		return errors.Wrap(err, "end")
	}
	for _, k := range keys {
		if err := e.EncodeElement(m[k], xml.StartElement{
			Name: xml.Name{Local: k},
		}); err != nil {
			return errors.Wrap(err, "elem")
		}
	}
	if err := e.EncodeToken(start.End()); err != nil {
		return errors.Wrap(err, "end")
	}

	return e.Flush()
}

// Config for ClickHouse.
type Config struct {
	XMLName xml.Name `xml:"clickhouse"`
	Logger  Logger   `xml:"logger"`
	HTTP    int      `xml:"http_port"`
	TCP     int      `xml:"tcp_port"`
	Host    string   `xml:"listen_host"`

	InterServerHTTP     *int    `xml:"interserver_http_port,omitempty"`
	InterServerHTTPHost *string `xml:"interserver_http_host,omitempty"`

	MaxServerMemoryUsage int `xml:"max_server_memory_usage,omitempty"`

	Path            string  `xml:"path"`
	TempPath        string  `xml:"tmp_path"`
	UserFilesPath   string  `xml:"user_files_path"`
	UserDirectories UserDir `xml:"user_directories"`

	MarkCacheSize int64 `xml:"mark_cache_size"`
	MMAPCacheSize int64 `xml:"mmap_cache_size"`

	OpenTelemetrySpanLog *OpenTelemetry `xml:"opentelemetry_span_log,omitempty"`
	// Sets the probability that the ClickHouse can start a trace for executed queries (if no parent trace context is supplied).
	OpenTelemetryStartTraceProbability *float64 `xml:"opentelemetry_start_trace_probability"`

	// ZooKeeper configures ZooKeeper nodes.
	ZooKeeper      []ZooKeeperNode `xml:"zookeeper>node,omitempty"`
	Macros         Map             `xml:"macros,omitempty"`
	DistributedDDL *DistributedDDL `xml:"distributed_ddl,omitempty"`

	// Keeper is config for clickhouse-keeper server.
	Keeper *KeeperConfig `xml:"keeper_server,omitempty"`

	RemoteServers Clusters `xml:"remote_servers,omitempty"`
}

type DistributedDDL struct {
	Path               string `xml:"path,omitempty"`
	Profile            string `xml:"profile,omitempty"`
	PoolSize           int    `xml:"pool_size"`
	TaskMaxLifetime    int    `xml:"task_max_lifetime,omitempty"`
	CleanupDelayPeriod int    `xml:"cleanup_delay_period,omitempty"`
	MaxTasksInQueue    int    `xml:"max_tasks_in_queue,omitempty"`
}

type CoordinationConfig struct {
	OperationTimeoutMs          int    `xml:"operation_timeout_ms,omitempty"`
	SessionTimeoutMs            int    `xml:"session_timeout_ms,omitempty"`
	RaftLogsLevel               string `xml:"raft_logs_level,omitempty"`
	HeartBeatIntervalMs         int    `xml:"heart_beat_interval_ms,omitempty"`
	DeadSessionCheckPeriodMs    int    `xml:"dead_session_check_period_ms,omitempty"`
	ElectionTimeoutLowerBoundMs int    `xml:"election_timeout_lower_bound_ms,omitempty"`
	ElectionTimeoutUpperBoundMs int    `xml:"election_timeout_upper_bound_ms,omitempty"`
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
	XMLName             xml.Name           `xml:"keeper_server"`
	TCPPort             int                `xml:"tcp_port,omitempty"`
	ServerID            int                `xml:"server_id,omitempty"`
	LogStoragePath      string             `xml:"log_storage_path,omitempty"`
	SnapshotStoragePath string             `xml:"snapshot_storage_path,omitempty"`
	Coordination        CoordinationConfig `xml:"coordination_settings"`
	Raft                RaftConfig         `xml:"raft_configuration"`
}

type ZooKeeperNode struct {
	Index int    `xml:"index,omitempty,attr"`
	Host  string `xml:"host,omitempty"`
	Port  int    `xml:"port,omitempty"`
}
