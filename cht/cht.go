// Package cht implements ClickHouse testing utilities, primarily end to end.
package cht

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/ClickHouse/ch-go/internal/e2e"
)

// EnvBin is environmental variable that sets paths to current
// ClickHouse binary.
const EnvBin = "CH_BIN"

//go:embed clickhouse.users.xml
var usersCfg []byte

// Bin returns path to current ClickHouse binary.
func Bin() (string, error) {
	v, ok := os.LookupEnv(EnvBin)
	if !ok {
		// Fallback to default binary name.
		// Should be in $PATH.
		v = "clickhouse"
	}
	p, err := exec.LookPath(v)
	if err != nil {
		return "", errors.Wrap(err, "lookup")
	}
	return p, nil
}

// Server represents testing ClickHouse server.
type Server struct {
	TCP    string
	HTTP   string
	Config Config
}

func writeXML(t testing.TB, name string, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	require.NoError(t, os.WriteFile(name, buf.Bytes(), 0o600))
}

func portOf(t testing.TB, addr string) int {
	t.Helper()

	addr = strings.TrimPrefix(addr, "http://")

	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	v, err := strconv.Atoi(port)
	require.NoError(t, err)

	return v
}

// Ports allocates n free ports.
func Ports(t testing.TB, n int) []int {
	ports := make([]int, n)
	var listeners []net.Listener
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, ln)
		ports[i] = ln.Addr().(*net.TCPAddr).Port
	}
	for _, ln := range listeners {
		require.NoError(t, ln.Close())
	}
	return ports
}

type options struct {
	tcp              int
	http             int
	httpInternal     *int
	httpInternalHost *string
	clusters         Clusters
	lg               *zap.Logger
	zooKeeper        []ZooKeeperNode
	keeper           *KeeperConfig
	macros           Map
	ddl              *DistributedDDL

	maxServerMemoryUsage int
}

func WithMaxServerMemoryUsage(n int) Option {
	return func(o *options) {
		o.maxServerMemoryUsage = n
	}
}

func WithKeeper(cfg KeeperConfig) Option {
	return func(o *options) {
		o.keeper = &cfg
	}
}

func WithDistributedDDL(ddl DistributedDDL) Option {
	return func(o *options) {
		o.ddl = &ddl
	}
}

func WithZooKeeper(nodes []ZooKeeperNode) Option {
	return func(o *options) {
		o.zooKeeper = nodes
	}
}

func WithInterServerHTTP(port int) Option {
	return func(o *options) {
		o.httpInternal = &port
	}
}

func WithInterServerHost(host string) Option {
	return func(o *options) {
		o.httpInternalHost = &host
	}
}

type Option func(o *options)

func WithClusters(c Clusters) Option {
	return func(o *options) {
		o.clusters = c
	}
}

func WithTCP(port int) Option {
	return func(o *options) {
		o.tcp = port
	}
}

func WithMacros(m Map) Option {
	return func(o *options) {
		o.macros = m
	}
}

func WithLog(lg *zap.Logger) Option {
	return func(o *options) {
		o.lg = lg
	}
}

// With composes opts into single Option.
//
// Useful for Many calls.
func With(opts ...Option) Option {
	return func(o *options) {
		for _, opt := range opts {
			opt(o)
		}
	}
}

// Many concurrent calls to New.
func Many(t testing.TB, opts ...Option) []Server {
	if len(opts) == 0 {
		t.Fatal("Many(t) is invalid")
	}
	var wg sync.WaitGroup
	out := make([]Server, len(opts))
	for i := range opts {
		wg.Add(1)
		o := opts[i]
		idx := i
		go func() {
			defer wg.Done()
			out[idx] = New(t, o)
		}()
	}
	wg.Wait()
	return out
}

// Skip test if e2e is not available.
func Skip(t testing.TB) {
	_ = BinOrSkip(t)
}

// BinOrSkip returns binary path or skips test.
func BinOrSkip(t testing.TB) string {
	status := e2e.Get(t)
	if status == e2e.Disabled {
		t.Skip("E2E: Disabled")
	}
	binaryPath, err := Bin()
	if err != nil {
		switch status {
		case e2e.NotSet:
			t.Skip("E2E: Skip")
		case e2e.Enabled:
			t.Fatalf("E2E: No binary: %v", err)
		}
	}
	require.NoError(t, err)
	return binaryPath
}

type OpenTelemetry struct {
	Engine          string `xml:"engine,omitempty"`
	Database        string `xml:"database,omitempty"`
	Table           string `xml:"table,omitempty"`
	FlushIntervalMs int    `xml:"flush_interval_milliseconds,omitempty"`
}

// New creates new ClickHouse server and returns it.
// Use Many to start multiple servers at once.
//
// Skips tests if CH_E2E variable is set to 0.
// Fails if CH_E2E is 1, but no binary is available.
// Skips if CH_E2E is unset and no binary.
//
// Override binary with CH_BIN.
// Can be clickhouse-server or clickhouse.
func New(t testing.TB, opts ...Option) Server {
	o := options{
		lg: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&o)
	}

	binaryPath := BinOrSkip(t)
	ctx, cancel := context.WithCancel(context.Background())

	// Setup data directory and config.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.xml")
	userCfgPath := filepath.Join(dir, "users.xml")
	cfg := Config{
		Logger: Logger{
			Level:   "trace",
			Console: 1,
		},

		HTTP: o.http,
		TCP:  o.tcp,

		InterServerHTTP: o.httpInternal,

		Host: "127.0.0.1",

		Path:          filepath.Join(dir, "data"),
		TempPath:      filepath.Join(dir, "tmp"),
		UserFilesPath: filepath.Join(dir, "users"),

		MaxServerMemoryUsage: o.maxServerMemoryUsage,

		MarkCacheSize: 5368709120,
		MMAPCacheSize: 1000,

		OpenTelemetrySpanLog: &OpenTelemetry{
			Table:    "opentelemetry_span_log",
			Database: "system",
			Engine: `engine MergeTree
            partition by toYYYYMM(finish_date)
            order by (finish_date, finish_time_us, trace_id)`,
		},

		UserDirectories: UserDir{
			UsersXML: UsersXML{
				Path: userCfgPath,
			},
		},

		Keeper:         o.keeper,
		ZooKeeper:      o.zooKeeper,
		RemoteServers:  o.clusters,
		Macros:         o.macros,
		DistributedDDL: o.ddl,
	}
	writeXML(t, cfgPath, cfg)
	for _, dir := range []string{
		cfg.Path,
		cfg.TempPath,
		cfg.UserFilesPath,
	} {
		require.NoError(t, os.MkdirAll(dir, 0o750))
	}
	require.NoError(t, os.WriteFile(userCfgPath, usersCfg, 0o600))

	// Setup command.
	var args []string
	if !strings.HasSuffix(binaryPath, "server") {
		// Binary bundle, adding subcommand.
		// Like in static distributions.
		args = append(args, "server")
	}
	args = append(args, "--config-file", cfgPath)
	cmd := exec.CommandContext(ctx, binaryPath, args...) // #nosec G204

	var (
		tcpAddr  string
		httpAddr string
	)
	started := make(chan struct{})
	onAddr := func(info logInfo) {
		if info.Ready {
			close(started)
		}
		if !strings.Contains(info.Addr, "127.0.0.1") {
			return
		}
		if strings.HasPrefix(info.Addr, "http:") {
			httpAddr = info.Addr
			cfg.HTTP = portOf(t, httpAddr)
		} else {
			tcpAddr = info.Addr
			cfg.TCP = portOf(t, tcpAddr)
		}
	}
	cmd.Stdout = logProxy(o.lg, onAddr)
	cmd.Stderr = logProxy(o.lg, onAddr)

	start := time.Now()
	require.NoError(t, cmd.Start())

	wait := make(chan error)
	go func() {
		defer close(wait)
		wait <- cmd.Wait()
	}()

	startTimeout := time.Second * 10
	if runtime.GOARCH == "riscv64" {
		// RISC-V devboards are slow.
		startTimeout = time.Minute
	}

	select {
	case <-started:
		t.Log("Started", time.Since(start).Round(time.Millisecond), tcpAddr, httpAddr)
	case err := <-wait:
		t.Fatal(err)
	case <-time.After(startTimeout):
		t.Fatal("Clickhouse timed out to start")
	}

	t.Cleanup(func() {
		defer cancel()

		require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))

		// Done.
		t.Log("Shutting down")
		startClose := time.Now()

		if err := <-wait; err != nil {
			// Check for SIGKILL.
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr)
			require.Equal(t, exitErr.Sys().(syscall.WaitStatus).Signal(), syscall.SIGKILL)
		}

		t.Log("Closed in", time.Since(startClose).Round(time.Millisecond))
	})

	return Server{
		TCP:    tcpAddr,
		HTTP:   httpAddr,
		Config: cfg,
	}
}
