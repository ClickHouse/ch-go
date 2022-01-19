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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/e2e"
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
		v = "clickhouse-server"
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

func Port(t testing.TB) int {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

type options struct {
	tcp      int
	http     int
	clusters Clusters
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

// New creates new ClickHouse server and returns it.
//
// Skip tests if CH_E2E variable is set to 0.
// Fails if CH_E2E is 1, but no binary is available.
// Skips if CH_E2E is unset and no binary.
//
// Override binary with CH_BIN.
// Can be clickhouse-server or clickhouse.
func New(t testing.TB, opts ...Option) Server {
	var o options

	for _, opt := range opts {
		opt(&o)
	}

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

		Host: "127.0.0.1",

		Path:          filepath.Join(dir, "data"),
		TempPath:      filepath.Join(dir, "tmp"),
		UserFilesPath: filepath.Join(dir, "users"),

		MarkCacheSize: 5368709120,
		MMAPCacheSize: 1000,

		UserDirectories: UserDir{
			UsersXML: UsersXML{
				Path: userCfgPath,
			},
		},

		RemoteServers: o.clusters,
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
	cmd.Stdout = logProxy(onAddr)
	cmd.Stderr = logProxy(onAddr)

	start := time.Now()
	require.NoError(t, cmd.Start())

	select {
	case <-started:
		t.Log("Started", time.Since(start).Round(time.Millisecond), tcpAddr, httpAddr)
	case <-time.After(time.Second * 10):
		t.Fatal("Clickhouse timed out to start")
	}

	t.Cleanup(func() {
		defer cancel()

		require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))

		// Done.
		t.Log("Shutting down")
		startClose := time.Now()

		if err := cmd.Wait(); err != nil {
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
