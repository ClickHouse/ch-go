// Package cht implements ClickHouse testing utilities, primarily end to end.
package cht

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/e2e"
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
}

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
	TCP  string
	HTTP string
}

func writeXML(t testing.TB, name string, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	require.NoError(t, os.WriteFile(name, buf.Bytes(), 0o600))
}

// New creates new ClickHouse server and returns it.
//
// Skip tests if CH_E2E variable is set to 0.
// Fails if CH_E2E is 1, but no binary is available.
// Skips if CH_E2E is unset and no binary.
//
// Override binary with CH_BIN.
// Can be clickhouse-server or clickhouse.
func New(t testing.TB) Server {
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

		// Automatically pick port.
		HTTP: 0,
		TCP:  0,

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
		} else {
			tcpAddr = info.Addr
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
		TCP:  tcpAddr,
		HTTP: httpAddr,
	}
}
