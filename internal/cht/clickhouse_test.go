package db

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type ClickHouseLogger struct {
	Level   string `xml:"level"`
	Console int    `xml:"console,omitempty"`
}

type ClickhouseUsersXML struct {
	Path string `xml:"path"`
}

type ClickhouseUserDirectories struct {
	UsersXML ClickhouseUsersXML `xml:"users_xml"`
}

type ClickHouseConfig struct {
	XMLName xml.Name         `xml:"clickhouse"`
	Logger  ClickHouseLogger `xml:"logger"`
	HTTP    int              `xml:"http_port"`
	TCP     int              `xml:"tcp_port"`
	Host    string           `xml:"host"`

	Path            string                    `xml:"path"`
	TempPath        string                    `xml:"tmp_path"`
	UserFilesPath   string                    `xml:"user_files_path"`
	UserDirectories ClickhouseUserDirectories `xml:"user_directories"`

	MarkCacheSize int `xml:"mark_cache_size"`
	MMAPCacheSize int `xml:"mmap_cache_size"`
}

func writeXML(t testing.TB, name string, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	require.NoError(t, os.WriteFile(name, buf.Bytes(), 0700))
}

//go:embed clickhouse.users.xml
var clickHouseUserConfig []byte

func TestClickHouse(t *testing.T) {
	// clickhouse-server [OPTION] [-- [ARG]...]
	// positional arguments can be used to rewrite config.xml properties, for
	// example, --http_port=8010
	//
	// -h, --help                        show help and exit
	// -V, --version                     show version and exit
	// -C<file>, --config-file=<file>    load configuration from a given file
	// -L<file>, --log-file=<file>       use given log file
	// -E<file>, --errorlog-file=<file>  use given log file for errors only
	// -P<file>, --pid-file=<file>       use given pidfile
	// --daemon                          Run application as a daemon.
	// --umask=mask                      Set the daemon's umask (octal, e.g. 027).
	// --pidfile=path                    Write the process ID of the application to
	// given file.

	const binary = "clickhouse-server"
	if _, err := exec.LookPath(binary); err != nil {
		t.Skipf("Binary %s not found: %v", binary, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.xml")
	userCfgPath := filepath.Join(dir, "users.xml")
	cfg := ClickHouseConfig{
		Logger: ClickHouseLogger{
			Level:   "trace",
			Console: 1,
		},
		HTTP: 3000,
		TCP:  3100,
		Host: "127.0.0.1",

		Path:          filepath.Join(dir, "data"),
		TempPath:      filepath.Join(dir, "tmp"),
		UserFilesPath: filepath.Join(dir, "users"),

		MarkCacheSize: 5368709120,
		MMAPCacheSize: 1000,

		UserDirectories: ClickhouseUserDirectories{
			UsersXML: ClickhouseUsersXML{
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
		require.NoError(t, os.MkdirAll(dir, 0777))
	}
	require.NoError(t, os.WriteFile(userCfgPath, clickHouseUserConfig, 0700))

	cmd := exec.CommandContext(ctx, "clickhouse-server",
		"--config-file", cfgPath,
	)
	start := time.Now()
	require.NoError(t, cmd.Start())

	for {
		res, err := http.Get(fmt.Sprintf("http://%s:%d", cfg.Host, cfg.HTTP))
		if err == nil {
			t.Log("Started", time.Since(start).Round(time.Millisecond))
			_ = res.Body.Close()
			require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
			break
		}
	}

	t.Log("Closing")
	startClose := time.Now()
	require.NoError(t, cmd.Wait())
	t.Log("Closed in", time.Since(startClose).Round(time.Millisecond))
}
