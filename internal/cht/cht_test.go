package cht

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

func writeXML(t testing.TB, name string, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	require.NoError(t, os.WriteFile(name, buf.Bytes(), 0700))
}

func TestRun(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.xml")
	userCfgPath := filepath.Join(dir, "users.xml")
	cfg := Config{
		Logger: Logger{
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
		require.NoError(t, os.MkdirAll(dir, 0777))
	}
	require.NoError(t, os.WriteFile(userCfgPath, usersCfg, 0700))

	cmd := exec.CommandContext(ctx, os.Getenv("CLICKHOUSE_BIN"), "server",
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
