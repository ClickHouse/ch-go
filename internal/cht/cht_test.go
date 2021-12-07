package cht

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/internal/e2e"
	"github.com/go-faster/ch/internal/proto"
)

func writeXML(t testing.TB, name string, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	require.NoError(t, os.WriteFile(name, buf.Bytes(), 0o700))
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
	e2e.Skip(t)

	binaryPath, err := Bin()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup data directory and config.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.xml")
	userCfgPath := filepath.Join(dir, "users.xml")
	cfg := Config{
		Logger: Logger{
			Level:   "trace",
			Console: 1,
		},
		HTTP: 31200,
		TCP:  31201,
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
		require.NoError(t, os.MkdirAll(dir, 0o777))
	}
	require.NoError(t, os.WriteFile(userCfgPath, usersCfg, 0o700))

	// Setup command.
	var args []string
	if !strings.HasSuffix(binaryPath, "server") {
		// Binary bundle, adding subcommand.
		// Like in static distributions.
		args = append(args, "server")
	}
	args = append(args, "--config-file", cfgPath)
	cmd := exec.CommandContext(ctx, binaryPath, args...)

	start := time.Now()
	require.NoError(t, cmd.Start())

	// Polling ClickHouse until ready.
	for {
		res, err := http.Get(fmt.Sprintf("http://%s:%d", cfg.Host, cfg.HTTP))
		if err != nil {
			continue
		}

		t.Log("Started in", time.Since(start).Round(time.Millisecond))
		_ = res.Body.Close()
		break
	}

	client, err := ch.Dial(ctx, net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.TCP)), ch.Options{})
	require.NoError(t, err)
	t.Log("Connected", client.ServerInfo(), client.Location())

	// Sending query.
	require.NoError(t, client.SendQuery(ctx, "CREATE TABLE test_table (id UInt64) ENGINE = MergeTree ORDER BY id", "1"))

	p, err := client.Packet()
	require.NoError(t, err)

	switch p {
	case proto.ServerCodeEndOfStream: // expected
		t.Log("Query sent")
	default:
		t.Fatal("unexpected server code", p)
	}

	// Select
	require.NoError(t, client.SendQuery(ctx, "SELECT 1 AS one", "2"))

	p, err = client.Packet()
	require.NoError(t, err)

	switch p {
	case proto.ServerCodeData: // expected
		t.Log("Data received")
		b, err := client.Block()
		require.NoError(t, err)
		t.Log(b, b.Info)
	default:
		t.Fatal("unexpected server code", p)
	}

	require.NoError(t, client.Close())
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
}
