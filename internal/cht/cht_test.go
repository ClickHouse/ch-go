package cht

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
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

	"github.com/go-faster/ch/internal/proto"
)

func writeXML(t testing.TB, name string, v interface{}) {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	require.NoError(t, e.Encode(v))
	require.NoError(t, os.WriteFile(name, buf.Bytes(), 0700))
}

const (
	clientHello = 0
	serverHello = 0
)

// Buffer implements ClickHouse binary protocol.
type Buffer struct {
	Buf []byte
}

// Reset buffer to zero length.
func (b *Buffer) Reset() {
	b.Buf = b.Buf[:0]
}

// Read implements io.Reader.
func (b *Buffer) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(b.Buf) == 0 {
		return 0, io.EOF
	}
	n = copy(p, b.Buf)
	b.Buf = b.Buf[n:]
	return n, nil
}

// PutUvarint encodes x to buffer as uvarint.
func (b *Buffer) PutUvarint(x uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, x)
	b.Buf = append(b.Buf, buf[:n]...)
}

func (b *Buffer) PutInt(x int) {
	b.PutUvarint(uint64(x))
}

// PutLen encodes length to buffer as uvarint.
func (b *Buffer) PutLen(x int) {
	b.PutUvarint(uint64(x))
}

// PutString encodes sting value to buffer.
func (b *Buffer) PutString(s string) {
	b.PutLen(len(s))
	b.Buf = append(b.Buf, s...)
}

type ClientHello struct {
	Name     string
	Major    int
	Minor    int
	Revision int
}

func (c ClientHello) Encode(b *Buffer) {
	b.PutString(c.Name)
	b.PutInt(c.Major)
	b.PutInt(c.Minor)
	b.PutInt(c.Revision)
}

func writeTCP(conn *net.TCPConn, buf *Buffer) error {
	_, err := conn.Write(buf.Buf)
	return err
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

	tcpAddr, err := net.ResolveTCPAddr("tcp4", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.TCP)))
	require.NoError(t, err)

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

	conn, err := net.DialTCP("tcp4", nil, tcpAddr)
	require.NoError(t, err)
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(time.Second)))

	// Perform handshake.
	b := new(Buffer)
	b.PutUvarint(uint64(proto.ClientHello))
	(ClientHello{
		Name:     "go-faster/ch",
		Major:    1,
		Minor:    1,
		Revision: 54429,
	}).Encode(b)
	b.PutString("default") // db
	b.PutString("default") // user
	b.PutString("")        // password

	require.NoError(t, writeTCP(conn, b))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	r := bufio.NewReader(conn)

	// Read message type.
	n, err := binary.ReadUvarint(r)
	require.NoError(t, err)
	if n != uint64(proto.ServerHello) {
		t.Fatalf("got unexpected message: %d", n)
	}

	// Read string length.
	n, err = binary.ReadUvarint(r)
	require.NoError(t, err)

	// Read server name. Should be "ClickHouse".
	b.Reset()
	b.Buf = append(b.Buf, make([]byte, n)...)
	if _, err := io.ReadFull(r, b.Buf); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, "ClickHouse", string(b.Buf))
	t.Log("Got ServerHello from", string(b.Buf))

	require.NoError(t, conn.Close())
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
