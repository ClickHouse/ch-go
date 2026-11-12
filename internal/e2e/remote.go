package e2e

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

// Environment variables that select an already running ClickHouse server
// (for example, ClickHouse Cloud) instead of a local server started by cht.
const (
	EnvHost     = "CH_HOST"     // host:port of the native protocol endpoint
	EnvUser     = "CH_USER"     // defaults to "default"
	EnvPassword = "CH_PASSWORD" // blank by default
	EnvTLS      = "CH_TLS"      // "true" to connect with TLS
)

// DatabasePrefix is the name prefix of the databases that tests create on a
// remote server. Each test connection gets its own database.
const DatabasePrefix = "ch_go_test_"

// Remote describes an already running ClickHouse server.
type Remote struct {
	Address  string
	User     string
	Password string
	TLS      *tls.Config
}

// GetRemote returns the remote server from the environment.
// It reports false if EnvHost is not set.
func GetRemote(tb testing.TB) (Remote, bool) {
	tb.Helper()
	addr, ok := os.LookupEnv(EnvHost)
	if !ok || addr == "" {
		return Remote{}, false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		tb.Fatalf("E2E: %s=%q must be host:port: %v", EnvHost, addr, err)
	}
	r := Remote{
		Address:  addr,
		User:     os.Getenv(EnvUser),
		Password: os.Getenv(EnvPassword),
	}
	if s := os.Getenv(EnvTLS); s != "" {
		v, err := strconv.ParseBool(s)
		if err != nil {
			tb.Fatalf("E2E: %s=%s is invalid: %v", EnvTLS, s, err)
		}
		if v {
			r.TLS = &tls.Config{
				ServerName: host,
				MinVersion: tls.VersionTLS12,
			}
		}
	}
	return r, true
}

// Timeouts for a remote server. They are larger than the client defaults
// because of network latency to the server.
const (
	RemoteDialTimeout = 20 * time.Second
	RemoteReadTimeout = 60 * time.Second
)

// Exec runs a single query on a new connection to the remote server, using
// the default database.
type Exec func(ctx context.Context, query string) error

// Database creates a new database with a unique name through exec and drops
// it when the test ends. A test connection that uses this database sees only
// its own tables, as with a new local server.
func Database(tb testing.TB, exec Exec) string {
	tb.Helper()

	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		tb.Fatalf("E2E: random database name: %v", err)
	}
	name := DatabasePrefix + strconv.FormatInt(time.Now().Unix(), 10) + "_" + hex.EncodeToString(b[:])

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := exec(ctx, "CREATE DATABASE "+name); err != nil {
		tb.Fatalf("E2E: create database %s: %v", name, err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := exec(ctx, "DROP DATABASE IF EXISTS "+name+" SYNC"); err != nil {
			tb.Errorf("E2E: drop database %s: %v", name, err)
		}
	})
	return name
}
