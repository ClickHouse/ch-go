package ch

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/internal/e2e"
	"github.com/ClickHouse/ch-go/internal/gold"
	"github.com/ClickHouse/ch-go/proto"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

// testServer returns options to connect to a new, empty test server.
//
// By default it starts a local server with cht. If e2e.EnvHost is set, it
// connects to that server instead and creates a new database for the test,
// so the test does not see tables of other tests.
func testServer(t testing.TB) Options {
	t.Helper()

	r, ok := e2e.GetRemote(t)
	if !ok {
		server := cht.New(t)
		return Options{Address: server.TCP}
	}

	opt := Options{
		Address:     r.Address,
		User:        r.User,
		Password:    r.Password,
		TLS:         r.TLS,
		DialTimeout: e2e.RemoteDialTimeout,
		ReadTimeout: e2e.RemoteReadTimeout,
	}
	opt.Database = e2e.Database(t, func(ctx context.Context, query string) error {
		client, err := Dial(ctx, opt)
		if err != nil {
			return err
		}
		return errors.Join(client.Do(ctx, Query{Body: query}), client.Close())
	})
	return opt
}

// isRemote reports whether tests run on a server that cht did not start.
func isRemote(t testing.TB) bool {
	_, ok := e2e.GetRemote(t)
	return ok
}

// withServer sets the connection fields of opt from server, unless opt
// already sets them.
func withServer(opt, server Options) Options {
	opt.Address = server.Address
	if opt.User == "" {
		opt.User = server.User
	}
	if opt.Password == "" {
		opt.Password = server.Password
	}
	if opt.Database == "" {
		opt.Database = server.Database
	}
	if opt.TLS == nil {
		opt.TLS = server.TLS
	}
	if opt.DialTimeout == 0 {
		opt.DialTimeout = server.DialTimeout
	}
	if opt.ReadTimeout == 0 {
		opt.ReadTimeout = server.ReadTimeout
	}
	return opt
}

func ConnOpt(t testing.TB, opt Options) *Client {
	t.Helper()

	ctx := context.Background()
	opt = withServer(opt, testServer(t))
	if opt.Logger == nil {
		opt.Logger = zaptest.NewLogger(t)
	}

	client, err := Dial(ctx, opt)
	// A remote server can have more than one replica. A replica that did not
	// get the new database yet returns UNKNOWN_DATABASE, so try again.
	for i := 0; i < 20 && IsErr(err, proto.ErrUnknownDatabase) && isRemote(t); i++ {
		time.Sleep(500 * time.Millisecond)
		client, err = Dial(ctx, opt)
	}
	require.NoError(t, err)

	t.Log("Connected", client.ServerInfo())
	t.Cleanup(func() {
		if !client.IsClosed() {
			require.NoError(t, client.Close())
		}
	})

	return client
}

func Conn(t testing.TB) *Client {
	return ConnOpt(t, Options{})
}

func SkipNoFeature(t *testing.T, client *Client, feature proto.Feature) {
	if !client.ServerInfo().Has(feature) {
		t.Skipf("Skipping (feature %q not supported)", feature)
	}
}

func TestDial(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		conn := Conn(t)
		require.NoError(t, conn.Ping(context.Background()))
	})
	t.Run("Closed", func(t *testing.T) {
		ctx := context.Background()
		conn, err := Dial(ctx, testServer(t))
		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		require.NoError(t, conn.Close())
		require.ErrorIs(t, conn.Ping(ctx), ErrClosed)
		require.ErrorIs(t, conn.Do(ctx, Query{}), ErrClosed)
	})
	t.Run("DatabaseNotFound", func(t *testing.T) {
		ctx := context.Background()
		client, err := Dial(ctx, withServer(Options{
			Database: "bad",
		}, testServer(t)))
		if IsErr(err, proto.ErrUnknownDatabase) {
			t.Skip("got error during handshake")
		}
		require.NoError(t, err)
		err = client.Do(ctx, Query{
			Body:   "SELECT 1",
			Result: discardResult(),
		})
		require.True(t, IsErr(err, proto.ErrUnknownDatabase))
	})
}

func TestExceptionUnwrap(t *testing.T) {
	flat := &Exception{
		Code:    proto.ErrReadonly,
		Name:    "foo",
		Message: "bar",
	}

	if !errors.Is(flat, proto.ErrReadonly) {
		t.Fatal("flat exception must be the error code it represents")
	}
}
