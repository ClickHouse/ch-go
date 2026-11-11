package chpool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/internal/e2e"
	"github.com/ClickHouse/ch-go/proto"
)

// testServer returns client options to connect to a new, empty test server.
// See testServer in the ch package tests.
func testServer(t testing.TB) ch.Options {
	t.Helper()

	r, ok := e2e.GetRemote(t)
	if !ok {
		server := cht.New(t)
		return ch.Options{Address: server.TCP}
	}

	opt := ch.Options{
		Address:     r.Address,
		User:        r.User,
		Password:    r.Password,
		TLS:         r.TLS,
		DialTimeout: e2e.RemoteDialTimeout,
		ReadTimeout: e2e.RemoteReadTimeout,
	}
	opt.Database = e2e.Database(t, func(ctx context.Context, query string) error {
		client, err := ch.Dial(ctx, opt)
		if err != nil {
			return err
		}
		return errors.Join(client.Do(ctx, ch.Query{Body: query}), client.Close())
	})
	return opt
}

func PoolConnOpt(t testing.TB, opt Options) *Pool {
	t.Helper()

	ctx := context.Background()
	server := testServer(t)

	if opt.ClientOptions.Logger == nil {
		opt.ClientOptions.Logger = zaptest.NewLogger(t)
	}

	c := &opt.ClientOptions
	c.Address = server.Address
	c.User = server.User
	c.Password = server.Password
	c.Database = server.Database
	c.TLS = server.TLS
	c.DialTimeout = server.DialTimeout
	c.ReadTimeout = server.ReadTimeout

	pool, err := Dial(ctx, opt)
	// A replica that did not get the new database yet returns
	// UNKNOWN_DATABASE, so try again.
	for i := 0; i < 20 && ch.IsErr(err, proto.ErrUnknownDatabase); i++ {
		time.Sleep(500 * time.Millisecond)
		pool, err = Dial(ctx, opt)
	}
	require.NoError(t, err)

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func PoolConn(t testing.TB) *Pool {
	return PoolConnOpt(t, Options{})
}

type IDo interface {
	Do(ctx context.Context, q ch.Query) (err error)
}

func testDo(t *testing.T, do IDo) {
	var (
		numbers int
		data    proto.ColUInt64
	)

	err := do.Do(context.Background(), ch.Query{
		Body: "SELECT number FROM system.numbers LIMIT 10",
		OnResult: func(ctx context.Context, b proto.Block) error {
			numbers += len(data)
			return nil
		},
		Result: proto.Results{
			{Name: "number", Data: &data},
		},
	})

	require.NoError(t, err)
	require.Equal(t, 10, numbers)
}

func waitForReleaseToComplete() {
	time.Sleep(500 * time.Millisecond)
}
