package ch

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/internal/gold"
	"github.com/ClickHouse/ch-go/proto"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func ConnOpt(t testing.TB, opt Options) *Client {
	t.Helper()

	ctx := context.Background()
	server := cht.New(t)

	if opt.Logger == nil {
		opt.Logger = zaptest.NewLogger(t)
	}

	opt.Address = server.TCP
	client, err := Dial(ctx, opt)
	require.NoError(t, err)

	t.Log("Connected", client.ServerInfo())
	t.Cleanup(func() {
		require.NoError(t, client.Close())
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
		server := cht.New(t)
		conn, err := Dial(ctx, Options{
			Address: server.TCP,
		})
		require.NoError(t, err)
		require.NoError(t, conn.Ping(ctx))
		require.NoError(t, conn.Close())
		require.ErrorIs(t, conn.Ping(ctx), ErrClosed)
		require.ErrorIs(t, conn.Do(ctx, Query{}), ErrClosed)
	})
	t.Run("DatabaseNotFound", func(t *testing.T) {
		ctx := context.Background()
		server := cht.New(t)
		client, err := Dial(ctx, Options{
			Address:  server.TCP,
			Database: "bad",
		})
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
