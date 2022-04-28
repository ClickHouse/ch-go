package ch

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/go-faster/ch/cht"
	"github.com/go-faster/ch/internal/gold"
	"github.com/go-faster/ch/proto"
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

func TestDial(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		conn := Conn(t)
		require.NoError(t, conn.Ping(context.Background()))
	})
	t.Run("DatabaseNotFound", func(t *testing.T) {
		ctx := context.Background()
		server := cht.New(t)
		client, err := Dial(ctx, Options{
			Address:  server.TCP,
			Database: "bad",
		})
		require.NoError(t, err)
		err = client.Do(ctx, Query{
			Body:   "SELECT 1",
			Result: discardResult(),
		})
		require.True(t, IsErr(err, proto.ErrUnknownDatabase))
	})
}
