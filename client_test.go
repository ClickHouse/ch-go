package ch

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/go-faster/ch/cht"
	"github.com/go-faster/ch/internal/gold"
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

	t.Log("Connected", client.ServerInfo(), client.Location())
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return client
}

func Conn(t testing.TB) *Client {
	return ConnOpt(t, Options{})
}
