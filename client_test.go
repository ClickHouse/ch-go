package ch

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/go-faster/ch/internal/cht"
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
	server := cht.Connect(t)

	if opt.Logger == nil {
		opt.Logger = zaptest.NewLogger(t)
	}

	client, err := Dial(ctx, server.TCP, opt)
	require.NoError(t, err)

	t.Log("Connected", client.serverInfo(), client.Location())
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return client
}

func Conn(t testing.TB) *Client {
	return ConnOpt(t, Options{})
}
