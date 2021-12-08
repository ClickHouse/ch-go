package ch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/ch/internal/cht"
)

func Conn(t testing.TB) *Client {
	t.Helper()

	ctx := context.Background()
	server := cht.Connect(t)

	client, err := Dial(ctx, server.TCP, Options{})
	require.NoError(t, err)

	t.Log("Connected", client.serverInfo(), client.Location())
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return client
}
