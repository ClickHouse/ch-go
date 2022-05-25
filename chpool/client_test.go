package chpool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_Do(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)
	conn, err := p.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()

	testDo(t, conn)
}

func TestClient_Ping(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)

	conn, err := p.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()

	require.NoError(t, conn.Ping(context.Background()))
}
