package chpool

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go"
)

func TestClient_Do(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)
	conn, err := p.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()

	testDo(t, conn)
}

func TestClient_ReleaseHealthCheck(t *testing.T) {
	t.Parallel()
	var healthCheckCnt int64
	p := PoolConnOpt(t, Options{
		HealthCheckFunc: func(ctx context.Context, client *ch.Client) error {
			atomic.AddInt64(&healthCheckCnt, 1)
			return nil
		},
	})
	conn, err := p.Acquire(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), atomic.LoadInt64(&healthCheckCnt))

	conn.Release()
	waitForReleaseToComplete()
	assert.Equal(t, int64(1), atomic.LoadInt64(&healthCheckCnt))
}

func TestClient_Ping(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)

	conn, err := p.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()

	require.NoError(t, conn.Ping(context.Background()))
}

func TestClient_Close(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)
	conn, err := p.Acquire(context.Background())
	require.NoError(t, err)

	err = conn.Close()
	require.NoError(t, err)
	require.True(t, conn.client().IsClosed())
}
