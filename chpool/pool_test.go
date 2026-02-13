package chpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDial(t *testing.T) {
	t.Parallel()
	t.Run("Connect", func(t *testing.T) {
		t.Parallel()
		p := PoolConn(t)
		require.NoError(t, p.Ping(context.Background()))
	})
	t.Run("Create Min Pool", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{
			MinConns: 2,
		})
		defer p.Close()

		require.EqualValues(t, 2, p.Stat().TotalResources())
	})
	t.Run("Max Conn Lifetime", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{
			MaxConnLifetime: time.Millisecond * 250,
		})
		defer p.Close()

		c, err := p.Acquire(context.Background())
		require.NoError(t, err)

		time.Sleep(p.options.MaxConnLifetime)
		c.Release()
		waitForReleaseToComplete()

		stats := p.Stat()
		assert.EqualValues(t, 0, stats.TotalResources())
	})
}

func TestPool_Do(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)

	testDo(t, p)
	waitForReleaseToComplete()

	stats := p.Stat()
	assert.EqualValues(t, 0, stats.AcquiredResources())
	assert.EqualValues(t, 2, stats.AcquireCount())
}

func TestPool_Ping(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)

	require.NoError(t, p.Ping(context.Background()))
	waitForReleaseToComplete()

	stats := p.Stat()
	assert.EqualValues(t, 0, stats.AcquiredResources())
	assert.EqualValues(t, 2, stats.AcquireCount())
}

func TestPool_Acquire(t *testing.T) {
	t.Parallel()
	p := PoolConn(t)

	conn, err := p.Acquire(context.Background())
	assert.NoError(t, err)

	conn.Release()
	waitForReleaseToComplete()
	require.EqualValues(t, 2, p.Stat().AcquireCount())
}

func TestPool_backgroundHealthCheck(t *testing.T) {
	t.Parallel()
	var healthCheckCnt int64
	p := PoolConnOpt(t, Options{
		MinConns: 1,
		HealthCheckFunc: func(ctx context.Context, client *ch.Client) error {
			atomic.AddInt64(&healthCheckCnt, 1)
			return nil
		},
		HealthCheckPeriod: 500 * time.Millisecond,
	})
	p.checkMinConns()
	p.checkIdleConnsHealth()
	assert.GreaterOrEqual(t, int64(1), atomic.LoadInt64(&healthCheckCnt))

	hc := atomic.LoadInt64(&healthCheckCnt)
	time.Sleep(750 * time.Millisecond)
	assert.Equal(t, hc+1, atomic.LoadInt64(&healthCheckCnt))
}
