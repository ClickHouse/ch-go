package chpool

import (
	"context"
	"testing"
	"time"

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
