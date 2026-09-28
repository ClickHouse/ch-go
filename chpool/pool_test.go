package chpool

import (
	"context"
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

func TestNew_InvalidAcquireStrategy(t *testing.T) {
	t.Parallel()
	_, err := New(context.Background(), Options{
		AcquireStrategy: AcquireStrategy(255),
	})
	require.ErrorContains(t, err, "AcquireStrategy")
}

func TestPool_AcquireStrategyOrder(t *testing.T) {
	t.Parallel()

	const conns = 4

	acquireAll := func(t *testing.T, p *Pool) []*Client {
		t.Helper()
		clients := make([]*Client, 0, conns)
		for i := 0; i < conns; i++ {
			c, err := p.Acquire(context.Background())
			require.NoError(t, err)
			clients = append(clients, c)
		}
		return clients
	}

	// releaseInOrder releases clients one by one and returns the
	// underlying connections in release order.
	releaseInOrder := func(clients []*Client) []*ch.Client {
		released := make([]*ch.Client, 0, len(clients))
		for _, c := range clients {
			released = append(released, c.client())
			c.Release()
		}
		return released
	}

	releaseAll := func(clients []*Client) {
		for _, c := range clients {
			c.Release()
		}
	}

	t.Run("FIFO", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{
			MaxConns:          conns,
			AcquireStrategy:   AcquireFIFO,
			HealthCheckPeriod: time.Hour,
		})

		released := releaseInOrder(acquireAll(t, p))

		reacquired := acquireAll(t, p)
		defer releaseAll(reacquired)
		require.EqualValues(t, conns, p.Stat().TotalResources())
		for i, c := range reacquired {
			require.Same(t, released[i], c.client(),
				"FIFO must return connections in release order (position %d)", i)
		}
	})

	t.Run("LIFO", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{
			MaxConns:          conns,
			HealthCheckPeriod: time.Hour,
		})

		released := releaseInOrder(acquireAll(t, p))

		reacquired := acquireAll(t, p)
		defer releaseAll(reacquired)
		require.EqualValues(t, conns, p.Stat().TotalResources())
		for i, c := range reacquired {
			require.Same(t, released[len(released)-1-i], c.client(),
				"LIFO must return connections in reverse release order (position %d)", i)
		}
	})
}

// TestPool_AcquireStrategyLoadSpread simulates connections to backends with
// unequal speed: each round acquires perRound connections and releases the
// connection to the "slow" backend last, so it is always the most recently
// released one. Under LIFO that connection tops the idle stack and is reused
// first every round, starving the rest; under FIFO all connections rotate.
// Single goroutine, no timing: fully deterministic.
func TestPool_AcquireStrategyLoadSpread(t *testing.T) {
	t.Parallel()

	const (
		conns    = 3
		perRound = 2 // must be < conns so LIFO can starve a connection
		rounds   = 30
	)

	runRounds := func(t *testing.T, p *Pool) (usage map[*ch.Client]int, firstAcquired []*ch.Client, slow *ch.Client) {
		t.Helper()
		usage = make(map[*ch.Client]int)

		// Warm up: create all connections and designate the slow one.
		// Released in reverse so the slow connection (warm[0]) ends up on
		// top of the LIFO idle stack and is the first acquire of round 0.
		warm := make([]*Client, 0, conns)
		for i := 0; i < conns; i++ {
			c, err := p.Acquire(context.Background())
			require.NoError(t, err)
			warm = append(warm, c)
		}
		slow = warm[0].client()
		for i := len(warm) - 1; i >= 0; i-- {
			warm[i].Release()
		}

		for r := 0; r < rounds; r++ {
			acquired := make([]*Client, 0, perRound)
			for i := 0; i < perRound; i++ {
				c, err := p.Acquire(context.Background())
				require.NoError(t, err)
				usage[c.client()]++
				acquired = append(acquired, c)
			}
			firstAcquired = append(firstAcquired, acquired[0].client())

			// Fast connections finish first; the slow one is
			// released last.
			var slowAcquired *Client
			for _, c := range acquired {
				if c.client() == slow {
					slowAcquired = c
					continue
				}
				c.Release()
			}
			if slowAcquired != nil {
				slowAcquired.Release()
			}
		}
		return usage, firstAcquired, slow
	}

	t.Run("LIFO concentrates load", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{MaxConns: conns, HealthCheckPeriod: time.Hour})
		usage, firstAcquired, slow := runRounds(t, p)

		// The slow connection is always on top of the stack and is
		// acquired first in every round.
		for r, c := range firstAcquired {
			require.Same(t, slow, c, "LIFO must acquire the slow connection first (round %d)", r)
		}
		// Only perRound connections ever serve queries, the rest
		// starve.
		require.Len(t, usage, perRound, "LIFO must starve connections beyond the top of the stack: %v", usage)
	})

	t.Run("FIFO spreads load", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{MaxConns: conns, AcquireStrategy: AcquireFIFO, HealthCheckPeriod: time.Hour})
		usage, _, _ := runRounds(t, p)

		// All connections rotate and each serves a fair share.
		require.Len(t, usage, conns)
		// Half of a perfectly even share is a comfortable floor: FIFO
		// deliberately under-serves the most recently released (slow)
		// connection, so counts are spread but not uniform.
		minShare := rounds * perRound / conns / 2
		for c, n := range usage {
			require.GreaterOrEqual(t, n, minShare, "connection %p starved: %v", c, usage)
		}
	})
}
