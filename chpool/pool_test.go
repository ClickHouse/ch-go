package chpool

import (
	"context"
	"net"
	"sync"
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

// TestPool_AcquireStrategyNoExtraDial checks that Acquire does not dial a new
// connection while idle connections are available when acquires run
// concurrently. Every new dial fails, so an extra dial shows as an Acquire
// error. It also checks that each Acquire is counted in Stat.
//
// For FIFO, the health check also runs in a loop, because FIFO and the health
// check both drain the idle set. For LIFO, a health check that runs at the
// same time as Acquire can cause an extra dial too; that is not tested here.
func TestPool_AcquireStrategyNoExtraDial(t *testing.T) {
	t.Parallel()

	const (
		idle       = 4
		maxConns   = 10
		workers    = 2 // Less than idle, so an idle connection is always available.
		iterations = 5000
	)

	run := func(t *testing.T, strategy AcquireStrategy, healthCheck bool) {
		p := PoolConnOpt(t, Options{
			MaxConns:          maxConns,
			AcquireStrategy:   strategy,
			HealthCheckPeriod: time.Hour,
			// Keep the health check from destroying connections, so
			// that the idle count stays fixed.
			MaxConnIdleTime: time.Hour,
			MaxConnLifetime: time.Hour,
		})

		warm := make([]*Client, 0, idle)
		for i := 0; i < idle; i++ {
			c, err := p.Acquire(context.Background())
			require.NoError(t, err)
			warm = append(warm, c)
		}
		for _, c := range warm {
			c.Release()
		}
		require.EqualValues(t, idle, p.Stat().IdleResources())

		// Make each new dial fail quickly. The constructor reads
		// p.options when it dials. The test writes p.options before
		// the workers start.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		closedAddr := ln.Addr().String()
		require.NoError(t, ln.Close())
		p.options.ClientOptions.Address = closedAddr
		p.options.ClientOptions.DialTimeout = 100 * time.Millisecond

		acquiresBefore := p.Stat().AcquireCount()

		var (
			wg       sync.WaitGroup
			failures atomic.Int64
			firstErr atomic.Value
			stop     = make(chan struct{})
		)
		if healthCheck {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						p.checkIdleConnsHealth()
					}
				}
			}()
		}
		var workersWG sync.WaitGroup
		for w := 0; w < workers; w++ {
			workersWG.Add(1)
			go func() {
				defer workersWG.Done()
				for i := 0; i < iterations; i++ {
					c, err := p.Acquire(context.Background())
					if err != nil {
						failures.Add(1)
						firstErr.CompareAndSwap(nil, err)
						continue
					}
					c.Release()
				}
			}()
		}
		workersWG.Wait()
		close(stop)
		wg.Wait()

		stat := p.Stat()
		t.Logf("failures=%d total=%d idle=%d acquires=%d emptyAcquires=%d",
			failures.Load(), stat.TotalResources(), stat.IdleResources(),
			stat.AcquireCount()-acquiresBefore, stat.EmptyAcquireCount())
		require.Zero(t, failures.Load(),
			"Acquire dialed, but %d idle connections were available for %d workers: %v",
			idle, workers, firstErr.Load())
		require.EqualValues(t, idle, stat.TotalResources())
		require.EqualValues(t, workers*iterations, stat.AcquireCount()-acquiresBefore,
			"Stat must count every Acquire")
	}

	t.Run("LIFO", func(t *testing.T) {
		t.Parallel()
		run(t, AcquireLIFO, false)
	})
	t.Run("FIFO", func(t *testing.T) {
		t.Parallel()
		run(t, AcquireFIFO, true)
	})
}

func BenchmarkPool_Acquire(b *testing.B) {
	for _, bc := range []struct {
		name     string
		strategy AcquireStrategy
	}{
		{name: "LIFO", strategy: AcquireLIFO},
		{name: "FIFO", strategy: AcquireFIFO},
	} {
		b.Run(bc.name, func(b *testing.B) {
			p := PoolConnOpt(b, Options{
				MaxConns:          16,
				AcquireStrategy:   bc.strategy,
				HealthCheckPeriod: time.Hour,
			})
			// Create all connections before the measurement.
			warm := make([]*Client, 0, 16)
			for i := 0; i < 16; i++ {
				c, err := p.Acquire(context.Background())
				require.NoError(b, err)
				warm = append(warm, c)
			}
			for _, c := range warm {
				c.Release()
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c, err := p.Acquire(context.Background())
					if err != nil {
						b.Error(err)
						return
					}
					c.Release()
				}
			})
		})
	}
}

// TestPool_AcquireFIFOLockStops checks that a caller that waits for the FIFO
// lock stops when its ctx is done, and that Close does not wait for the lock.
// An acquire can hold the lock while pool.Acquire waits for a release, so a
// wait for the lock must not ignore ctx or block Close.
func TestPool_AcquireFIFOLockStops(t *testing.T) {
	t.Parallel()

	const wait = 5 * time.Second

	t.Run("Acquire", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{
			AcquireStrategy:   AcquireFIFO,
			HealthCheckPeriod: time.Hour,
		})
		lockFIFOForTest(t, p)

		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			c, err := p.Acquire(ctx)
			if err == nil {
				c.Release()
			}
			done <- err
		}()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(wait):
			t.Fatal("Acquire did not stop when its ctx was done")
		}
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()
		p := PoolConnOpt(t, Options{
			AcquireStrategy:   AcquireFIFO,
			HealthCheckPeriod: 10 * time.Millisecond,
		})
		lockFIFOForTest(t, p)
		// Let the health check start to wait for the lock.
		time.Sleep(50 * time.Millisecond)

		done := make(chan struct{})
		go func() {
			p.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(wait):
			t.Fatal("Close waited for the FIFO lock")
		}
	})
}

// lockFIFOForTest holds the FIFO lock until the test ends.
func lockFIFOForTest(t *testing.T, p *Pool) {
	t.Helper()
	require.True(t, p.lockFIFO(nil))
	t.Cleanup(p.unlockFIFO)
}
