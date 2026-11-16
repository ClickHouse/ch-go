package chpool

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"

	"github.com/go-faster/errors"
	"github.com/jackc/puddle/v2"
)

// Pool of connections to ClickHouse.
type Pool struct {
	pool    *puddle.Pool[*connResource]
	options Options

	// fifoMu serializes AcquireFIFO acquires. In AcquireFIFO mode, every
	// caller of pool.AcquireAllIdle must hold fifoMu. Otherwise an acquire
	// can find the idle set empty while idle connections exist, and dial.
	fifoMu sync.Mutex

	closeOnce sync.Once
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// AcquireStrategy controls which idle connection Acquire returns.
type AcquireStrategy uint8

const (
	// AcquireLIFO returns the most recently released connection. Under
	// light load, queries reuse a small set of connections and the other
	// connections age out via MaxConnIdleTime. The trade-off is that load
	// does not spread across connections, so if a load balancer sends
	// connections to different replicas, a few replicas get most queries.
	// This is the default.
	AcquireLIFO AcquireStrategy = iota
	// AcquireFIFO returns the least recently released connection,
	// rotating through all idle connections. Use it when connections
	// land on heterogeneous backends behind a load balancer: it
	// spreads queries evenly instead of concentrating load on the
	// slowest backend. The trade-off is that all idle connections stay
	// in rotation, so the pool does not shrink via MaxConnIdleTime
	// while it stays under load.
	// Each acquire scans the idle set, which costs O(n) in the number of
	// idle connections and serializes concurrent acquires. The order is
	// best-effort: a connection released during an acquire can be
	// returned before older ones.
	AcquireFIFO
)

// Options for Pool.
type Options struct {
	ClientOptions     ch.Options
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	MaxConns          int32
	MinConns          int32
	HealthCheckPeriod time.Duration
	// AcquireStrategy controls which idle connection Acquire returns.
	// The default is AcquireLIFO.
	AcquireStrategy AcquireStrategy
}

// Defaults for pool.
const (
	DefaultMaxConnLifetime   = time.Hour
	DefaultMaxConnIdleTime   = time.Minute * 30
	DefaultHealthCheckPeriod = time.Minute
)

func (o *Options) setDefaults() {
	if o.MaxConnLifetime == 0 {
		o.MaxConnLifetime = DefaultMaxConnLifetime
	}
	if o.MaxConnIdleTime == 0 {
		o.MaxConnIdleTime = DefaultMaxConnIdleTime
	}
	if o.MaxConns == 0 {
		o.MaxConns = int32(runtime.NumCPU())
	}
	if o.HealthCheckPeriod == 0 {
		o.HealthCheckPeriod = DefaultHealthCheckPeriod
	}
}

// Dial returns a pool of connections to ClickHouse.
// Checks if ClickHouse is available, fails if not.
func Dial(ctx context.Context, opt Options) (*Pool, error) {
	return newPool(ctx, opt, true)
}

// New returns a pool of connections to ClickHouse.
func New(ctx context.Context, opt Options) (*Pool, error) {
	return newPool(ctx, opt, false)
}

func newPool(ctx context.Context, opt Options, dial bool) (*Pool, error) {
	opt.setDefaults()
	if opt.AcquireStrategy > AcquireFIFO {
		return nil, errors.Errorf("invalid AcquireStrategy %d", opt.AcquireStrategy)
	}
	p := &Pool{
		options:   opt,
		closeChan: make(chan struct{}),
	}
	puddleConfig := &puddle.Config[*connResource]{
		Constructor: func(ctx context.Context) (*connResource, error) {
			c, err := ch.Dial(ctx, p.options.ClientOptions)
			if err != nil {
				return nil, err
			}

			return &connResource{
				client:  c,
				clients: make([]Client, 64),
			}, nil
		},
		Destructor: func(c *connResource) {
			_ = c.client.Close()
		},
		MaxSize: opt.MaxConns,
	}

	pool, err := puddle.NewPool[*connResource](puddleConfig)
	if err != nil {
		return nil, err
	}
	p.pool = pool

	if err := p.createIdleResources(ctx, int(p.options.MinConns)); err != nil {
		p.Close()
		return nil, err
	}

	if dial {
		res, err := p.pool.Acquire(ctx)
		if err != nil {
			p.Close()
			return nil, err
		}
		res.Release()
	}

	p.wg.Add(1)
	go p.backgroundHealthCheck()

	return p, nil
}

// Acquire connection from pool.
func (p *Pool) Acquire(ctx context.Context) (*Client, error) {
	var (
		res *puddle.Resource[*connResource]
		err error
	)
	if p.options.AcquireStrategy == AcquireFIFO {
		res, err = p.acquireFIFO(ctx)
	} else {
		res, err = p.pool.Acquire(ctx)
	}
	if err != nil {
		return nil, err
	}

	return res.Value().getConn(p, res), nil
}

// acquireFIFO acquires the least recently used idle connection.
//
// Puddle keeps idle connections in a LIFO stack and has no option to change
// the order. So acquireFIFO takes all idle connections, puts the oldest one
// back last so that it is on top of the stack, and then pops it with
// pool.Acquire. pool.Acquire keeps Stat correct; AcquireAllIdle does not
// change Stat.
//
// fifoMu is held until pool.Acquire returns. All other callers of
// AcquireAllIdle also hold fifoMu, so no one can empty the idle set before
// pool.Acquire pops it. Idle connections do not hold a pool slot, so
// pool.Acquire does not dial or wait for a slot.
func (p *Pool) acquireFIFO(ctx context.Context) (*puddle.Resource[*connResource], error) {
	// Do not take the idle set if ctx is done: pool.Acquire fails at once.
	if ctx.Err() != nil {
		return p.pool.Acquire(ctx)
	}

	p.fifoMu.Lock()
	idle := p.pool.AcquireAllIdle()
	if len(idle) == 0 {
		p.fifoMu.Unlock()
		// No idle connection: dial or wait for a release. Do this
		// without fifoMu, because it can take a long time.
		return p.pool.Acquire(ctx)
	}
	oldest := 0
	for i, res := range idle {
		if res.LastUsedNanotime() < idle[oldest].LastUsedNanotime() {
			oldest = i
		}
	}
	// ReleaseUnused keeps LastUsedNanotime, so MaxConnIdleTime
	// enforcement is not changed.
	for i, res := range idle {
		if i != oldest {
			res.ReleaseUnused()
		}
	}
	idle[oldest].ReleaseUnused()
	res, err := p.pool.Acquire(ctx)
	p.fifoMu.Unlock()
	return res, err
}

func (p *Pool) Do(ctx context.Context, q ch.Query) (err error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()

	return c.Do(ctx, q)
}

func (p *Pool) Ping(ctx context.Context) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()

	return c.Ping(ctx)
}

func (p *Pool) backgroundHealthCheck() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.options.HealthCheckPeriod)
	for {
		select {
		case <-p.closeChan:
			ticker.Stop()
			return
		case <-ticker.C:
			p.checkIdleConnsHealth()
			p.checkMinConns()
		}
	}
}

func (p *Pool) checkIdleConnsHealth() {
	if p.options.AcquireStrategy == AcquireFIFO {
		// See fifoMu. Destroy closes connections in the background,
		// so fifoMu is not held during network I/O.
		p.fifoMu.Lock()
		defer p.fifoMu.Unlock()
	}
	resources := p.pool.AcquireAllIdle()

	now := time.Now()
	for _, res := range resources {
		if now.Sub(res.CreationTime()) > p.options.MaxConnLifetime {
			res.Destroy()
		} else if res.IdleDuration() > p.options.MaxConnIdleTime {
			res.Destroy()
		} else {
			res.ReleaseUnused()
		}
	}
}

func (p *Pool) checkMinConns() {
	for i := p.options.MinConns - p.pool.Stat().TotalResources(); i > 0; i-- {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = p.pool.CreateResource(ctx)
		}()
	}
}

func (p *Pool) createIdleResources(ctx context.Context, resourcesCount int) error {
	for i := 0; i < resourcesCount; i++ {
		err := p.pool.CreateResource(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Stat return pool statistic.
func (p *Pool) Stat() *puddle.Stat {
	return p.pool.Stat()
}

// Close pool.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeChan)
		p.wg.Wait()
		p.pool.Close()
	})
}
