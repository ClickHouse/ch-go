package chpool

import (
	"context"
	"runtime"
	"sort"
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

	// fifoMu serializes idle-stack reordering for AcquireFIFO.
	fifoMu sync.Mutex

	closeOnce sync.Once
	closeChan chan struct{}
	wg        sync.WaitGroup
}

// AcquireStrategy controls which idle connection Acquire returns.
type AcquireStrategy uint8

const (
	// AcquireLIFO returns the most recently released connection,
	// keeping a small set of connections warm. This is the default.
	AcquireLIFO AcquireStrategy = iota
	// AcquireFIFO returns the least recently released connection,
	// rotating through all idle connections. Use it when connections
	// land on heterogeneous backends behind a load balancer: it
	// spreads queries evenly instead of concentrating load on the
	// slowest backend. The trade-off is that all idle connections stay
	// in rotation, so the pool does not shrink via MaxConnIdleTime
	// while it stays under load.
	// Each acquire also reorders and reallocates the idle set, which
	// serializes concurrent acquires and costs O(n log n) in the number
	// of idle connections; ordering is best-effort when acquires race,
	// and Stat's EmptyAcquireCount and EmptyAcquireWaitTime may be
	// inflated because the reorder briefly drains the idle set.
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
	// Skip the reorder if ctx is already done: pool.Acquire will fail
	// immediately, and draining the idle set would penalize other callers.
	if p.options.AcquireStrategy == AcquireFIFO && ctx.Err() == nil {
		p.rotateIdleToFIFO()
	}
	res, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return res.Value().getConn(p, res), nil
}

// rotateIdleToFIFO reorders puddle's idle stack so that the least recently
// used connection is on top and is returned by the next pool.Acquire.
// Puddle always acquires in LIFO order and does not expose an ordering
// option, so the stack is reordered before each acquire instead.
func (p *Pool) rotateIdleToFIFO() {
	// Serialize reorders: two concurrent rotations would otherwise split
	// the idle set between them and re-push it interleaved instead of
	// globally ordered. A goroutine that already finished its own rotate
	// can still observe a momentarily drained idle list and dial a new
	// connection when the pool is below MaxConns, so FIFO ordering is
	// best-effort under concurrency and self-corrects on the next acquire.
	// The health check's AcquireAllIdle intentionally does not take fifoMu;
	// the worst case is one partial health cycle.
	p.fifoMu.Lock()
	defer p.fifoMu.Unlock()

	idle := p.pool.AcquireAllIdle()
	// Sort newest first so the oldest connection is pushed last and ends
	// up on top of the idle stack. ReleaseUnused keeps LastUsedNanotime
	// intact, so MaxConnIdleTime enforcement is unaffected. Stable sort
	// keeps puddle's newest-first stack order for equal timestamps.
	sort.SliceStable(idle, func(i, j int) bool {
		return idle[i].LastUsedNanotime() > idle[j].LastUsedNanotime()
	})
	for _, res := range idle {
		res.ReleaseUnused()
	}
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
