package chpool

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"

	"github.com/jackc/puddle/v2"
)

// Pool of connections to ClickHouse.
type Pool struct {
	pool    *puddle.Pool[*connResource]
	options Options

	closeOnce sync.Once
	closeChan chan struct{}
}

// Options for Pool.
type Options struct {
	ClientOptions     ch.Options
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	MaxConns          int32
	MinConns          int32
	HealthCheckPeriod time.Duration
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

	go p.backgroundHealthCheck()

	return p, nil
}

// Acquire connection from pool.
func (p *Pool) Acquire(ctx context.Context) (*Client, error) {
	res, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return res.Value().getConn(p, res), nil
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
		p.pool.Close()
	})
}
