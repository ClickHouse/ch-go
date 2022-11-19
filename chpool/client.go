package chpool

import (
	"context"
	"time"

	"github.com/jackc/puddle/v2"

	"github.com/ClickHouse/ch-go"
)

// Client is an acquired *ch.Client from a Pool.
type Client struct {
	res *puddle.Resource[*connResource]
	p   *Pool
}

// Release returns client to the pool.
func (c *Client) Release() {
	if c.res == nil {
		return
	}

	client := c.client()

	if client.IsClosed() || time.Since(c.res.CreationTime()) > c.p.options.MaxConnLifetime {
		c.res.Destroy()
		return
	}

	c.res.Release()
}

func (c *Client) Do(ctx context.Context, q ch.Query) (err error) {
	return c.client().Do(ctx, q)
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client().Ping(ctx)
}

func (c *Client) client() *ch.Client {
	return c.res.Value().client
}
