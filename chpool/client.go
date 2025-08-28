package chpool

import (
	"context"

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

	// calling async since connIsHealthy may block
	go func() {
		if c.p.connIsHealthy(c.res) {
			c.p.options.ClientOptions.Logger.Debug("chpool: releasing connection")
			c.res.Release()
		} else {
			c.p.options.ClientOptions.Logger.Debug("chpool: destoying connection")
			c.res.Destroy()
		}
	}()
}

func (c *Client) Do(ctx context.Context, q ch.Query) (err error) {
	return c.client().Do(ctx, q)
}

func (c *Client) Ping(ctx context.Context) error {
	return c.client().Ping(ctx)
}

func (c *Client) Close() error {
	var err error

	client := c.client()
	if !client.IsClosed() {
		err = client.Close()
	}

	c.res.Destroy()

	return err
}

func (c *Client) client() *ch.Client {
	return c.res.Value().client
}
