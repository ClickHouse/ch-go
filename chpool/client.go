package chpool

import (
	"context"
	"github.com/go-faster/ch"
	puddle "github.com/jackc/puddle/puddleg"
	"time"
)

type Client struct {
	res *puddle.Resource[*connResource]
	p   *Pool
}

func (c *Client) Release() {
	if c.res == nil {
		return
	}

	client := c.client()

	if client.IsClosed() || time.Now().Sub(c.res.CreationTime()) > c.p.options.MaxConnLifetime {
		c.res.Destroy()
		return
	}

	c.res.Release()
}

func (c *Client) Do(ctx context.Context, q ch.Query) (err error) {
	return c.client().Do(ctx, q)
}

func (c *Client) client() *ch.Client {
	return c.res.Value().client
}
