package ch

import (
	"context"

	"github.com/go-faster/errors"

	proto2 "github.com/go-faster/ch/proto"
)

func (c *Client) Ping(ctx context.Context) error {
	c.buf.Encode(proto2.ClientCodePing)
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}
	p, err := c.packet(ctx)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	switch p {
	case proto2.ServerCodePong:
		return nil
	default:
		return errors.Errorf("unexpected packet %s", p)
	}
}
