package ch

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/go-faster/ch/internal/proto"
)

func (c *Client) handshake(ctx context.Context) error {
	c.buf.Reset()
	c.info.Encode(c.buf)
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	code, err := c.packet()
	if err != nil {
		return errors.Wrap(err, "packet")
	}
	expected := proto.ServerCodeHello
	if code != expected {
		return errors.Errorf("got %s instead of %s", code, expected)
	}
	if err := c.server.Decode(c.reader); err != nil {
		return errors.Wrap(err, "decode server info")
	}

	return nil
}
