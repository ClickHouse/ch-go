package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/ch/internal/proto"
)

func (c *Client) handshake(ctx context.Context) error {
	handshakeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, wgCtx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		// Watchdog goroutine to handle context cancellation and
		// abort handshake with connection closing.
		select {
		case <-handshakeCtx.Done():
			// Handshake done, no more need for watchdog.
			return nil
		case <-ctx.Done():
			// Parent context done, should abort handshake.
			if err := c.conn.Close(); err != nil {
				return errors.Wrap(err, "close")
			}

			// Returning nil, because context error is already propagated by
			// error group.
			return nil
		}
	})
	wg.Go(func() error {
		defer cancel()

		c.buf.Reset()
		c.info.Encode(c.buf)
		if err := c.flush(wgCtx); err != nil {
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
	})

	if err := wg.Wait(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Parent context is cancel, propagating error to allow error
			// traversal, like errors.Is(err, context.Canceled) assertion.
			return errors.Wrap(multierr.Append(err, ctxErr), "parent context done")
		}

		return errors.Wrap(err, "failed")
	}

	return nil
}
