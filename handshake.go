package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/ch/proto"
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

		code, err := c.packet(ctx)
		if err != nil {
			return errors.Wrap(err, "packet")
		}
		expected := proto.ServerCodeHello
		if code != expected {
			return errors.Errorf("got %s instead of %s", code, expected)
		}
		if err := c.decode(&c.server); err != nil {
			return errors.Wrap(err, "decode server info")
		}

		if c.protocolVersion > c.server.Revision {
			// Downgrade to server version.
			c.protocolVersion = c.server.Revision
		}

		c.lg.Debug("Connected",
			zap.Int("client.protocol_version", c.info.ProtocolVersion),
			zap.Int("server.revision", c.server.Revision),
			zap.Int("protocol_version", c.protocolVersion),
			zap.Int("server.major", c.server.Major),
			zap.Int("server.minor", c.server.Minor),
			zap.Int("server.patch", c.server.Patch),
			zap.String("server", c.server.String()),
		)

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
