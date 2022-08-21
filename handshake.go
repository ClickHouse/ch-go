package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/ClickHouse/ch-go/otelch"
	"github.com/ClickHouse/ch-go/proto"
)

func (c *Client) encodeAddendum() {
	if proto.FeatureQuotaKey.In(c.protocolVersion) {
		c.buf.PutString(c.quotaKey)
	}
}

func (c *Client) handshake(ctx context.Context) error {
	handshakeCtx, cancel := context.WithCancel(context.Background())
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
		if code == proto.ServerCodeException {
			// Bad password, etc.
			e, err := c.exception()
			if err != nil {
				return errors.Wrap(err, "decode exception")
			}
			return errors.Wrap(e, "exception")
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
			zap.Int("protocol_version", c.protocolVersion),

			zap.Int("server.revision", c.server.Revision),
			zap.Int("server.major", c.server.Major),
			zap.Int("server.minor", c.server.Minor),
			zap.Int("server.patch", c.server.Patch),
			zap.String("server.name", c.server.String()),

			zap.Int("client.protocol_version", c.info.ProtocolVersion),
			zap.Int("client.major", c.version.Major),
			zap.Int("client.minor", c.version.Minor),
			zap.Int("client.patch", c.version.Patch),
			zap.String("client.name", c.version.Name),
		)
		if c.otel {
			trace.SpanFromContext(ctx).SetAttributes(
				otelch.ServerName(c.server.String()),
				otelch.ProtocolVersion(c.protocolVersion),
			)
		}
		if proto.FeatureAddendum.In(c.protocolVersion) {
			c.lg.Debug("Writing addendum")
			c.encodeAddendum()
			if err := c.flush(wgCtx); err != nil {
				return errors.Wrap(err, "flush")
			}
		}

		return nil
	})

	if err := wg.Wait(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Parent context is canceled, propagating error to allow error
			// traversal, like errors.Is(err, context.Canceled) assertion.
			return errors.Wrap(multierr.Append(err, ctxErr), "parent context done")
		}

		return errors.Wrap(err, "failed")
	}

	return nil
}
