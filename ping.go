package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"

	"github.com/ClickHouse/ch-go/otelch"
	"github.com/ClickHouse/ch-go/proto"
)

// Ping checks whether the server responds. A failed exchange closes the client
// unless the server returned a complete exception.
//
// Do not call concurrently with Do.
func (c *Client) Ping(ctx context.Context) (err error) {
	if c.IsClosed() {
		return ErrClosed
	}
	if c.otel {
		newCtx, span := c.tracer.Start(ctx, "Ping",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				otelch.ProtocolVersion(c.protocolVersion),
			),
		)
		ctx = newCtx
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed")
			} else {
				span.SetStatus(codes.Ok, "")
			}
			span.End()
		}()
	}
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "context")
	}

	decodedException := false
	defer func() {
		if err == nil || decodedException {
			return
		}
		if closeErr := c.Close(); closeErr != nil && !errors.Is(closeErr, ErrClosed) {
			err = multierr.Append(err, closeErr)
		}
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			err = multierr.Append(err, ctxErr)
		}
	}()

	c.writer.ChainBuffer(func(b *proto.Buffer) {
		b.Encode(proto.ClientCodePing)
	})
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	p, err := c.packet(ctx)
	if err != nil {
		return errors.Wrap(err, "read")
	}
	switch p {
	case proto.ServerCodePong:
		return nil
	case proto.ServerCodeException:
		e, err := c.exception()
		if err != nil {
			return errors.Wrap(err, "decode exception")
		}
		decodedException = true
		return errors.Wrap(e, "exception")
	default:
		return errors.Errorf("unexpected packet %s", p)
	}
}
