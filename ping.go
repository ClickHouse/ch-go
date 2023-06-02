package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/ClickHouse/ch-go/otelch"
	"github.com/ClickHouse/ch-go/proto"
)

// Ping server.
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
	c.buf.Encode(proto.ClientCodePing)
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
	default:
		return errors.Errorf("unexpected packet %s", p)
	}
}
