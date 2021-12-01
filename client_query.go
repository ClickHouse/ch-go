package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/ch/internal/proto"
)

// CancelQuery cancels query.
func (c *Client) CancelQuery(ctx context.Context) error {
	proto.ClientCodeCancel.Encode(c.buf)
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

// SendQuery starts query.
func (c *Client) SendQuery(ctx context.Context, query, queryID string) error {
	c.encode(proto.Query{
		ID:          queryID,
		Body:        query,
		Secret:      "",
		Stage:       proto.StageComplete,
		Compression: c.compression,
		Info: proto.ClientInfo{
			ProtocolVersion: c.info.ProtocolVersion,
			Major:           c.info.Major,
			Minor:           c.info.Minor,
			Patch:           0,
			Interface:       proto.InterfaceTCP,
			Query:           proto.ClientQueryInitial,

			InitialUser:    "",
			InitialQueryID: "",
			InitialAddress: c.conn.LocalAddr().String(),
			OSUser:         "",
			ClientHostname: "",
			ClientName:     c.info.Name,

			Span:     trace.SpanContextFromContext(ctx),
			QuotaKey: "",
		},
	})

	// Blank data as EOF.
	c.encode(proto.ClientData{})

	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}
