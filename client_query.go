package ch

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/ch/internal/proto"
)

// cancelQuery cancels query.
func (c *Client) cancelQuery(ctx context.Context) error {
	proto.ClientCodeCancel.Encode(c.buf)
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

// sendQuery starts query.
func (c *Client) sendQuery(ctx context.Context, query, queryID string) error {
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

// Query to ClickHouse.
type Query struct {
	Query   string
	QueryID string         // optional
	Columns []proto.Column // optional
}

// Query performs Query on ClickHouse server.
func (c *Client) Query(ctx context.Context, q Query) error {
	if q.QueryID == "" {
		q.QueryID = uuid.New().String()
	}
	if err := c.sendQuery(ctx, q.Query, q.QueryID); err != nil {
		return errors.Wrap(err, "send")
	}

	var block proto.Block

Fetch:
	for {
		if ctx.Err() != nil {
			_ = c.cancelQuery(context.Background())
			return errors.Wrap(ctx.Err(), "canceled")
		}

		code, err := c.packet()
		if err != nil {
			return errors.Wrap(err, "packet")
		}

		switch code {
		case proto.ServerCodeData:
			if proto.FeatureTempTables.In(c.info.ProtocolVersion) {
				v, err := c.reader.Str()
				if err != nil {
					return errors.Wrap(err, "temp table")
				}
				_ = v
			}
			if err := block.DecodeBlock(c.reader, c.info.ProtocolVersion, q.Columns); err != nil {
				return errors.Wrap(err, "decode block")
			}
		case proto.ServerCodeException:
			e, err := c.exception()
			if err != nil {
				return errors.Wrap(err, "decode exception")
			}
			return e
		case proto.ServerCodeProgress:
			p, err := c.progress()
			if err != nil {
				return errors.Wrap(err, "progress")
			}
			_ = p
		case proto.ServerCodeProfile:
			p, err := c.profile()
			if err != nil {
				return errors.Wrap(err, "profile")
			}
			_ = p
		case proto.ServerCodeEndOfStream:
			break Fetch
		default:
			return errors.Errorf("unexpected code %s", code)
		}
	}

	return nil
}
