package ch

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	proto2 "github.com/go-faster/ch/proto"
)

// cancelQuery cancels query.
func (c *Client) cancelQuery(ctx context.Context) error {
	proto2.ClientCodeCancel.Encode(c.buf)
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

// sendQuery starts query.
func (c *Client) sendQuery(ctx context.Context, query, queryID string) {
	if ce := c.lg.Check(zap.DebugLevel, "sendQuery"); ce != nil {
		ce.Write(
			zap.String("query", query),
			zap.String("query_id", queryID),
		)
	}
	c.encode(proto2.Query{
		ID:          queryID,
		Body:        query,
		Secret:      "",
		Stage:       proto2.StageComplete,
		Compression: c.compression,
		Info: proto2.ClientInfo{
			ProtocolVersion: c.info.ProtocolVersion,
			Major:           c.info.Major,
			Minor:           c.info.Minor,
			Patch:           0,
			Interface:       proto2.InterfaceTCP,
			Query:           proto2.ClientQueryInitial,

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

	// External tables end.
	c.encode(proto2.ClientData{})
}

// Query to ClickHouse.
type Query struct {
	// Body of query, like "SELECT 1".
	Body string
	// QueryID is ID of query, defaults to new UUIDv4.
	QueryID string

	// Input columns for INSERT operations.
	Input []proto2.InputColumn
	// Result columns for SELECT operations.
	Result []proto2.ResultColumn

	OnData     func(ctx context.Context) error
	OnProgress func(ctx context.Context, p proto2.Progress) error
	OnProfile  func(ctx context.Context, p proto2.Profile) error
}

func (c *Client) decodeBlock(ctx context.Context, q Query) error {
	if proto2.FeatureTempTables.In(c.info.ProtocolVersion) {
		v, err := c.reader.Str()
		if err != nil {
			return errors.Wrap(err, "temp table")
		}
		if v != "" {
			return errors.Errorf("unexpected temp table %q", v)
		}
	}
	var block proto2.Block
	if err := block.DecodeBlock(c.reader, c.info.ProtocolVersion, q.Result); err != nil {
		return errors.Wrap(err, "decode block")
	}
	if block.End() {
		return nil
	}
	if f := q.OnData; f != nil {
		if err := f(ctx); err != nil {
			return errors.Wrap(err, "data")
		}
	}
	return nil
}

// Query performs Query on ClickHouse server.
func (c *Client) Query(ctx context.Context, q Query) error {
	if q.QueryID == "" {
		q.QueryID = uuid.New().String()
	}

	c.sendQuery(ctx, q.Body, q.QueryID)

	if len(q.Input) > 0 {
		rows := q.Input[0].Data.Rows()
		c.encode(proto2.ClientData{
			Block: proto2.Block{
				Info: proto2.BlockInfo{
					BucketNum: -1,
				},
				Columns: len(q.Input),
				Rows:    rows,
			},
		})
		for _, col := range q.Input {
			if r := col.Data.Rows(); r != rows {
				return errors.Errorf("%q has %d rows, expected %d", col.Name, r, rows)
			}

			col.EncodeStart(c.buf)
			col.Data.EncodeColumn(c.buf)

			if err := c.flush(ctx); err != nil {
				return errors.Wrap(err, "flush")
			}
		}

		// End of data.
		c.encode(proto2.ClientData{})
	}

	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	for {
		if ctx.Err() != nil {
			_ = c.cancelQuery(context.Background())
			return errors.Wrap(ctx.Err(), "canceled")
		}
		code, err := c.packet(ctx)
		if err != nil {
			return errors.Wrap(err, "packet")
		}

		switch code {
		case proto2.ServerCodeData:
			if err := c.decodeBlock(ctx, q); err != nil {
				return errors.Wrap(err, "decode block")
			}
		case proto2.ServerCodeException:
			e, err := c.exception()
			if err != nil {
				return errors.Wrap(err, "decode exception")
			}
			return e
		case proto2.ServerCodeProgress:
			p, err := c.progress()
			if err != nil {
				return errors.Wrap(err, "progress")
			}
			if ce := c.lg.Check(zap.DebugLevel, "Progress"); ce != nil {
				ce.Write(
					zap.Uint64("rows", p.Rows),
					zap.Uint64("total_rows", p.TotalRows),
					zap.Uint64("bytes", p.Bytes),
					zap.Uint64("wrote_bytes", p.WroteBytes),
					zap.Uint64("wrote_rows", p.WroteRows),
				)
			}
			if f := q.OnProgress; f != nil {
				if err := f(ctx, p); err != nil {
					return errors.Wrap(err, "progress")
				}
			}
		case proto2.ServerCodeProfile:
			p, err := c.profile()
			if err != nil {
				return errors.Wrap(err, "profile")
			}
			if ce := c.lg.Check(zap.DebugLevel, "Profile"); ce != nil {
				ce.Write(
					zap.Uint64("rows", p.Rows),
					zap.Uint64("bytes", p.Bytes),
					zap.Uint64("blocks", p.Blocks),
				)
			}
			if f := q.OnProfile; f != nil {
				if err := f(ctx, p); err != nil {
					return errors.Wrap(err, "profile")
				}
			}
		case proto2.ServerCodeTableColumns:
			// Ignoring for now.
			var info proto2.TableColumns
			if err := c.decode(&info); err != nil {
				return errors.Wrap(err, "table columns")
			}
		case proto2.ServerCodeEndOfStream:
			return nil
		default:
			return errors.Errorf("unexpected code %s", code)
		}
	}
}
