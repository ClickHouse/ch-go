package ch

import (
	"context"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/ch/internal/proto"
)

func (c *Client) CancelQuery(ctx context.Context) error {
	proto.ClientCodeCancel.Encode(c.buf)
	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func (c *Client) SendQuery(ctx context.Context, query, queryID string) error {
	proto.ClientCodeQuery.Encode(c.buf)
	c.buf.PutString(queryID)

	if c.server.Has(proto.FeatureClientWriteInfo) {
		info := proto.ClientInfo{
			Revision:  c.info.Revision,
			Major:     c.info.Major,
			Minor:     c.info.Minor,
			Patch:     0,
			Interface: proto.ClientInterfaceTCP,
			Query:     proto.ClientQueryInitial,

			InitialUser:    "",
			InitialQueryID: "",
			InitialAddress: "",
			OSUser:         "",
			ClientHostname: "",
			ClientName:     c.info.Name,

			Span:     trace.SpanContextFromContext(ctx),
			QuotaKey: "",
		}
		info.EncodeAware(c.buf, c.server.Revision)
	}

	// Settings.
	if c.server.Has(proto.FeatureSettingsSerializedAsStrings) {
		for _, s := range c.settings {
			c.buf.PutString(s.Key)
			if s.Important {
				c.buf.PutInt(1)
			} else {
				c.buf.PutInt(0)
			}
			c.buf.PutString(s.Value)
		}
	}
	c.buf.PutString("") // end of settings

	if c.server.Has(proto.FeatureInterServerSecret) {
		c.buf.PutString("") // ?
	}

	proto.StageComplete.Encode(c.buf)
	c.compression.Encode(c.buf)

	c.buf.PutString(query)

	if err := c.flush(ctx); err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}
