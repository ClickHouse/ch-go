package ch

import (
	"context"

	"github.com/go-faster/ch/internal/proto"
)

type Block struct {
}

func (c *Client) SendBlock(ctx context.Context, table string, b *Block) error {
	proto.ClientCodeData.Encode(c.buf)

	c.buf.PutString(table)

	return nil
}
