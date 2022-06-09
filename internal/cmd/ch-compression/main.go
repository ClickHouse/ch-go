package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func run(ctx context.Context) error {
	c, err := ch.Dial(ctx, ch.Options{
		Compression: ch.CompressionNone,
	})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() { _ = c.Close() }()

	var result proto.Results
	selectStr := ch.Query{
		Body:   "SELECT 'foo' AS s",
		Result: result.Auto(),
	}

	if err := c.Do(ctx, selectStr); err != nil {
		return errors.Wrap(err, "select")
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
