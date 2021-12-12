package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/proto"
)

func run(ctx context.Context) error {
	c, err := ch.Dial(ctx, "localhost:9000", ch.Options{})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = c.Close()
	}()

	var (
		rows       uint64
		totalBytes uint64
		data       proto.ColUInt64
	)
	start := time.Now()
	if err := c.Query(ctx, ch.Query{
		Body: "SELECT number FROM system.numbers LIMIT 500000000",
		OnProgress: func(ctx context.Context, p proto.Progress) error {
			rows += p.Rows
			totalBytes += p.Bytes
			return nil
		},
		Result: []proto.ResultColumn{
			{Name: "number", Data: &data},
		},
	}); err != nil {
		return errors.Wrap(err, "query")
	}

	duration := time.Since(start)
	fmt.Println(duration.Round(time.Millisecond), rows, "rows",
		humanize.Bytes(totalBytes),
		humanize.Bytes(uint64(float64(totalBytes)/duration.Seconds()))+"/s",
	)

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
