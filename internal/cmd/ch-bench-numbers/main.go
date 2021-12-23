package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/proto"
)

func run(ctx context.Context) error {
	var arg struct {
		Workers int
	}
	flag.IntVar(&arg.Workers, "j", 2, "concurrent workers to use")
	flag.Parse()

	var (
		rows       uint64
		totalBytes uint64
	)

	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < arg.Workers; i++ {
		g.Go(func() error {
			c, err := ch.Dial(ctx, "localhost:9000", ch.Options{})
			if err != nil {
				return errors.Wrap(err, "dial")
			}
			defer func() {
				_ = c.Close()
			}()

			var data proto.ColUInt64
			if err := c.Query(ctx, ch.Query{
				Body: "SELECT number FROM system.numbers LIMIT 500000000",
				OnResult: func(ctx context.Context, block proto.Block) error {
					return nil
				},
				OnProgress: func(ctx context.Context, p proto.Progress) error {
					atomic.AddUint64(&rows, p.Rows)
					atomic.AddUint64(&totalBytes, p.Bytes)
					return nil
				},
				Result: proto.Results{
					{Name: "number", Data: &data},
				},
			}); err != nil {
				return errors.Wrap(err, "query")
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "wait")
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
