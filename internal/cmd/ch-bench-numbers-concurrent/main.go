package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func run(ctx context.Context) (re error) {
	var arg struct {
		Jobs    int
		Profile string
		Rows    int
	}
	flag.IntVar(&arg.Jobs, "j", 4, "jobs")
	flag.IntVar(&arg.Rows, "n", 50_000_0000, "rows")
	flag.StringVar(&arg.Profile, "profile", "", "memory profile")
	flag.Parse()

	if arg.Profile != "" {
		f, err := os.Create(arg.Profile)
		if err != nil {
			return errors.Wrap(err, "create profile")
		}
		defer func() {
			if err := f.Close(); err != nil {
				re = multierr.Append(re, err)
			}

			fmt.Println("Done, profile wrote to", arg.Profile)
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			return errors.Wrap(err, "start profile")
		}
		defer pprof.StopCPUProfile()
	}

	var (
		gotRows  atomic.Uint64
		gotBytes atomic.Uint64
	)
	g, ctx := errgroup.WithContext(ctx)
	start := time.Now()
	for i := 0; i < arg.Jobs; i++ {
		g.Go(func() error {
			c, err := ch.Dial(ctx, ch.Options{})
			if err != nil {
				return errors.Wrap(err, "dial")
			}
			var data proto.ColUInt64
			if err := c.Do(ctx, ch.Query{
				Body: "SELECT number FROM system.numbers_mt LIMIT 500000000",
				OnProgress: func(ctx context.Context, p proto.Progress) error {
					gotBytes.Add(p.Bytes)
					return nil
				},
				OnResult: func(ctx context.Context, block proto.Block) error {
					gotRows.Add(uint64(len(data)))
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
	fmt.Println(duration.Round(time.Millisecond), gotRows.Load(), "rows",
		humanize.Bytes(gotBytes.Load()),
		humanize.Bytes(uint64(float64(gotBytes.Load())/duration.Seconds()))+"/s",
		arg.Jobs, "jobs",
	)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
