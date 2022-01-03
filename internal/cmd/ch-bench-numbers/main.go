package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/multierr"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/proto"
)

func run(ctx context.Context) (re error) {
	var arg struct {
		Count   int
		Profile string
	}
	flag.IntVar(&arg.Count, "n", 20, "count")
	flag.StringVar(&arg.Profile, "profile", "cpu.out", "memory profile")
	flag.Parse()

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

	c, err := ch.Dial(ctx, "localhost:9000", ch.Options{})
	if err != nil {
		return errors.Wrap(err, "dial")
	}

	var data proto.ColUInt64
	for i := 0; i < arg.Count; i++ {
		start := time.Now()
		if err := c.Do(ctx, ch.Query{
			Body:     "SELECT number FROM system.numbers_mt LIMIT 500000000",
			OnResult: func(ctx context.Context, block proto.Block) error { return nil },
			Result: proto.Results{
				{Name: "number", Data: &data},
			},
		}); err != nil {
			return errors.Wrap(err, "query")
		}
		fmt.Println(time.Since(start))
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
