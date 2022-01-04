package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/proto"
	"github.com/go-faster/errors"
	"go.uber.org/multierr"
)

func run(ctx context.Context) (re error) {
	var arg struct {
		Count     int
		Profile   string
		Trace     string
		Numbers   int
		BlockSize int
	}
	flag.IntVar(&arg.Count, "n", 20, "count")
	flag.IntVar(&arg.Numbers, "numbers", 500_000_000, "numbers count")
	flag.IntVar(&arg.BlockSize, "block-size", 65_536, "maximum row count in block")
	flag.StringVar(&arg.Profile, "profile", "cpu.out", "cpu profile")
	flag.StringVar(&arg.Trace, "trace", "trace.out", "trace")
	flag.Parse()

	cpuOut, err := os.Create(arg.Profile)
	if err != nil {
		return errors.Wrap(err, "create profile")
	}
	defer func() {
		if err := cpuOut.Close(); err != nil {
			re = multierr.Append(re, err)
		}
		fmt.Println("Done, profile wrote to", arg.Profile)
	}()
	if err := pprof.StartCPUProfile(cpuOut); err != nil {
		return errors.Wrap(err, "start profile")
	}
	defer pprof.StopCPUProfile()

	traceOut, err := os.Create(arg.Trace)
	if err != nil {
		return errors.Wrap(err, "create profile")
	}
	defer func() {
		if err := traceOut.Close(); err != nil {
			re = multierr.Append(re, err)
		}
		fmt.Println("Done, trace wrote to", arg.Trace)
	}()
	if err := trace.Start(traceOut); err != nil {
		return errors.Wrap(err, "start profile")
	}
	defer trace.Stop()

	c, err := ch.Dial(ctx, "localhost:9000", ch.Options{
		Settings: []ch.Setting{
			ch.SettingInt("max_block_size", arg.BlockSize),
		},
	})
	if err != nil {
		return errors.Wrap(err, "dial")
	}

	var data proto.ColUInt64
	for i := 0; i < arg.Count; i++ {
		start := time.Now()
		if err := c.Do(ctx, ch.Query{
			Body:     fmt.Sprintf("SELECT number FROM system.numbers_mt LIMIT %d", arg.Numbers),
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
