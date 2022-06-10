package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"go.uber.org/multierr"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func run(ctx context.Context) (re error) {
	var arg struct {
		Count     int
		Profile   string
		Trace     string
		Numbers   int
		BlockSize int
		Address   string
	}
	flag.IntVar(&arg.Count, "n", 1, "count")
	flag.IntVar(&arg.Numbers, "numbers", 500_000_000, "numbers count")
	flag.IntVar(&arg.BlockSize, "block-size", 65_536, "maximum row count in block")
	flag.StringVar(&arg.Profile, "profile", "", "cpu profile")
	flag.StringVar(&arg.Trace, "trace", "", "trace")
	flag.StringVar(&arg.Address, "addr", "localhost:9000", "server address")
	flag.Parse()

	if arg.Profile != "" {
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
	}
	if arg.Trace != "" {
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
	}

	c, err := ch.Dial(ctx, ch.Options{
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
		duration := time.Since(start)
		gotBytes := uint64(arg.Numbers * 8)
		fmt.Println(duration.Round(time.Millisecond), arg.Numbers, "rows",
			humanize.Bytes(gotBytes),
			humanize.Bytes(uint64(float64(gotBytes)/duration.Seconds()))+"/s",
		)
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
