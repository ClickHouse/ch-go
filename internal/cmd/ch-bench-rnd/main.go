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

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func run(ctx context.Context) error {
	var arg struct {
		CPUProfile string
	}
	flag.StringVar(&arg.CPUProfile, "cpuprofile", "", "write cpu profile to `file`")
	flag.Parse()

	if arg.CPUProfile != "" {
		f, err := os.Create(arg.CPUProfile)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		if err := pprof.StartCPUProfile(f); err != nil {
			return errors.Wrap(err, "start cpu profile")
		}
		defer pprof.StopCPUProfile()
	}

	c, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = c.Close()
	}()

	var (
		rows       uint64
		totalBytes uint64
		data       proto.ColUInt32
	)
	start := time.Now()
	if err := c.Do(ctx, ch.Query{
		Body: "SELECT v FROM test_values",
		OnProgress: func(ctx context.Context, p proto.Progress) error {
			rows += p.Rows
			totalBytes += p.Bytes
			return nil
		},
		Result: proto.Results{
			{Name: "v", Data: &data},
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
