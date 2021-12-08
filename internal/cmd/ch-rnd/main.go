package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/internal/proto"
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
		defer func() {
			_ = f.Close()
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			return errors.Wrap(err, "start cpu profile")
		}
		defer pprof.StopCPUProfile()
	}

	c, err := ch.Dial(ctx, "localhost:9000", ch.Options{})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = c.Close()
	}()
	var data proto.ColumnUInt32

	start := time.Now()

	var rows uint64
	if err := c.Query(ctx, ch.Query{
		Body: "SELECT v FROM test_values",
		OnProgress: func(ctx context.Context, p proto.Progress) error {
			rows += p.Rows
			return nil
		},
		Result: []proto.ResultColumn{
			{Name: "v", Data: &data},
		},
	}); err != nil {
		return errors.Wrap(err, "query")
	}

	fmt.Println(time.Since(start).Round(time.Millisecond), rows)

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
