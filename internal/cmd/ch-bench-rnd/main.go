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

func formatNum(v uint64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%6.2f billion", float64(v)/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%6.2f million", float64(v)/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%6.2f thousand", float64(v)/1e3)
	default:
		return fmt.Sprintf("%d", v)
	}
}

func run(ctx context.Context) error {
	var arg struct {
		CPUProfile string
		Type       string
	}
	flag.StringVar(&arg.Type, "type", "", "column type to use")
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

	for _, data := range []proto.Column{
		new(proto.ColStr),
		new(proto.ColStr).Array(),
		new(proto.ColUInt8),
		new(proto.ColFloat64),
		new(proto.ColDateTime),
		new(proto.ColUInt64),
		new(proto.ColUInt64).Array(),
		new(proto.ColUInt64).Nullable(),
		new(proto.ColUUID),
	} {
		if arg.Type != "" && arg.Type != data.Type().String() {
			continue
		}
		var (
			rows       uint64
			totalBytes uint64
		)
		const (
			randSeed  = 1
			maxStrLen = 6
			maxArrLen = 3
			limit     = 500_000_000
		)
		start := time.Now()
		targetLimit := limit
		switch data.Type() {
		case "String":
			targetLimit = limit / maxStrLen
		case "Array(String)":
			targetLimit = limit / maxStrLen / maxArrLen
		}
		if err := c.Do(ctx, ch.Query{
			Body: fmt.Sprintf("SELECT v FROM generateRandom('v %s',  %d, %d, %d) LIMIT %d",
				data.Type(), randSeed, maxStrLen, maxArrLen, targetLimit,
			),
			OnResult: func(ctx context.Context, block proto.Block) error {
				return nil
			},
			OnProgress: func(ctx context.Context, p proto.Progress) error {
				rows += p.Rows
				totalBytes += p.Bytes
				return nil
			},
			Result: proto.Results{
				{Name: "v", Data: data},
			},
		}); err != nil {
			return errors.Wrap(err, "query")
		}
		duration := time.Since(start)
		fmt.Printf("%16s %8s %8s %5s %s rows/sec\n",
			data.Type(),
			duration.Round(time.Millisecond),
			humanize.Bytes(totalBytes),
			humanize.Bytes(uint64(float64(totalBytes)/duration.Seconds()))+"/sec",
			formatNum(uint64(float64(targetLimit)/duration.Seconds())),
		)
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
