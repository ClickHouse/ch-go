package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"golang.org/x/sync/errgroup"

	"github.com/ClickHouse/ch-go/proto"
)

func run(ctx context.Context) error {
	ln, err := net.Listen("tcp4", "127.0.0.1:9001")
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	// Preparing data.
	data := new(proto.Buffer)
	const (
		rows   = 65535
		blocks = 500_000_000 / rows
		chunk  = 3
		chunks = blocks / chunk
	)
	{
		proto.ServerCodeData.Encode(data)
		data.PutString("") // no temp table
		var col proto.ColUInt64
		for i := uint64(0); i < rows; i++ {
			col.Append(i)
		}
		block := proto.Block{
			Info:    proto.BlockInfo{BucketNum: -1},
			Columns: 1,
			Rows:    rows,
		}
		input := []proto.InputColumn{
			{Name: "number", Data: col},
		}
		if err := block.EncodeBlock(data, proto.Version, input); err != nil {
			return errors.Wrap(err, "prepare data")
		}
	}

	var raw []byte
	for i := 0; i < chunk; i++ {
		raw = append(raw, data.Buf...)
	}

	fmt.Println("starting", "with chunk of", humanize.Bytes(uint64(len(raw))))

	process := func(conn net.Conn) error {
		go func() {
			// Drain input.
			_, _ = io.Copy(io.Discard, conn)
		}()
		defer func() { _ = conn.Close() }()
		b := new(proto.Buffer)
		b.EncodeAware(&proto.ServerHello{
			Name:        "ch-bench-server",
			Major:       0,
			Minor:       12,
			Revision:    proto.Version,
			Timezone:    "UTC",
			DisplayName: "Bench",
			Patch:       1,
		}, proto.Version)
		if _, err := conn.Write(b.Buf); err != nil {
			return errors.Wrap(err, "write server hello")
		}

		for i := 0; i < chunks; i++ {
			if _, err := conn.Write(raw); err != nil {
				return errors.Wrap(err, "write chunk")
			}
		}

		// End of data.
		b.Reset()
		proto.ServerCodeEndOfStream.Encode(b)
		if _, err := conn.Write(b.Buf); err != nil {
			return errors.Wrap(err, "write end of stream")
		}

		return nil
	}

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				conn, err := ln.Accept()
				if err != nil {
					return errors.Wrap(err, "accept")
				}
				if err := process(conn); err != nil {
					return errors.Wrap(err, "process")
				}
			}
		})
	}

	return g.Wait()
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
