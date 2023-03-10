package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go/proto"
)

func run() error {
	// For example, we have the following table:
	/*
		CREATE TABLE IF NOT EXISTS events
		(
		    event Enum8('WatchEvent'=1, 'PushEvent'=2, 'IssuesEvent'=3, 'PullRequestEvent'=4),
		    repo Int64,
		    actor Int64,
		    time DateTime
		)
		    ENGINE = ReplacingMergeTree
		        PARTITION BY (toMonth(time), event)
		        ORDER BY (event, repo, time);
	*/
	// You can use clickhouse-local to load and play with data:
	//
	// clickhouse local --structure "event Enum8('WatchEvent'=1, 'PushEvent'=2, 'IssuesEvent'=3, 'PullRequestEvent'=4), repo Int64, actor Int64, time DateTime" --input-format Native --interactive --file events.native
	//
	// Also, to load data from a file, you can use clickhouse-client:
	//
	// clickhouse-client --query "INSERT INTO estimator.events FORMAT Native" < events.native
	//
	// See test.sh.
	var (
		colEv      proto.ColEnum8 // raw Enum8 column without value inference
		colRepoID  proto.ColInt64
		colActorID proto.ColInt64
		colTime    proto.ColDateTime

		buf proto.Buffer
	)

	// Generate some random data and append to columns.
	r := rand.New(rand.NewSource(42)) // #nosec: G404
	t := time.Date(2016, 10, 10, 23, 52, 44, 10541234, time.UTC)
	appendValues := func(n int) {
		for i := 0; i < n; i++ {
			t = t.Add(time.Duration(r.Int63n(1000)) * time.Millisecond)
			colEv.Append(proto.Enum8(r.Int63n(3) + 1))
			colRepoID.Append(r.Int63())
			colActorID.Append(r.Int63())
			colTime.Append(t)
		}
	}
	appendValues(5_000)

	// Create new proto.Input for our columns.
	// Note that we wrap raw Enum8 column to match the table structure.
	colEvEnum := proto.Wrap(&colEv, `'WatchEvent'=1, 'PushEvent'=2, 'IssuesEvent'=3, 'PullRequestEvent'=4`)
	input := proto.Input{
		{Name: "event", Data: colEvEnum},
		{Name: "repo", Data: &colRepoID},
		{Name: "actor", Data: &colActorID},
		{Name: "time", Data: &colTime},
	}
	write := func() error {
		// Write new block to io.Writer.
		//
		// You can write multiple blocks in sequence.
		b := proto.Block{
			Rows:    colEv.Rows(),
			Columns: len(input),
		}
		// Note that we are using version 54451, proto.Version will fail.
		if err := b.EncodeRawBlock(&buf, 54451, input); err != nil {
			return errors.Wrap(err, "encode")
		}

		// Write buffer to output io.Writer. In out case, it is os.Stdout.
		if _, err := os.Stdout.Write(buf.Buf); err != nil {
			return errors.Wrap(err, "write")
		}

		return nil
	}
	if err := write(); err != nil {
		return errors.Wrap(err, "write")
	}

	// You can append new block to output file after current block,
	// that is the way to deal with large data.
	//
	// To encode new block, reset the buffer and columns.
	input.Reset()
	buf.Reset()

	// Now you can fill columns and write them in new block.
	appendValues(5_000)
	if err := write(); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v", err)
	}
}
