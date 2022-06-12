package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func run(ctx context.Context) error {
	c, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = c.Close()
	}()

	if err := c.Do(ctx, ch.Query{
		Body: "DROP TABLE test_cardinality_table",
	}); err != nil && !ch.IsErr(err, proto.ErrUnknownTable) {
		return errors.Wrap(err, "create table")
	}
	if err := c.Do(ctx, ch.Query{
		Body: "CREATE TABLE test_cardinality_table (v LowCardinality(String)) ENGINE = TinyLog",
	}); err != nil {
		return errors.Wrap(err, "create table")
	}

	s := &proto.ColStr{}
	data := s.LowCardinality()
	s.Append("One")
	s.Append("Two")

	if err := c.Do(ctx, ch.Query{
		Body: "INSERT INTO test_cardinality_table VALUES",
		Input: []proto.InputColumn{
			{Name: "v", Data: data},
		},
	}); err != nil {
		return errors.Wrap(err, "insert")
	}
	if err := c.Do(ctx, ch.Query{
		Body: "SELECT * FROM test_cardinality_table VALUES",
		Result: proto.Results{
			{Name: "v", Data: data},
		},
	}); err != nil {
		return errors.Wrap(err, "insert")
	}

	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}
