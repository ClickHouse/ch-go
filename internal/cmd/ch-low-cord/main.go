package main

import (
	"context"
	"fmt"
	"os"

	"github.com/go-faster/errors"

	"github.com/go-faster/ch"
	"github.com/go-faster/ch/proto"
)

func run(ctx context.Context) error {
	c, err := ch.Dial(ctx, "localhost:9000", ch.Options{})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = c.Close()
	}()

	if err := c.Query(ctx, ch.Query{
		Body: "DROP TABLE test_cardinality_table",
	}); err != nil && !ch.IsErr(err, proto.ErrUnknownTable) {
		return errors.Wrap(err, "create table")
	}
	if err := c.Query(ctx, ch.Query{
		Body: "CREATE TABLE test_cardinality_table (v LowCardinality(String)) ENGINE = TinyLog",
	}); err != nil {
		return errors.Wrap(err, "create table")
	}

	s := &proto.ColStr{}
	data := proto.ColLowCardinality{
		Key:   proto.KeyUInt8,
		Keys8: proto.ColUInt8{0, 1, 0, 1, 0, 1, 1, 1, 0, 0},
		Index: s,
	}
	s.Append("One")
	s.Append("Two")

	if err := c.Query(ctx, ch.Query{
		Body: "INSERT INTO test_cardinality_table VALUES",
		Input: []proto.InputColumn{
			{Name: "v", Data: &data},
		},
	}); err != nil {
		return errors.Wrap(err, "insert")
	}
	if err := c.Query(ctx, ch.Query{
		Body: "SELECT * FROM test_cardinality_table VALUES",
		Result: proto.Results{
			{Name: "v", Data: &data},
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
