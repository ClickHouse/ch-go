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
		Body: "DROP TABLE test_map_table",
	}); err != nil && !ch.IsErr(err, proto.ErrUnknownTable) {
		return errors.Wrap(err, "create table")
	}
	if err := c.Do(ctx, ch.Query{
		Body: "CREATE TABLE test_map_table (v Map(String, String)) ENGINE = TinyLog",
	}); err != nil {
		return errors.Wrap(err, "create table")
	}

	var (
		keys   = &proto.ColStr{}
		values = &proto.ColStr{}
		data   = &proto.ColMap{
			Keys:   keys,
			Values: values,
		}
	)

	for _, v := range []struct {
		Key, Value string
	}{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	} {
		keys.Append(v.Key)
		values.Append(v.Value)
	}
	data.Offsets = proto.ColUInt64{
		2, // [0:2]
		3, // [2:3]
	}

	if err := c.Do(ctx, ch.Query{
		Body: "INSERT INTO test_map_table VALUES",
		Input: []proto.InputColumn{
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
