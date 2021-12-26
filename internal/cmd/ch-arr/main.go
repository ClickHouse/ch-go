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

	if err := c.Do(ctx, ch.Query{
		Body: "DROP TABLE test_array_table",
	}); err != nil && !ch.IsErr(err, proto.ErrUnknownTable) {
		return errors.Wrap(err, "create table")
	}
	if err := c.Do(ctx, ch.Query{
		Body: "CREATE TABLE test_array_table (v Array(String)) ENGINE = MergeTree ORDER BY v",
	}); err != nil {
		return errors.Wrap(err, "create table")
	}

	var data proto.ColStr
	arr := proto.ColArr{
		Data: &data,
	}
	data.ArrAppend(&arr, []string{"foo", "bar", "baz"})
	data.ArrAppend(&arr, []string{"Hello", "World!"})
	data.ArrAppend(&arr, []string{"", "", "0", "None", "False"})
	if err := c.Do(ctx, ch.Query{
		Body: "INSERT INTO test_array_table VALUES",
		Input: []proto.InputColumn{
			{Name: "v", Data: &arr},
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
