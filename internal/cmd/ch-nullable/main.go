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
		Body: "DROP TABLE test_nullable_table",
	}); err != nil && !ch.IsErr(err, proto.ErrUnknownTable) {
		return errors.Wrap(err, "create table")
	}
	if err := c.Do(ctx, ch.Query{
		Body: "CREATE TABLE test_nullable_table (v Nullable(String)) ENGINE = TinyLog",
	}); err != nil {
		return errors.Wrap(err, "create table")
	}

	var (
		values = &proto.ColStr{}
		data   = &proto.ColNullable{
			Values: values,
		}
	)

	for _, v := range []struct {
		Value string
		Null  bool
	}{
		{Value: "foo"},
		{Value: "", Null: true},
		{Value: "bar"},
		{Value: "baz"},
		{Value: "test", Null: true},
	} {
		values.Append(v.Value)
		if v.Null {
			data.Nulls = append(data.Nulls, 1)
		} else {
			data.Nulls = append(data.Nulls, 0)
		}
	}

	if err := c.Do(ctx, ch.Query{
		Body: "INSERT INTO test_nullable_table VALUES",
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
