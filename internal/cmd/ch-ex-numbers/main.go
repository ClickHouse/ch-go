package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	ctx := context.Background()
	c, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		panic(err)
	}
	var (
		numbers int
		data    proto.ColUInt64
	)
	if err := c.Do(ctx, ch.Query{
		Body: "SELECT number FROM system.numbers LIMIT 500000000",
		OnResult: func(ctx context.Context, b proto.Block) error {
			numbers += len(data)
			return nil
		},
		Result: proto.Results{
			{Name: "number", Data: &data},
		},
	}); err != nil {
		panic(err)
	}
	fmt.Println("numbers:", numbers)
}
