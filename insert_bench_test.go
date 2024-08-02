package ch

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/proto"
)

func BenchmarkInsert(b *testing.B) {
	cht.Skip(b)
	srv := cht.New(b)

	bench := func(rows int) func(b *testing.B) {
		return func(b *testing.B) {
			ctx := context.Background()
			c, err := Dial(ctx, Options{
				Address:     srv.TCP,
				Compression: CompressionDisabled,
			})
			if err != nil {
				b.Fatal(errors.Wrap(err, "dial"))
			}
			defer func() { _ = c.Close() }()

			if err := c.Do(ctx, Query{
				Body: "CREATE TABLE IF NOT EXISTS test_table (id Int64) ENGINE = Null",
			}); err != nil {
				b.Fatal(err)
			}

			var id proto.ColInt64
			for i := 0; i < rows; i++ {
				id = append(id, 1)
			}

			b.SetBytes(int64(rows) * 8)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := c.Do(ctx, Query{
					Body: "INSERT INTO test_table VALUES",
					Input: []proto.InputColumn{
						{Name: "id", Data: id},
					},
				}); err != nil {
					b.Fatal()
				}
			}
		}
	}
	for _, rows := range []int{
		10_000,
		100_000,
		1_000_000,
		10_000_000,
		100_000_000,
	} {
		b.Run(fmt.Sprintf("Rows%d", rows), bench(rows))
	}
}
