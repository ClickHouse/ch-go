package ch

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go/cht"
	"github.com/ClickHouse/ch-go/proto"
)

func BenchmarkInsert(b *testing.B) {
	cht.Skip(b)
	srv := cht.New(b)

	bench := func(data proto.ColInput) func(b *testing.B) {
		return func(b *testing.B) {
			ctx := context.Background()
			c, err := Dial(ctx, Options{
				Address:     srv.TCP,
				Compression: CompressionDisabled,
			})
			if err != nil {
				b.Fatal(errors.Wrap(err, "dial"))
			}

			b.Cleanup(func() {
				if err := c.Do(ctx, Query{
					Body: "DROP TABLE IF EXISTS test_table",
				}); err != nil {
					b.Logf("Cleanup failed: %+v", err)
				}
				_ = c.Close()
			})
			if err := c.Do(ctx, Query{
				Body: fmt.Sprintf("CREATE TABLE IF NOT EXISTS test_table (row %s) ENGINE = Null", data.Type()),
			}); err != nil {
				b.Fatal(err)
			}

			var tmp proto.Buffer
			data.EncodeColumn(&tmp)

			b.SetBytes(int64(len(tmp.Buf)))
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := c.Do(ctx, Query{
					Body: "INSERT INTO test_table VALUES",
					Input: []proto.InputColumn{
						{Name: "row", Data: data},
					},
				}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	for _, gen := range []struct {
		name    string
		getData func(rows int) proto.ColInput
		maxRows int
	}{
		{
			"ColInt64",
			func(rows int) proto.ColInput {
				var data proto.ColInt64
				for i := 0; i < rows; i++ {
					data.Append(int64(i))
				}
				return data
			},
			-1,
		},
		{
			"SmallColStr",
			func(rows int) proto.ColInput {
				var data proto.ColStr
				for i := 0; i < rows; i++ {
					data.Append(fmt.Sprintf("%016x", i))
				}
				return data
			},
			1_000_000,
		},
		{
			"BigColStr",
			func(rows int) proto.ColInput {
				var (
					data    proto.ColStr
					scratch = strings.Repeat("abcd", 1024)
				)
				for i := 0; i < rows; i++ {
					data.Append(scratch)
				}
				return data
			},
			100_000,
		},
	} {
		b.Run(gen.name, func(b *testing.B) {
			for _, rows := range []int{
				10_000,
				100_000,
				1_000_000,
				10_000_000,
				100_000_000,
			} {
				if gen.maxRows > 0 && rows > gen.maxRows {
					continue
				}
				data := gen.getData(rows)

				b.Run(fmt.Sprintf("Rows%d", rows), bench(data))
			}
		})
	}
}
