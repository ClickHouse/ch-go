# ch [![](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/go-faster/ch#section-documentation) [![](https://img.shields.io/codecov/c/github/go-faster/ch?label=cover)](https://codecov.io/gh/go-faster/ch) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

WIP TCP ClickHouse client in Go.

```console
go get github.com/go-faster/ch
```

## Example
```go
package main

import (
  "context"
  "fmt"

  "github.com/go-faster/ch"
  "github.com/go-faster/ch/proto"
)

func main() {
  ctx := context.Background()
  c, err := ch.Dial(ctx, "localhost:9000", ch.Options{})
  if err != nil {
    panic(err)
  }
  var (
    numbers int
    data    proto.ColUInt64
  )
  if err := c.Query(ctx, ch.Query{
    Body: "SELECT number FROM system.numbers LIMIT 500000000",
    OnData: func(ctx context.Context) error {
      numbers += len(data)
      return nil
    },
    Result: []proto.ResultColumn{
      {Name: "number", Data: &data},
    },
  }); err != nil {
    panic(err)
  }
  fmt.Println("numbers:", numbers)
}
```

```
750ms  50B rows  4GB 5.3GB/s 1 job
 1.3s 2.5B rows 20GB  15GB/s 5 jobs
```

## Features
* OpenTelemetry support
* No reflection or `interface{}`
* Column-first design that is [dramatically more efficient](https://github.com/go-faster/ch-bench)
  * Up to 40x faster than row-first design around `sql`
  * Up to 500x faster than HTTP API
  * Low memory overhead (column blocks are slices, i.e. continuous memory)
  * As close to ClickHouse as possible
* Rigorously tested
  * **ARM**64, Windows, Mac, Linux (also x86)
  * Unit tests for encoding and decoding
    * Golden files for all packets, columns
    * Both server and client structures
    * Ensuring that partial read leads to failure
  * End-to-end [tests](.github/workflows/e2e.yml)
    * 21.8.11.4-lts
    * 21.9.6.24-stable
    * 21.10.4.26-stable
    * 21.11.4.14-stable
    * 21.12.1.8691-testing
  * Fuzzing
* Int128 and UInt128
* LZ4 compression

## Supported types
* UInt8, UInt16, UInt32, UInt64
* Int8, Int16, Int32, Int64
* UInt128, Int128
* IPv4, IPv6
* DateTime, DateTime64
* String
* FixedString(N)
* Array(T)

## TODO
- [ ] Enum8, Enum16
- [ ] LowCardinality
- [ ] Map(K, V)
- [ ] Nullable
- [ ] External tables
- [ ] Server in Go for tests
- [ ] Pooling
- [ ] ZSTD Compression

## Reference

* [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
* [clickhouse-go](https://github.com/ClickHouse/clickhouse-go)
* [python driver](https://github.com/mymarilyn/clickhouse-driver)
