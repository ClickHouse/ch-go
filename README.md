# ch [![](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/ClickHouse/ch-go#section-documentation)
Low level TCP [ClickHouse](https://clickhouse.com/) client and protocol implementation in Go. Designed for very fast data block streaming with low network, cpu and memory overhead.

Use [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) for high-level `database/sql`-compatible client.

* [Feedback](https://github.com/ClickHouse/ch-go/discussions/6)
* [Benchmarks](https://github.com/ClickHouse/ch-go-bench#benchmarks)
* [Protocol reference](https://go-faster.org/docs/clickhouse)

*[ClickHouse](https://clickhouse.com/) is an open-source, high performance columnar OLAP database management system for real-time analytics using SQL.*

```console
go get github.com/ClickHouse/ch-go@latest
```

## Example
```go
package main

import (
  "context"
  "fmt"

  "github.com/ClickHouse/ch-go"
  "github.com/ClickHouse/ch-go/proto"
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
  if err := c.Do(ctx, ch.Query{
    Body: "SELECT number FROM system.numbers LIMIT 500000000",
    // OnResult will be called on next received data block.
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
```

```
393ms 0.5B rows  4GB  10GB/s 1 job
874ms 2.0B rows 16GB  18GB/s 4 jobs
```

### Results

To stream query results, set `Result` and `OnResult` fields of [Query](https://pkg.go.dev/github.com/ClickHouse/ch-go#Query).
The `OnResult` will be called after `Result` is filled with received data block.

The `OnResult` is optional, but query will fail if more than single block is received, so it is ok to solely set the `Result`
if only one row is expected.

#### Automatic result inference
```go
var result proto.Results
q := ch.Query{
  Body:   "SELECT * FROM table",
  Result: result.Auto(),
}
```

#### Single result with column name inference
```go
var res proto.ColBool
q := ch.Query{
  Body:   "SELECT v FROM test_table",
  Result: proto.ResultColumn{Data: &res},
}
```

## Features
* OpenTelemetry support
* No reflection or `interface{}`
* Generics (go1.18) for `Array[T]`, `LowCardinaliy[T]`, `Map[K, V]`, `Nullable[T]`
* **Column**-oriented design that operates directly with **blocks** of data
  * [Dramatically more efficient](https://github.com/ClickHouse/ch-go-bench)
  * Up to 100x faster than row-first design around `sql`
  * Up to 700x faster than HTTP API
  * Low memory overhead (data blocks are slices, i.e. continuous memory)
  * Highly efficient input and output block streaming
  * As close to ClickHouse as possible
* Structured query execution telemetry streaming
  * Query progress
  * Profiles
  * Logs
  * [Profile events](https://github.com/ClickHouse/ClickHouse/issues/26177)
* LZ4, ZSTD or *None* (just checksums for integrity check) compression
* [External data](https://clickhouse.com/docs/en/engines/table-engines/special/external-data/) support
* Rigorously tested
  * Windows, Mac, Linux (also x86)
  * Unit tests for encoding and decoding
    * ClickHouse **Server** in **Go** for faster tests
    * Golden files for all packets, columns
    * Both server and client structures
    * Ensuring that partial read leads to failure
  * End-to-end [tests](.github/workflows/e2e.yml)
    - 21.8.13.6-lts
    - 21.9.6.24-stable
    - 21.10.4.26-stable
    - 21.11.10.1-stable
    - 21.12.3.32-stable
    - 22.1.3.7-stable
  * Fuzzing

## Supported types
* UInt8, UInt16, UInt32, UInt64, UInt128, UInt256
* Int8, Int16, Int32, Int64, Int128, Int256
* Date, Date32, DateTime, DateTime64
* Decimal32, Decimal64, Decimal128, Decimal256 (only low-level raw values)
* IPv4, IPv6
* String, FixedString(N)
* UUID
* Array(T)
* Enum8, Enum16
* LowCardinality(T)
* Map(K, V)
* Bool
* Tuple(T1, T2, ..., Tn)
* Nullable(T)
* Point

## Generics

### ArrayOf

Generic for `Array(T)`

```go
// Array(String)
arr := proto.NewArray[string](new(proto.ColStr))
// Or
arr := new(proto.ColStr).Array()
q := ch.Query{
  Body:   "SELECT ['foo', 'bar', 'baz']::Array(String) as v",
  Result: arr.Results("v"),
}
// Do ...
arr.Row(0) // ["foo", "bar", "baz"]
```

## TODO
- [ ] Types
  - [ ] [Decimal(P, S)](https://clickhouse.com/docs/en/sql-reference/data-types/decimal/) API
  - [ ] JSON
  - [ ] SimpleAggregateFunction
  - [ ] AggregateFunction
  - [ ] Nothing
  - [ ] Interval
  - [ ] Nested
- [ ] Reading and writing *Native* format dumps
- [ ] Improved i/o timeout handling for reading packets from server
  - [ ] Close connection on context cancellation in all cases
  - [ ] Ensure that reads can't block forever

## Reference
* [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
* [clickhouse-go](https://github.com/ClickHouse/clickhouse-go)
* [python driver](https://github.com/mymarilyn/clickhouse-driver)

## License
Apache License 2.0
