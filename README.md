# ch [![](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/go-faster/ch#section-documentation) [![](https://img.shields.io/codecov/c/github/go-faster/ch?label=cover)](https://codecov.io/gh/go-faster/ch) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

TCP [ClickHouse](https://clickhouse.com/) client in Go. Designed for very fast data block streaming with low network, cpu and memory overhead.

Work in progress, please [leave feedback](https://github.com/go-faster/ch/discussions/6) on package API or features.
Also, see [benchmarks](https://github.com/go-faster/ch-bench#benchmarks) and [protocol reference](https://go-faster.org/docs/clickhouse).

*[ClickHouse](https://clickhouse.com/) is an open-source, high performance columnar OLAP database management system for real-time analytics using SQL.*

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

To stream query results, set `Result` and `OnResult` fields of [Query](https://pkg.go.dev/github.com/go-faster/ch#Query).
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
* Generics (go1.18) for `ArrayOf[T]`, `LowCardinaliyOf[T]`, `EnumOf[T]`
* **Column**-oriented design that operates with **blocks**
  * [Dramatically more efficient](https://github.com/go-faster/ch-bench)
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
* LZ4, ZSTD or *None* (just checksums) compression
* [External data](https://clickhouse.com/docs/en/engines/table-engines/special/external-data/) support
* Rigorously tested
  * **ARM**64, Windows, Mac, Linux (also x86)
  * Unit tests for encoding and decoding
    * ClickHouse **Server** in **Go** for faster tests
    * Golden files for all packets, columns
    * Both server and client structures
    * Ensuring that partial read leads to failure
  * End-to-end [tests](.github/workflows/e2e.yml)
    - 21.8.11.4-lts
    - 21.9.6.24-stable
    - 21.10.4.26-stable
    - 21.11.4.14-stable
    - 21.11.7.9-stable
    - 21.12.2.17-stable
  * Fuzzing

## Supported types
* UInt8, UInt16, UInt32, UInt64, UInt128, UInt256
* Int8, Int16, Int32, Int64, Int128, Int256
* Date, Date32, DateTime, DateTime64
* Decimal32, Decimal64, Decimal128, Decimal256
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

## TODO
- [ ] Connection pools
- [ ] TLS
- [ ] API UX Improvements (with 1.18 generics?)
    - [x] Enum
    - [x] LowCardinality
    - [ ] Array(T)
    - [ ] FixedString(N)
    - [ ] Map(K, V)
    - [ ] [Decimal(P, S)](https://clickhouse.com/docs/en/sql-reference/data-types/decimal/)
    - [ ] Nullable(T)
    - [ ] Tuple?
- [ ] Code generation from DDL
  - [ ] Parser
  - [ ] Code generator for SELECT/INSERT
  - [ ] Query builder
- [ ] DSL for DDL
- [ ] `database/sql` integration
- [ ] Reading and writing *Native* format dumps

## Reference

* [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
* [clickhouse-go](https://github.com/ClickHouse/clickhouse-go)
* [python driver](https://github.com/mymarilyn/clickhouse-driver)

## License
Apache License 2.0
