# ch [![](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/go-faster/ch#section-documentation) [![](https://img.shields.io/codecov/c/github/go-faster/ch?label=cover)](https://codecov.io/gh/go-faster/ch) [![experimental](https://img.shields.io/badge/-experimental-blueviolet)](https://go-faster.org/docs/projects/status#experimental)

WIP TCP ClickHouse client in Go.

## Features
* OpenTelemetry support
* No reflection or `interface{}`
* Column-first design that is dramatically more efficient
  * Up to 40x faster than row-first design around `sql`
  * Up to 500x faster than HTTP API
  * Low memory overhead (column blocks are slices, i.e. continuous memory)
* Rigorously tested
  * ARM, Windows, Mac, Linux (also x86)
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
* As close to ClickHouse as possible

## Supported types
* UInt8, UInt16, UInt32, UInt64
* Int8, Int16, Int32, Int64
* UInt128, Int128
* String
* Array(T)

## Reference

* [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
* [clickhouse-go](https://github.com/ClickHouse/clickhouse-go)
* [python driver](https://github.com/mymarilyn/clickhouse-driver)
