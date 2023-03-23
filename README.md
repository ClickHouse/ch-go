# ch [![](https://img.shields.io/badge/go-pkg-00ADD8)](https://pkg.go.dev/github.com/ClickHouse/ch-go#section-documentation)
Low level TCP [ClickHouse](https://clickhouse.com/) client and protocol implementation in Go. Designed for very fast data block streaming with low network, cpu and memory overhead.

NB: **No pooling, reconnects** and **not** goroutine-safe by default, only single connection.
Use [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) for high-level `database/sql`-compatible client,
pooling for ch-go is available as [chpool](https://pkg.go.dev/github.com/ClickHouse/ch-go/chpool) package.

* [Feedback](https://github.com/ClickHouse/ch-go/discussions/6)
* [Benchmarks](https://github.com/go-faster/ch-bench#benchmarks)
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
  c, err := ch.Dial(ctx, ch.Options{Address: "localhost:9000"})
  if err != nil {
    panic(err)
  }
  var (
    numbers int
    data    proto.ColUInt64
  )
  if err := c.Do(ctx, ch.Query{
    Body: "SELECT number FROM system.numbers LIMIT 500000000",
    Result: proto.Results{
      {Name: "number", Data: &data},
    },
    // OnResult will be called on next received data block.
    OnResult: func(ctx context.Context, b proto.Block) error {
      numbers += len(data)
      return nil
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

### Writing data

See [examples/insert](./examples/insert).

For table
```sql
CREATE TABLE test_table_insert
(
    ts                DateTime64(9),
    severity_text     Enum8('INFO'=1, 'DEBUG'=2),
    severity_number   UInt8,
    body              String,
    name              String,
    arr               Array(String)
) ENGINE = Memory
```

We prepare data block for insertion as follows:

```go
var (
	body      proto.ColStr
	name      proto.ColStr
	sevText   proto.ColEnum
	sevNumber proto.ColUInt8

	ts  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano) // DateTime64(9)
	arr = new(proto.ColStr).Array()                                   // Array(String)
	now = time.Date(2010, 1, 1, 10, 22, 33, 345678, time.UTC)
)

// Append 10 rows to initial data block.
for i := 0; i < 10; i++ {
	body.AppendBytes([]byte("Hello"))
	ts.Append(now)
	name.Append("name")
	sevText.Append("INFO")
	sevNumber.Append(10)
	arr.Append([]string{"foo", "bar", "baz"})
}

input := proto.Input{
	{Name: "ts", Data: ts},
	{Name: "severity_text", Data: &sevText},
	{Name: "severity_number", Data: sevNumber},
	{Name: "body", Data: body},
	{Name: "name", Data: name},
	{Name: "arr", Data: arr},
}
```

#### Single data block
```go
if err := conn.Do(ctx, ch.Query{
	// Or "INSERT INTO test_table_insert (ts, severity_text, severity_number, body, name, arr) VALUES"
	// Or input.Into("test_table_insert")
	Body: "INSERT INTO test_table_insert VALUES",
	Input: input,
}); err != nil {
	panic(err)
}
```

### Stream data
```go
// Stream data to ClickHouse server in multiple data blocks.
var blocks int
if err := conn.Do(ctx, ch.Query{
	Body:  input.Into("test_table_insert"), // helper that generates INSERT INTO query with all columns
	Input: input,

	// OnInput is called to prepare Input data before encoding and sending
	// to ClickHouse server.
	OnInput: func(ctx context.Context) error {
		// On OnInput call, you should fill the input data.
		//
		// NB: You should reset the input columns, they are
		// not reset automatically.
		//
		// That is, we are re-using the same input columns and
		// if we will return nil without doing anything, data will be
		// just duplicated.

		input.Reset() // calls "Reset" on each column

		if blocks >= 10 {
			// Stop streaming.
			//
			// This will also write tailing input data if any,
			// but we just reset the input, so it is currently blank.
			return io.EOF
		}

		// Append new values:
		for i := 0; i < 10; i++ {
			body.AppendBytes([]byte("Hello"))
			ts.Append(now)
			name.Append("name")
			sevText.Append("DEBUG")
			sevNumber.Append(10)
			arr.Append([]string{"foo", "bar", "baz"})
		}

		// Data will be encoded and sent to ClickHouse server after returning nil.
		// The Do method will return error if any.
		blocks++
		return nil
	},
}); err != nil {
	panic(err)
}
```

### Writing dumps in Native format

You can use `ch-go` to write ClickHouse dumps in [Native][native] format:

> The most efficient format. Data is written and read by blocks in binary format. For each block, the number of rows,
> number of columns, column names and types, and parts of columns in this block are recorded one after another.
> In other words, this format is “columnar” – it does not convert columns to rows.
> This is the format used in the native interface for interaction between servers,
> for using the command-line client, and for C++ clients.

[native]: https://clickhouse.com/docs/en/interfaces/formats/#native

See [./internal/cmd/ch-native-dump](./internal/cmd/ch-native-dump/main.go) for more sophisticated example.

Example:
```go
var (
    colK proto.ColInt64
    colV proto.ColInt64
)
// Generate some data.
for i := 0; i < 100; i++ {
    colK.Append(int64(i))
    colV.Append(int64(i) + 1000)
}
// Write data to buffer.
var buf proto.Buffer
input := proto.Input{
    {"k", colK},
    {"v", colV},
}
b := proto.Block{
    Rows:    colK.Rows(),
    Columns: len(input),
}
// Note that we are using version 54451, proto.Version will fail.
if err := b.EncodeRawBlock(&buf, 54451, input); err != nil {
    panic(err)
}

// You can write buf.Buf to io.Writer, e.g. os.Stdout or file.
var out bytes.Buffer
_, _ = out.Write(buf.Buf)

// You can encode multiple buffers in sequence.
//
// To do this, reset buf and all columns, append new values
// to columns and call EncodeRawBlock again.
buf.Reset()
colV.Reset()
colV.Reset()
```

## Features
* OpenTelemetry support
* No reflection or `interface{}`
* Generics (go1.18) for `Array[T]`, `LowCardinaliy[T]`, `Map[K, V]`, `Nullable[T]`
* [Reading or writing](#dumps) ClickHouse dumps in `Native` format
* **Column**-oriented design that operates directly with **blocks** of data
  * [Dramatically more efficient](https://github.com/ClickHouse/ch-bench)
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
  * End-to-end [tests](.github/workflows/e2e.yml) on multiple LTS and stable versions
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
* Nothing, Interval

## Enums

You can use automatic enum inference in `proto.ColEnum`, this will come with some performance penalty.

To use `proto.ColEnum8` and `proto.ColEnum16`, you need to explicitly provide DDL for them via `proto.Wrap`:

```go
var v proto.ColEnum8

const ddl = `'Foo'=1, 'Bar'=2, 'Baz'=3`
input := []proto.InputColumn{
  {Name: "v", Data: proto.Wrap(&v, ddl)},
}
```

## Generics

Most columns implement [proto.ColumnOf\[T\]](https://pkg.go.dev/github.com/ClickHouse/ch-go/proto#ColumnOf) generic constraint:
```go
type ColumnOf[T any] interface {
	Column
	Append(v T)
	AppendArr(vs []T)
	Row(i int) T
}
```

For example, [ColStr](https://pkg.go.dev/github.com/ClickHouse/ch-go/proto#ColStr) (and [ColStr.LowCardinality](https://pkg.go.dev/github.com/ClickHouse/ch-go/proto#ColStr.LowCardinality)) implements `ColumnOf[string]`.
Same for arrays: `new(proto.ColStr).Array()` implements `ColumnOf[[]string]`, column of `[]string` values.

### Array

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

## Dumps

### Reading

Use `proto.Block.DecodeRawBlock` on `proto.NewReader`:

```go
func TestDump(t *testing.T) {
	// Testing decoding of Native format dump.
	//
	// CREATE TABLE test_dump (id Int8, v String)
	//   ENGINE = MergeTree()
	// ORDER BY id;
	//
	// SELECT * FROM test_dump
	//   ORDER BY id
	// INTO OUTFILE 'test_dump_native.raw' FORMAT Native;
	data, err := os.ReadFile(filepath.Join("_testdata", "test_dump_native.raw"))
	require.NoError(t, err)
	var (
		dec    proto.Block
		ids    proto.ColInt8
		values proto.ColStr
	)
	require.NoError(t, dec.DecodeRawBlock(
		proto.NewReader(bytes.NewReader(data)),
		proto.Results{
			{Name: "id", Data: &ids},
			{Name: "v", Data: &values},
		}),
	)
}
```

### Writing

Use `proto.Block.EncodeRawBlock` with version `54451` on `proto.Buffer` with `Rows` and `Columns` set:

```go
func TestLocalNativeDump(t *testing.T) {
	ctx := context.Background()
	// Testing clickhouse-local.
	var v proto.ColStr
	for _, s := range data {
		v.Append(s)
	}
	buf := new(proto.Buffer)
	b := proto.Block{Rows: 2, Columns: 2}
	require.NoError(t, b.EncodeRawBlock(buf, 54451, []proto.InputColumn{
		{Name: "title", Data: v},
		{Name: "data", Data: proto.ColInt64{1, 2}},
	}), "encode")

	dir := t.TempDir()
	inFile := filepath.Join(dir, "data.native")
	require.NoError(t, os.WriteFile(inFile, buf.Buf, 0600), "write file")

	cmd := exec.Command("clickhouse-local", "local",
		"--logger.console",
		"--log-level", "trace",
		"--file", inFile,
		"--input-format", "Native",
		"--output-format", "JSON",
		"--query", "SELECT * FROM table",
	)
	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	cmd.Stdout = out
	cmd.Stderr = errOut

	t.Log(cmd.Args)
	require.NoError(t, cmd.Run(), "run: %s", errOut)
	t.Log(errOut)

	v := struct {
		Rows int `json:"rows"`
		Data []struct {
			Title string `json:"title"`
			Data  int    `json:"data,string"`
		}
	}{}
	require.NoError(t, json.Unmarshal(out.Bytes(), &v), "json")
	assert.Equal(t, 2, v.Rows)
	if assert.Len(t, v.Data, 2) {
		for i, r := range []struct {
			Title string `json:"title"`
			Data  int    `json:"data,string"`
		}{
			{"Foo", 1},
			{"Bar", 2},
		} {
			assert.Equal(t, r, v.Data[i])
		}
	}
}
```

## TODO
- [ ] Types
  - [ ] [Decimal(P, S)](https://clickhouse.com/docs/en/sql-reference/data-types/decimal/) API
  - [ ] JSON
  - [ ] SimpleAggregateFunction
  - [ ] AggregateFunction
  - [x] Nothing
  - [x] Interval
  - [ ] Nested
  - [ ] [Geo types](https://clickhouse.com/docs/en/sql-reference/data-types/geo/)
    - [x] Point
    - [ ] Ring
    - [ ] Polygon
    - [ ] MultiPolygon
- [ ] Improved i/o timeout handling for reading packets from server
  - [ ] Close connection on context cancellation in all cases
  - [ ] Ensure that reads can't block forever

## Reference
* [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
* [clickhouse-go](https://github.com/ClickHouse/clickhouse-go)
* [python driver](https://github.com/mymarilyn/clickhouse-driver)

## License
Apache License 2.0
