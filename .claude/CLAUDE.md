# Communication style

* Follow ASD-STE100

# ch-go — Agent Instructions

`github.com/ClickHouse/ch-go` is a low-level native TCP client and protocol implementation for
ClickHouse. It is designed for fast block streaming with low network, CPU, and memory overhead.
The high-level `database/sql` client is `clickhouse-go`, which uses `ch-go/proto` internally.

For usage, see `README.md`. Run tests with `make test` (`go.test.sh`: normal run, then `-race`).

---

## Packages at a Glance

| Package     | Responsibility |
|-------------|----------------|
| `ch` (root) | `Client`, `Dial`, `Options`, `Query`, `Do`, `Ping`, `Exception` |
| `proto`     | Wire protocol: packets, `Block`, `Buffer`/`Reader`, all column types (`Col*`) |
| `chpool`    | Connection pool on top of `ch.Client` |
| `compress`  | LZ4 / ZSTD block compression |
| `otelch`    | OpenTelemetry attribute keys |
| `ssh`       | SSH key authentication |
| `cht`       | Test helper: starts a local `clickhouse` server binary |
| `internal/` | `gold` (golden files), `e2e`, `ztest`, benchmarks and examples in `cmd/` |

A single `Client` is **one connection**. It has no pooling, no reconnects, and it is **not**
goroutine-safe. Do not add hidden goroutines or locks to `Client` to change this; concurrency
belongs in `chpool`.

`ch.Client` and `proto` are also consumed by `clickhouse-go`. A change to an exported `proto`
symbol can break that repo. Prefer additive changes.

---

## Go Idioms Enforced Here

Follow [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) in full. The rules below
are the ones that matter most in this codebase. `golangci-lint` (`.golangci.yml`) runs in CI.

**Errors**
- Use `github.com/go-faster/errors`, not stdlib `errors` or `fmt.Errorf`.
- Wrap with `errors.Wrap(err, "context")` / `errors.Wrapf`. Keep the message short and lowercase,
  no trailing punctuation — `"decode block"`, not `"failed to decode block."`.
- Never discard errors with `_`. Return, handle, or wrap.
- Do not `panic` in library code on bad input from the network or from the caller. Return an error.
- Server exceptions are `*ch.Exception`. Callers test them with `ch.IsErr(err, proto.ErrXxx)` /
  `ch.AsException`. Keep that chain intact when wrapping.

**Context**
- Every network call takes `context.Context` as its first parameter.
- Never store a `Context` in a struct field.

**Performance (hot paths in `proto` and block encode/decode)**
- Do not allocate per row. Reuse buffers; columns expose `Reset()` so callers can reuse them
  across blocks.
- Do not add reflection or `interface{}` boxing to encode/decode paths.
- A change that claims a speed-up must include a benchmark (`go test -bench`) with before/after
  numbers in the PR.
- Respect the `_safe` / `_unsafe` split for column types: unsafe fast paths need a safe fallback
  under the matching build tags.

**Naming**
- Receiver names: short abbreviation of the type (`c` for `Client`, `b` for `Buffer`).
- Acronyms keep their case: `TCP`, `TLS`, `ID`, `UUID`, `IPv4`.
- Unexported constants: `mixedCaps`, never `ALL_CAPS`.

**Imports**
- Three groups separated by blank lines: stdlib → external → internal (`github.com/ClickHouse/ch-go`).

---

## Generated Code

Do not edit generated files by hand. Edit the generator, then run `go generate ./...`.

- `proto/*_gen.go` and `proto/*_gen_test.go` come from `proto/cmd/ch-gen-col` (templates in that
  directory). Add new fixed-size numeric column types there.
- `*_enum.go` files come from `enumer` (`//go:generate` lines next to the type).

---

## Design Principle

**Make the API easy to use correctly and hard to use incorrectly.**

- `Query.Result` + `OnResult` stream blocks. Without `OnResult`, a query that returns more than one
  block fails instead of silently dropping data.
- Query parameters (`Query.Parameters`) prevent SQL injection. Never interpolate user input into
  query strings.
- Calls on a closed client return `ErrClosed` rather than panicking.

When proposing a new API surface, ask: *can a caller use this incorrectly without the compiler or
runtime catching it?* If yes, redesign.

---

## Workflow

- Do not commit directly to `main`. All changes go through a pull request.
- Do not rebase or amend commits on a shared branch — add new commits instead.
- Do not force-push to `main`.
- Bug fixes need a regression test that fails before the fix.
- New column type support requires: the `Col*` implementation in `proto/`, an encode/decode
  round-trip test, a golden file in `_golden/` (`gold.Bytes`), and an end-to-end test against a
  real server in the root package (`Conn(t)`).
- Golden files: update with `go test ./proto/... -update`. Review the diff of `_golden/` before
  committing; an unexpected change there is a wire-format break.
- Integration tests get a connection with `Conn(t)` / `ConnOpt(t, opt)` (root package) or
  `PoolConn(t)` (`chpool`). Do not call `cht.New` directly in a new test; use `testServer(t)` for
  the base `Options` if the test needs to call `Dial` itself. Do not mock the server for protocol
  behavior — test against a real ClickHouse.
- By default the helpers start a local server with `cht`, which needs a `clickhouse` binary
  (`CH_BIN`). With `CH_E2E=1` a missing binary is a failure, otherwise the test is skipped.
- With `CH_HOST` set (plus `CH_USER`, `CH_PASSWORD`, `CH_TLS`), the helpers connect to that server
  instead and give each connection a new `ch_go_test_*` database. The `Tests cloud` workflow
  (`tests:run-cloud` label) uses this with ClickHouse Cloud. Rules for tests that must pass there:
  - Do not qualify table names with a database (`default.t`); use the connection's database.
  - Use `MergeTree ORDER BY tuple()` (or a real sorting key). Cloud does not support the `Log`
    family (`TinyLog`, `Log`, `StripeLog`). Do not use `Memory` for a table that more than one
    connection reads: each connection can go to a different replica.
  - A test that depends on the `cht` server config (`system.opentelemetry_span_log`, clusters,
    custom users) must skip when `isRemote(t)` is true.
- Gate tests that need a newer server feature with `SkipNoFeature(t, client, proto.FeatureXxx)` so
  they skip on older servers in the CI version matrix.
- When referencing ClickHouse SQL types, functions, or log messages in text, wrap them in
  backticks: `` `DateTime64` ``, `` `MergeTree` ``.
