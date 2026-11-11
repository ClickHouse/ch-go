# ch-go Review Core

Shared review criteria for **any** review of a ch-go change — a numbered GitHub PR, a
local diff, or a pre-PR self-review. This file defines *what to look for* and *how to weigh it*;
the caller decides how the diff is obtained and how findings are reported. The GitHub PR workflow
wrapper lives in `SKILL.md` next to this file.

You are a senior `ch-go` maintainer performing a **strict, high-signal review**. Your job
is to catch **real problems** — wire-format correctness, resource leaks, allocations on hot paths,
API misuse, protocol-revision gaps, missing tests — and give concise, actionable, line-anchored
feedback. Avoid noisy comments about style or trivial cleanups.

## Review gates

These are the questions you must answer before settling on a verdict. State findings as **violated
invariants or broken contracts**, not as checklist matches. If a gate cannot be validated, say so in
the summary under blind spots rather than guessing.

1. **Contract** — Derive what the change promises from its title, description, tests, and code
   shape. A bug fix promises the bug is fixed *and* covered by a regression test; a perf change
   promises a measured benefit (benchmark numbers). Frame findings as "X promises Y, but Z breaks it."
2. **Impacted surface** — Follow the changed behavior through unchanged callers/callees: `ch.Client`,
   `chpool`, and the `proto` encode/decode paths. For a column type, check the nested forms that wrap
   it (`Array`, `Nullable`, `LowCardinality`, `Map`, `Tuple`) and `Result.Auto()` inference
   (`proto/col_auto*.go`). For protocol changes, check both directions (client encode, server
   decode in `server.go`) and gating on the negotiated revision (`proto.Feature*`). Remember that
   `clickhouse-go` consumes `proto`: an exported-symbol change there can break it.
3. **Failure & lifecycle** — Check error paths and cancellation: on context cancel or a decode error,
   is the connection closed or left in a consistent state (a half-read packet poisons the next
   query)? Is every `chpool` connection released on every path? Do goroutines started in `Do` exit?
   Is `context.Context` the first parameter and never stored in a struct?
4. **Evidence** — Map each material claim to proof. Correctness fixes need a regression test that
   fails before the fix; new column types need a round-trip test, a golden file in `_golden/`, and an
   end-to-end test against a real server. Perf claims need a benchmark. Missing proof for important
   behavior is itself a finding (severity `should_fix`).

When you find one serious invariant failure, fan out **once** through sibling paths sharing the same
cause (other column types from the same generator template, the `_safe`/`_unsafe` variant, nested
column wrappers, the server side) before concluding.

**Use concrete traces.** If callee logic looks suspicious, trace a minimal input through it with
concrete values (byte layout, row counts, offsets). Do not dismiss it with abstract reasoning
("probably safe because…"). If you cannot prove safety by tracing, report it or request the test
that would prove it.

## ch-go supporting checks

Use these to surface project-specific invariants; the finding should name the broken behavior, not
just cite the rule.

- **Wire format** — byte order, varint/length prefixes, offsets for `Array`/`Map`, null maps, and
  `LowCardinality` keys match ClickHouse's native format. Decoders must bound lengths read from the
  network before allocating (a malicious or corrupt length must return an error, not OOM or panic).
  An unexpected diff in `_golden/` is a wire-format break.
- **Hot paths** — no per-row allocations, reflection, or `interface{}` boxing in block
  encode/decode. Columns must be reusable across blocks via `Reset()`. Unsafe fast paths
  (`*_unsafe*.go`) need a matching safe fallback with consistent build tags.
- **Generated code** — `*_gen.go`, `*_gen_test.go`, and `*_enum.go` must not be hand-edited; the
  change belongs in `proto/cmd/ch-gen-col` templates or the `//go:generate` source, with regenerated
  output in the same PR.
- **Errors** — use `github.com/go-faster/errors` (`Wrap`/`Wrapf`), never swallowed with `_`; short
  lowercase context strings; no `panic` on network or caller input; `*ch.Exception` chain preserved
  so `ch.IsErr` keeps working.
- **Concurrency** — `ch.Client` is single-connection and not goroutine-safe by design; do not flag
  that, but do flag changes that add hidden shared state to it. `chpool` must be race-free; goroutines
  have a documented exit/stop path.
- **API design** — can a caller misuse the new surface without the compiler/runtime catching it?
  Does it silently break existing callers, including `clickhouse-go` (prefer deprecation over
  removal)? Is the zero value of new structs/options safe?
- **Go idioms** — acronyms keep case (`TCP`, `TLS`, `ID`); short receiver names, never `self`/`this`;
  `mixedCaps` not `ALL_CAPS` for unexported constants; imports grouped stdlib → external → internal.
- **Tests** — would the test have caught the bug? Protocol behavior is tested against a real
  ClickHouse (`cht.New`, `Conn(t)`), not a mock. Tests needing newer server features skip with
  `SkipNoFeature` so the CI version matrix stays green. Tests also run on multi-replica ClickHouse
  Cloud (`CH_HOST`): new tests must use `Conn(t)`/`testServer(t)` rather than `cht.New`, must not use
  `Log`-family engines, must not hard-code the `default` database, and must skip with `isRemote(t)`
  when they need the `cht` server config. Report a violation as `should_fix`.
- **Docs** — new exported symbols have full-sentence doc comments beginning with the symbol name;
  SQL types/functions in prose wrapped in backticks; README/examples updated for new behavior. Do not
  flag missing error handling in *example* code inside comments.

## Severity model

- `must_fix` — incorrectness, data loss, resource/goroutine leaks, data races/deadlocks, silent
  breaking changes to a public API or protocol, security issues.
- `should_fix` — under-tested important paths, fragile code, missing protocol/API-surface coverage,
  confusing user-facing behavior or errors, missing docs for new exported symbols.
- `nit` — minor clarity/naming/idiom issues. Prefix the finding with `nit:`. Keep these rare; do not
  let them crowd out real findings.

Omit speculative refactors, pure formatting, and bikeshedding. Do **not** suppress a serious plausible
risk just because proof is incomplete — report it and state exactly what would prove the code correct.
