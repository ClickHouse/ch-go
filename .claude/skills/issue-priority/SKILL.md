---
name: issue-priority
description: >
  Decide the priority of a `ClickHouse/ch-go` issue: `Urgent`, `High`,
  `Medium`, or `Low`, for the org issue field **Priority**. Use this skill
  when the task is to "prioritize", "set the priority of", "rate", or "triage"
  an issue, or to explain why an issue has a given priority. It defines the
  reach tiers of the packages, the severity rows, the base-level matrix, the
  modifiers, and the confidence.
---

# Issue priority

This skill is the single source of truth for the **Priority** of
`ClickHouse/ch-go` issues. People and automated triage use the same rules.

The value goes into the org issue field **Priority** (`Urgent`, `High`,
`Medium`, `Low`). The field is visible to org members only. An automated tool
sets the value once, never changes a value that is already set, and keeps the
level and its reason out of public comments.

## When to set no priority

Set no priority when one of these is true:

- A maintainer can close the issue with no further action: it is already fixed
  on `main`, a duplicate, works as intended, already supported, answered, or
  obsolete.
- Only a deprecated public API reaches the defect (a `Deprecated:` paragraph in
  the Go doc comment, or docs that call the API deprecated; for example
  `Query.OnProfileEvent` and `Query.OnLog`). Fixes to deprecated code are not
  released.
- The issue is a question, or it is about CI, the release process, or a
  discussion.

## Vulnerabilities that are not public yet

A security issue must not stay public with a description of how to reproduce
it. This section applies when a reproduction or code inspection confirms a
vulnerability that is not public yet. The classification skill of this
repository defines it
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md), "A
vulnerability that is not public yet"): memory corruption is one also when only
a hostile server or network causes it, and a hang, or a crash with no memory
corruption, that only such a server or network causes is not one.

- Report it in a private GitHub vulnerability report for this repository
  (**Security** → **Report a vulnerability**), never in a public issue,
  comment, or pull request. If a public issue reports it, copy the title, the
  text, and the comments of the issue into the report first.
- Set no Priority on the public issue. Close it with a comment that gives no
  details, and ask a repository administrator to delete it: closing does not
  hide the text of the issue.
- Rate the vulnerability with the rules below, and put the level and its reason
  into the private report as the proposed severity.

A vulnerability that is already public (a published CVE or GHSA, for example in
a dependency), and an authentication method that does not work or is missing,
get a Priority on the issue as usual.

## 1. Reach

**Reach** tells how many applications can hit the defect. It comes from the
public API that leads to the code with the defect: an API that almost every
application calls has a high reach, an API that few applications use has a low
reach. There are four tiers, highest first:

- **Core:** almost every application calls this API.
- **Common:** many applications use it, but not all.
- **Narrow:** few applications use it (rare types, experimental APIs, special
  setups).
- **Satellite:** applications do not import it (examples, tests, docs, tools).

To find the tier, find the most-used public API that reaches the defect, and
look it up in the table below. Use the API, not the file that contains the
defect.

ch-go has two kinds of users: applications that call ch-go directly, and
`clickhouse-go`, which uses the ch-go column types and buffers (`proto`) and
the compression (`compress`) for all its users. The reach is the tier of the
most-used public path that reaches the defect, not the tier of the file that
contains it.

For code that clickhouse-go calls, the reach is the higher of two tiers: the
tier in the table below, and the tier that the clickhouse-go priority rules
([`.claude/skills/issue-priority/SKILL.md`](https://github.com/ClickHouse/clickhouse-go/blob/main/.claude/skills/issue-priority/SKILL.md))
give the clickhouse-go path that reaches the defect. For example, a
`proto.ColDateTime64` decode defect that clickhouse-go reaches through
`Rows.Scan` is Core. To find the calls, search a clickhouse-go checkout:
`grep -rn 'ClickHouse/ch-go/' --include=*.go`. clickhouse-go uses
`proto.Buffer`, `proto.Reader`, `proto.Block`, the server packets, the
`Col*` types of its scalar column types, and `compress`. It has its own
`Array`, `Map`, `Nullable`, `LowCardinality`, and `Tuple` code, so a defect in
`ColArr`, `ColMap`, `ColNullable`, `ColLowCardinality`, or `ColTuple` reaches
only direct ch-go users.

| Tier      | Public paths |
| --------- | ------------ |
| Core      | `ch.Dial` and `Client.Do` with `Query.Body`, `Query.Input`, `Query.Result`, `OnInput`, and `OnResult` (`proto.Input`, `proto.Results`); `Client.Ping`, `Client.Close`; the handshake, username and password authentication, TLS, timeouts, and context cancellation; errors (`*ch.Exception`, `ch.IsErr`, `ch.AsException`); block encoding and decoding (`proto.Block`, `proto.Buffer`, `proto.Reader`); compression (`compress`: LZ4, ZSTD); these column types: integers up to 64 bits, `ColFloat32`, `ColFloat64`, `ColBool`, `ColStr`, `ColFixedStr`, `ColDate`, `ColDate32`, `ColDateTime`, `ColDateTime64`, `ColDecimal32` to `ColDecimal256`, `ColUUID`, `ColEnum`, `ColEnum8`, `ColEnum16`, `ColNullable`, `ColLowCardinality`, `ColArr`, `ColMap`, `ColTuple` |
| Common    | `chpool`; `proto.ColAuto` and `proto.Results.Auto`; query parameters (`Query.Parameters`); `Query.Settings`, `Query.QueryID`, `Query.QuotaKey`; the callbacks `OnProgress`, `OnProfile`, `OnProfileEvents`, `OnLogs`; external data (`Query.ExternalData`, `Query.ExternalTable`); `ColRaw`; the column types `ColIPv4`, `ColIPv6`, `ColInt128`, `ColInt256`, `ColUInt128`, `ColUInt256` |
| Narrow    | the other column types: `ColPoint`, `ColInterval`, `ColTime`, `ColTime64`, `ColBFloat16`, `ColQBit`, `ColNothing`, `ColJSONStr`, `ColLowCardinalityRaw`; SSH key authentication (`ssh`, `Options.SSHSigner`); OpenTelemetry (`otelch`, `Options.TracerProvider`, `Options.MeterProvider`); a custom dialer (`Options.Dialer`); inter-server queries (`Query.Secret`, `Query.InitialUser`); `Options.ProtocolVersion` |
| Satellite | `cht` (the test helper that starts a local server), `internal/` (`gold`, `e2e`, `ztest`, `cmd/`), `examples/`, `_golden/`, `README.md`, the agent skills and agents in `.claude/` |

## 2. Severity

First name the **impacts** of the issue with the classification skill of this
repository
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md)): none,
one, or more of Security (part vulnerability or part authentication), Data
integrity, Stability, and Performance. Most issues have none.

Then use the first severity row that fits. With more than one impact, the
highest row applies:

| Severity                | Use when |
| ----------------------- | -------- |
| Wrong data or security  | impact **Data integrity**: the client returns or stores wrong or incomplete data with no error, or reports success for a query that failed; or impact **Security** with a vulnerability: a security defect |
| Broken, no workaround   | the feature fails (an error, a hang, a panic, a leak that ends in a crash or in exhausted connections), and the caller has no reasonable workaround; also a wrong or incomplete error with no workaround: the failure is reported, but its type, code, or message is wrong or lost. The impact **Stability** (a hang, a panic, such a leak) uses this row when the caller has no reasonable workaround |
| Workaround exists       | the feature fails, but the caller can avoid the failure with a small change in their own code (name the workaround in the reason); also impact **Performance**: a result that is correct but slow or uses too much memory |
| Cosmetic, docs, feature | wrong log or message text, documentation, a feature request |

An issue that has the impact Security with no vulnerability (an authentication
method that does not work or is missing) does not use the first row. Rate an
authentication method that does not work by its failure ("Broken, no
workaround" or "Workaround exists"), and a request for a missing method as a
feature request.

## 3. Base level

| Severity ↓ / Reach →    | Core   | Common | Narrow | Satellite |
| ----------------------- | ------ | ------ | ------ | --------- |
| Wrong data or security  | Urgent | Urgent | High   | Medium    |
| Broken, no workaround   | High   | High   | Medium | Low       |
| Workaround exists       | Medium | Medium | Low    | Low       |
| Cosmetic, docs, feature | Low    | Low    | Low    | Low       |

## 4. Modifiers

Apply them in this order:

1. **Security with a shown path from untrusted input.** The issue or a
   reproduction shows how a value that an application takes from its users
   reaches the defect: `Urgent` at any reach. Skip the other modifiers. A
   defect in the encoding of query parameters (`query_params.go`) that lets a
   parameter value change the meaning of the query is a candidate: look for
   such a path. If none is shown, rate the defect by the failure that it shows.
   A vulnerability that is not public yet goes to a private vulnerability
   report, not to a public issue (see "Vulnerabilities that are not public
   yet").
2. **Panic.** The library panics: a Go `panic` in ch-go code, not in the
   caller's code, and not in `cht`, `internal/`, `examples/`, or the tests.
   The level is at least `High`, at any reach. Library code must not panic on
   input from the network or from the caller (`AGENTS.md`), and a panic stops
   the whole process unless the caller recovers it.
3. **Ingestion.** The defect is on the insert path (`Client.Do` with
   `Query.Input` or `OnInput`, block encoding, the `Col*` encoders), and the
   client sends data that is lost, corrupted, or different from the given
   value, with no error. This includes a missing column value, and a column
   that gets a default value in place of the given value: up one level. This
   can give `Urgent`. Wrong data in a table stays wrong after the fix; a defect
   on the read path does not change the stored data.
4. **Regression in the latest published release:** up one level. This can give
   `Urgent`. It is a regression only when the latest release shows the problem
   and the release before it does not. The tags are `vX.Y.Z` (for example
   `v0.76.0`).
5. **Customer request in Linear.** The issue links (`linear.app`) a Linear
   issue for a customer request: up one level, but never to `Urgent` (`High`
   stays `High`).

A defect on the read path (`Query.Result`, `OnResult`, the `Col*` decoders)
that does not panic gets neither of rules 2 and 3. So at the same reach and
severity, a read defect ranks below a panic and below an ingestion defect. For
code that clickhouse-go calls, the clickhouse-go rules have the same two rules.

## 5. Confidence

A missing reproduction lowers the confidence, not the level. Record the
confidence with the value when the tool that sets it supports it:

| Confidence | Use when |
| ---------- | -------- |
| `HIGH`     | a run shows the problem (or, for a feature, code inspection confirms its absence on `main`) |
| `MEDIUM`   | code inspection shows a defect, but no reproduction was run (for example, it needs ClickHouse Cloud) |
| `LOW`      | the report lacks what a reproduction needs, or a run did not show the problem |

## 6. Reason

Write one line with the value:

`<tier> (<path>) · <impacts, or "no impact"> · <severity> · <modifiers, or "no modifier"> → <level>.`

Name the workaround when the severity is "Workaround exists". When the
confidence is `LOW`, say what is missing. When the tier comes from a
clickhouse-go path, name that path (for example
`Core (clickhouse-go Rows.Scan of DateTime64)`).

## 7. Fixes that clickhouse-go needs

A fix in code that clickhouse-go calls reaches clickhouse-go users only after a
ch-go release and a dependency update in clickhouse-go (`go.mod`). When the
tier comes from a clickhouse-go path, the fix PR says that clickhouse-go needs
that update. A change to an exported `proto` symbol can break clickhouse-go
(`AGENTS.md`). The priority stays as the table gives it.
