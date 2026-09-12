# Decisions — Project Conventions with Rationale

Important project decisions in crazy-goat/tikv-php that subagents should
not silently deviate from.

## Branch naming: `fix/<N>-<kebab-case>` (no `issue-` prefix)

Feature branches use `fix/<NUMBER>-<short-description>` (preferred), or
`feature/`, `docs/`, `refactor/` with the same shape. Unlike some projects,
there is **no** `issue-` prefix. Examples: `fix/96-n-plus-one-pd-region-lookups`,
`fix/104-pdclient-region-leader-fail-closed`.

## Commit style: `type: subject (#N)`

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `chore`. Optional
scope in parens `(rawkv)`, `(txnkv)`, `(retry)`, `(grpc)`, `(cache)`,
`(connection)`. Issue reference at the end: `(#98)` or `(closes #98)`.
Examples:
```
feat: batch pessimistic lock RPCs by region (#98)
fix: deletePrefix() now rejects prefixes consisting entirely of 0xFF bytes (#105)
```

## Issues are tracked in version milestones; work proceeds bottom-up

Every issue belongs to a version milestone (`v0.4.0` … `v0.14.0` currently
open; closed ones are released). The next issue to work on is always taken
from the **lowest-version milestone that still has open issues** — higher
versions are only started when every lower milestone is empty. Within a
milestone, `severity:*` labels (critical > high > medium > low) decide the
order, then bug over enhancement/documentation.

## Default branch is `master`, direct commits forbidden

All work happens on feature branches; merges go through squash-merged PRs.

## PHPStan level 9, `declare(strict_types=1)` everywhere

Static analysis runs at level 9 and every source file must declare strict
types. PSR-12 + Slevomat coding standard (enforced by PHPCS).

## No enforced coverage floor

Unlike some related projects, CI does not enforce a minimum coverage
percentage. Coverage is collected (PCOV, `grpc-unit-tests` job) but is
informational only.

## PHP support: >= 8.2, CI matrix 8.2–8.4

The library requires PHP >= 8.2; CI runs unit tests on 8.2, 8.3 and 8.4
(lint and gRPC tests on 8.4 only).

## RegionCache superseded-range removal emits no `regionInvalidated` metric, and an O(n) scan per `put()` is accepted

Two review decisions on the #238 fix (REG-07) that should not be "corrected" later:

1. **`removeOverlapping()` is deliberately silent metrics-wise.** Dropping a
   cached entry because the incoming region supersedes its range is a data
   update, not an error invalidation — the `invalidate()` / "single emission
   point" rule from issue #474 covers error-driven drops only. Emitting here
   would double-count eviction-style reasons.
2. **The overlap scan is a sorted left/right walk from the insert position,
   not a full O(n) loop over `entries`.** Because the cache maintains a
   sorted, non-overlapping invariant, every overlapping entry must be
   adjacent to `findInsertPosition($startKey)`: the walk goes right while the
   entry's `startKey < $endKey` and left while the entry's `endKey >
   $startKey`, stopping at the first merely-touching entry on each side —
   O(log n + k) per `put()` (first review accepted an O(n) scan; commit
   bbb4ec8 replaced it after the invariant was verified). Only revisit if
   profiling shows `put()` on a full cache is hot.

## Tests are delegated / E2E needs Docker

E2E suites (`E2E-RawKV`, `E2E-TxnKV`) spin up real TiKV clusters via docker
compose — they only run when relevant paths change (`src/`, `tests/E2E/`,
docker/composer files, CI workflow).

## `scan(limit: 0)`'s `maxScanRows` guard is per fetched page, not per row (issue #191)

Review of the #191 branch (`RawKvScanner::assertWithinScanLimit()`) confirmed
the guard is evaluated after each internally paged fetch, so an unbounded scan
whose `options['maxScanRows']` is smaller than the page size (`scanPageSize`,
default `MAX_SCAN_LIMIT` = 10240) still reads and buffers one full page before
throwing; `getScannedRows()` can therefore exceed `getMaxRows()` (the
client-level test pins `maxRows=1` / `scannedRows=2`). Peak memory is
`maxScanRows` rows of accumulated results plus up to
`maxConcurrency × scanPageSize` rows of one in-flight page — not a hard
`maxScanRows` bound. This is deliberate: shrinking the page to `maxScanRows`
would issue one RawScan RPC per row for small guards, so do not "tighten" the
guard to per-row without accepting that cost. A useful side effect to keep:
the loop can only continue while accumulated rows are below `maxScanRows`, so
the guard also bounds the page count — even a server that ignores the
continuation cursor cannot make `scan(limit: 0)` loop forever.
