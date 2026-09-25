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

## RegionCache superseded-range removal emits no `regionInvalidated` metric; ordered writes use a treap

Two review decisions on the #238 fix (REG-07) that should not be "corrected" later:

1. **`removeOverlapping()` is deliberately silent metrics-wise.** Dropping a
   cached entry because the incoming region supersedes its range is a data
   update, not an error invalidation — the `invalidate()` / "single emission
   point" rule from issue #474 covers error-driven drops only. Emitting here
   would double-count eviction-style reasons.
2. **Ordered lookup and overlap removal use a start-key treap, while entries
   are stored by region ID.** A lower-bound/predecessor walk and ordered
   overlap removal are O((k + 1) log n), without packed-array splices or
   full identity-index shifts. LRU recency is an insertion-ordered hash and TTL
   cleanup uses a lazy expiry heap, so eviction and periodic sweeps do not
   scan the complete cache.

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

## All key ordering goes through `Client\Util\KeyOrder`, enforced by a PHPStan rule (issue #186)

TiKV orders keys by unsigned byte value, PHP 8 compares two numeric strings
numerically — so a bare `<`/`>` on keys silently means the wrong thing for
`"20"` vs `"100"`. Every key comparison in `src/Client` therefore goes through
`CrazyGoat\TiKV\Client\Util\KeyOrder` (`cmp`/`lt`/`lte`/`gt`/`gte`/`eq`/
`inRange`/`successor`), including the former `strcmp()` sites and the
`$key . "\x00"` successor trick, which exists so that idiom is documented in
one place. `CrazyGoat\TiKV\Phpstan\KeyOrderComparisonRule` (registered once in
`phpstan.neon`) reports `<`, `<=`, `>`, `>=` between two key-like strings, so
the decision is machine-enforced, not a review convention. The rule is
deliberately narrow: both operands must be typed `string` (an `int|string` call
index stays comparable) and both must read like a key — a key-like
variable/property *name* (`$startKey`, `$region->endKey`) or a string literal,
because `$key < 'z'` is how keys are compared in practice. It is **both**
operands, not either: with `||` a literal operand makes every `$message < 'z'`
a finding. Two residual blind spots are deliberate: a function-call result and
an array element carry no name to narrow on (`$key < self::limitKey()`,
`$key < $bounds[0]`). Widening further means flagging every call site, which
is why the rule subscribes to `Node\Expr\BinaryOp` (not `Node\Expr`, which
would run it on every expression in every analysed file) and filters by
`instanceof`.

Two operational notes. Do not register the rule under both `services:` and
`rules:` in `phpstan.neon` — that instantiates it twice and reports every
finding twice. And do not test it with a `RuleTestCase`: `PHPStan\Rules\Rule`
is autoloadable only from phpstan.phar, which needs ext-phar, and CI's
`unit-tests` job runs `php -n`. The rule is tested by `composer phpstan`
itself: `tests/Unit/Phpstan/KeyOrderRuleFixture.php` contains the shapes it
must report, and its `ignoreErrors` entry has `reportUnmatched: true`, so a
rule that stopped firing fails the run with `ignore.unmatched`. When changing
the rule, run `vendor/bin/phpstan clear-result-cache` first — the result cache
is not keyed on custom-rule source.

## Region-routing test boundaries must be decimal, not alphabetic (issue #232)

Every region-cache and range-clipper test that existed before #232 built its
region layout from `'a'`, `'m'`, `'key1'` — ASCII from the middle of the byte
range, precisely where PHP's numeric-string comparison and TiKV's byte order
agree, which is why the whole bug class of #186 passed CI unnoticed. The
numeric-boundary vectors of #232 are now pinned as regression tests in
`tests/Unit/Cache/RegionCacheNumericBoundaryTest.php` (the six-region layout
`["", "1000") … ["999", "")`, the `"1e3"` vs `"1000"` distinctness, and a
`strcmp` differential run over deterministic decimal keys) and in
`tests/Unit/Region/RegionRangeClipperTest.php` (three-region clip, sub-range
clip, and the `deleteRange("20", "300")` / `('3', '9')` shapes). When adding a
region-routing test, do not reuse alphabetic boundaries: pick boundaries whose
first byte disagrees with their numeric value, or the test cannot fail.
## Issue #261's inverted region-cache acceptance criterion is not implemented on purpose (issue #261)

Issue #261's acceptance criteria ask for `RegionCache::getByKey("99")` to
return `null` (a miss) for a cached region `["100", "")`. That expectation is
inverted, and `tests/Unit/Cache/RegionCacheNumericBoundaryTest.php` pins the
*correct* answer (the region) instead — both in the wrong-region case
(`testGetByKeyRoutes99ToTheRegionThatContainsIt()`, which is what the criteria
are really after: the pre-fix cache served `["", "100")`, a region that does
not contain `"99"`) and in the partial-cache case
(`testGetByKeyServes99FromTheOnlyCachedRegion()`). Bytewise `"99" > "100"`
(`'9'` = 0x39 > `'1'` = 0x31), so `"99"` is *inside* `["100", +inf)`: the cache
is a partial view of the keyspace, the byte-ordered predecessor walk must find
the one region it holds, and a `null` here would be a false negative that costs
a PD round trip on every lookup of a key the client already has. Do not
"fix" the test to match the issue text.

One attribution trap worth recording, because it is the kind of thing a
docblock gets wrong silently: the null-returning behaviour is **not**
"pre-#186". The numeric `RegionCache` predecessor walk was replaced by **#321
(PR #462, commit `aaadc4c`)**, which is an *ancestor* of #186 (`2ad8236`), so
the numeric `getByKey()` was already gone when #186 landed. Verified with
`git worktree add` probes against `aaadc4c^` (7017c28) and `2ad8236^`
(a4f898b): the former returns `null` for the partial-cache case and the wrong
region (1) for the two-region case, the latter answers both correctly. Write
the revision you actually measured, and `git log -S` it before claiming a
"pre-#N" behaviour.
