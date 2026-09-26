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

One consequence of "key-like **name**" that closes the loop on the rule's own
coverage: the two places in the tree that exist precisely to *evaluate* the wrong
comparison — `BinaryKeyVectors::isDiscriminating()`/`phpVerdict()` and
`RawKvBatchKeyOrderTest::phpVerdict()` — name their parameters `$left`/`$right`
rather than `$key`/`$otherKey`, so the rule stays silent on the deliberate
`$left < $right` and the fixtures keep compiling. If either is ever renamed to a
key-like name, the rule fires on a comparison that is the *subject* of the test
rather than a bug.

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

## Retryability and cache invalidation are two decisions, in two functions (issue #233)

`RetryExecutor` used to express one decision — "is this error retryable?" — and
hang the region-cache drop off its *positive* branch, so every fatal error
skipped the drop. That coupling is what made a fatal `KeyNotInRegion` a
sustained 11-minute outage: the error that means "your cached routing
information is wrong" was the one error that kept its own cause cached (up to
`ttlSeconds + jitterSeconds` = 660 s), and no application-level action could
clear it. They are independent — client-go drops the region on a routing error
regardless of the retry verdict — so they are two functions now:
`ErrorClassifier::classify()` (reached through `handleNotLeader()` → custom
classifier → `ErrorClassifier`) keeps the retry decision exactly where it was,
and `RetryExecutor::invalidatesRoutingOnFatal()` states the invalidation
decision, evaluated above the fatal `throw`.

Two properties of that predicate are decisions, not omissions. It holds only
for a `RegionException` carrying a *routing* `ErrorKind`, so the non-routing
kinds (`RaftEntryTooLarge`, `FlashbackInProgress`, `FlashbackNotPrepared`, …)
and any non-`RegionException` fatal error leave the cache alone — those say the
request reached the right region and the region refused it, so dropping the
entry would buy a re-resolve and nothing else. And its `match` has **no**
`default` arm: PHPStan reports `match.unhandled` at level 9, so a new
`ErrorKind` cannot be added without deciding here.

Both gates were measured, not assumed (a scratch copy of the tree with
`case ScratchSentinel = 'scratch_sentinel';` added to `ErrorKind`, then
deleted). `match.unhandled` fires in exactly three places — `RetryExecutor.php`
(`invalidatesRoutingOnFatal()`), `ErrorClassifier.php` (`classifyByKind()`) and
`ErrorClassifierTest.php` (`errorClassForKind()`) — i.e. **every `match` over
`ErrorKind`**, in production and in tests alike. The routing table in
`RetryExecutorFatalInvalidationTest` is a hand-written array literal, so
PHPStan never sees it; its gate is the runtime assertion
`testRoutingTableCoversEveryErrorKindCase()`, which compares the table's keys
against `ErrorKind::cases()` and is the only test that fails *because of the
missing routing decision* (`ErrorClassifierTest`'s own providers are literals
too, so they stay green).

Note also that a new `ErrorKind` case is not merely a table row:
`RegionException::detectErrorKind()` calls `$error->has{PascalCase}()` for
every case, so a case with no matching `errorpb.Error` oneof turns *every*
`RegionException::fromRegionError()` call into
`Error: Call to undefined method …`. That is not silent, but the blast radius
is wrong for the mistake — the same sentinel measured 8 errors + 3 failures
across 11 tests in `LockResolverTest`, `RawKvScannerTest`, `TxnReaderTest` and
`TransactionTest`, and not one of those names the enum. A one-line test
asserting `method_exists(Error::class, 'has' . PascalCase($kind->value))` for
every case would turn that scatter into a legible failure.

The retryable path is deliberately untouched: it still invalidates for every
retryable error, and #245 (REG-14 — a separate open milestone issue with its
own acceptance criteria) is what will route it through this same predicate. Do
not "finish" that narrowing here; the predicate is public and static precisely
so that PR has a seam to reuse instead of a third copy of the list.

That seam is a compatibility commitment, not a convenience: `RetryExecutor` is
a documented user-constructible collaborator (`docs/configuration.md`), so
`invalidatesRoutingOnFatal()`'s name and signature are semver-visible. #245 may
still prefer to move the decision into `ErrorClassifier` — it needs the
*kind*, not the exception — and such a relocation stays compatible with the
commitment, because what callers are promised is the routing/non-routing table,
not the class it lives in. Do not add a third copy of the list under either
name.

## A class of bug gets a seam, a rule and a fixture — not three site patches (issue #180)

Closing #180 after #186, #232 and #261: the answer to "how do we know this class
is done" is one `KeyOrder` seam (so a new comparison site has something to use),
one PHPStan rule (so a bare `<` on keys cannot come back), and **one** shared
vector fixture with a cross-implementation differential (so a component that
silently disagrees with the others fails). `tests/Unit/Support/BinaryKeyVectors.php`
owns the layouts, the discriminating pairs and the probe keys;
`tests/Unit/Util/KeyOrderDifferentialTest.php` runs them through `KeyOrder`,
`RegionCache`, `RegionRangeClipper` and `ScanIterator`.

Say precisely how independent that reference is, because the strength of the whole
argument rests on it and it is *not* uniform. It shares no code with `KeyOrder`,
with `RegionCache::getByKey()`, with `keyInRegion` or with `ScanIterator` — those
comparisons are plain `strcmp()` over vectors no production class sees. For
`RegionRangeClipper` the picture is different: `referenceForwardClip()` re-derives
`clipForward()`'s `max`/`min` and its `''`-as-+infinity algebra condition for
condition, so a disagreement there proves a transcription error and nothing more.
(The `clipReverse()` intersection check was removed for exactly this reason; the
reverse direction is covered by the property below instead.) **That is why the
tiling property is the load-bearing check for the clipper**:
`testClippedSubRangesTileTheRequestWithoutGapOrOverlap()` cuts each request at the
bounds of the regions it touches into maximal byte intervals and requires one
sub-range per interval, each owned by the region the `strcmp` reference assigns —
no gap, no overlap, no id in the wrong place. A statement about the sub-ranges
rather than a second run of the formula, and one that still holds if the same bug
is injected into both the clipper and the fixture's interval derivation.

The rest of the reason is arithmetic, not taste: five of the nine auditors of the
2026-08-08 review reported this same root cause independently (RAW-01, REG-01,
GRPC-02, TXN-15, the test audit) and it still took three separate issues and
three PRs to land, because each finding shipped its own vectors. Two rules for new
work in this class: add a boundary to the shared fixture and let the differential
report what moved — never a new site-local layout; and let the fixture *derive* its
expectations (a key pair is admitted to the discriminating table only when asking
PHP what `$a < $b` says proves it disagrees with byte order, so the table cannot rot
into pairs a pre-#186 comparison passes).
## A Raw-mode region boundary is stored bytes, not the key you split at (issue #188)

PD stores the default keyspace's region boundaries memory-comparable encoded:
splitting the keyspace at `rawkv-188-txnsplit` (TiKV's `SplitRegion` RPC with
that raw user key) records the boundary
`7261776b762d3138ff382d74786e73706cff6974000000000000f9`, verified on a live
cluster. A `Mode::Raw` bundle never sees that user key as a boundary:
`CodecV1::encodeRegionKey()` returns the key unchanged in `Mode::Raw`
(`src/Client/Codec/CodecV1.php:37,53-60`), and `RegionInfoMapper::fromProto()`
decodes PD's stored boundaries back through the same passthrough codec, so the
client asks PD *and* reads the answer in the unencoded key space — the space
`RegionResolver::batchResolveRegions()` also computes its
`[minKey, successor(maxKey))` window in (`src/Client/Region/RegionResolver.php:139`).
One run, both bundles, same cluster:

```text
--- Mode::Raw boundaries as this client sees them
  region 32    start=7261776b762d3138ff382d74786e73706cff6974000000000000f9  ← MCE bytes
--- Mode::Txn boundaries as this client sees them
  region 32    start=7261776b762d3138382d74786e73706c6974                   ← the user key
```

`Mode::Txn` is why `TxnKvE2ETest::splitTxnKeyspaceIntoRegions()` works and its
sibling `testTxnWorkloadAcrossPreSplitRegions()` is the automated split-point
vector: encode on the way out, decode on the way in, so `RegionInfo::startKey`
is the user key again and the region that starts at `"\x01\x00"` / `"mrr"` is
visible and assertable. A Raw-mode test that waits for a boundary it created
with `SplitRegion` waits forever — that is the whole reason #188's E2E coverage
used to self-skip (two runs, 1 m 15 s each, zero real coverage) and had to be
replaced.

What the replacement does instead of creating a boundary:
`RawKvE2ETest::smallestRegionStartKey()` reads the layout through a `Mode::Raw`
bundle — the same codec and `PdClient` the resolver uses, so its answer *is* a
boundary in the key space the window is computed in — and
`testBatchRoundTripResolvesEveryKeyWhenTheLargestOneIsARegionStartKey()` uses
the smallest non-empty start key as the batch maximum, its proper prefixes as
the other keys, and asserts every key resolves (whole batch, one-key batch, and
`get()`). A single-region cluster offers no non-empty start key (`''` is the
keyspace minimum and can never be a batch maximum), so the batch degrades to a
plain `uniqid()`-keyed set and the test still runs and still pins the no-drop
guarantee and the degenerate `[k, k . "\x00")` window. Verified as a real
regression test: reverting the window to the pre-#244 `[minKey, maxKey)` in a
scratch copy of `src/` makes it fail with
`TiKvException: PD could not resolve the region for key …; refusing to silently
drop it from the batch`.

The issue's literal case (choose a boundary with `SplitRegion`, write to it) is
a documented manual procedure — "Region boundaries: the #188 split point by
hand" in `docs/development.md` — because it is not a test-suite action: it
leaves a permanent region boundary behind, which would make the shared suite's
layout depend on run order. `pd-ctl operator add split-region --keys <hex>`
does store the bytes verbatim and *is* visible to a Raw client, but that
boundary is not MCE-shaped and costs more than it buys: every `Mode::Txn`
bundle then throws `InvalidArgumentException: Invalid MCE marker 0x…` out of
`MemComparableCodec::decode()` (`src/Client/Codec/MemComparableCodec.php:88`),
and PD and `RegionResolver` disagree about which region owns the keys between
that boundary and its MCE-encoded neighbour, so RawKV reads and writes there
fail after the retry budget is spent. Merge such a boundary away again.

Production impact of the key-space asymmetry itself: none against a
TiKV-created cluster. All 204 RawKV E2E tests pass, and the shapes where the two
key spaces *can* differ — the stored boundary bytes themselves and their
prefixes, which sit in the padding gap `[K, MemComparableCodec::encode($K))` —
round-trip: for every non-empty boundary of an eight-region cluster the
boundary bytes and their one- and two-byte prefixes were written and read back
(21 keys, no region error, no retry). The client misroutes only when a stored
boundary is not MCE-shaped, which no TiKV path produces.

## A key whose region cannot be resolved is an internal error, never a valid outcome (issue #187)

Closing #187 after #244: **every** region-grouping loop in the client fails
closed. A miss is a `TiKvException` naming the key through
`KeyRedactor::redact()` — never a `continue`, never a `null` in a result map.
The reason is that no entry point can report a partial result:
`batchPut()`, `batchDelete()` and `ingest()` return `void`, so a skipped key
was a write reported as done and never sent, and `batchGet()`'s `null` was
indistinguishable from a legitimately missing key. client-go is the reference:
`RegionCache.GroupKeysByRegion` propagates the resolution error rather than
dropping keys.

Three decisions inside that, each of which was available and was rejected:

1. **One accessor, not six copies of the message.** `RegionGrouper::resolvedRegion()`
   is the single reader of a `batchResolveRegions()` map, used by
   `batchPut()`/`batchGet()`/`batchDelete()`, the three multi-region re-split
   paths, `SstIngestor`, `TxnReader` and both `RegionGrouper` groupers — so the
   wording cannot drift between sites, and the guard is *testable* (see 2).
2. **Keep the guard even though it is unreachable.** `RegionResolver` is
   `final` and fails closed, so the loop-level miss is not producible from the
   public API — which is exactly why deleting the guard would be safe *today*
   and unsafe the first time the resolver changes. It is kept as a tested seam
   (`ResolvedRegionSeamTest` drives it with a hand-built partial map) and
   `NoSilentRegionDropGuardTest` fails on the `if (<null check>) { continue; }`
   shape returning to `src/Client` at all, allowlisted only for the two files
   that legitimately discard an internal cache node or a malformed configured
   PD endpoint. The rule is deliberately broader than `$region === null`:
   a name-keyed rule is defeated by `$r`, which is what three of the five
   sites used.
3. **No per-key `getRegionInfo()` fallback in `batchResolveRegions()`.** The
   issue suggested it "so partial results become the exception rather than the
   norm" — but the throw *is* the exception now, and a partial map is no
   longer an outcome at all, so the criterion is met without it. Against it:
   `ScanRegions` and `GetRegion` are the same PD, so a window that missed a key
   misses it the second time too — the fallback would buy N extra round trips
   and the same error; it would *mask* the half-open-window bug of #244 (the
   E2E vector for which, `testBatchRoundTripResolvesEveryKeyWhenTheLargestOneIsARegionStartKey`,
   currently bites by reverting to `[minKey, maxKey)` and would then pass via
   the fallback); and #288 (open, v0.8.0) restructures this method to read the
   region cache first, so a second resolution path added now is precisely the
   code #288 would rewrite, with its own cache-accounting decision to make.
   A real case where `ScanRegions` misses a key `GetRegion` finds belongs in
   #288, not here.

4. **The guard keys on an absence check, not on `=== null`.** `NoSilentRegionDropGuardTest`
   was originally written to match *an identity comparison against `null`* —
   the shape the issue quoted. That was one spelling too few: rewriting a
   guarded `continue` as `if (!isset($resolved[$key])) { continue; }`, or as
   `if (!array_key_exists($key, $resolved)) { continue; }`, is the same silent
   drop in the form PHP makes easiest to write, and **both passed the whole
   1648-test `Unit` suite** at the two `RegionGrouper` loops a
   `set(K); commit()` pair reaches. The rule now recognises any absence test
   (`=== null` either way round, `isset()`, `empty()`, `array_key_exists()`)
   and a data-provider test pins the recognised spellings, so the broadening
   cannot be narrowed back silently. It over-matches on purpose: a *positive*
   `isset($resolved[$key])` keeps the entry and is flagged anyway, because the
   cost is one reviewed allowlist row (`ConnectionFactory::resolveGrpcChannelArgs()`,
   which skips a configured gRPC option the caller did not supply) and the
   alternative is a guard whose evasion is a two-character edit. The lesson is
   the one the rule was written for — the original was already deliberately
   broader than `$region === null`, because a name-keyed rule is defeated by
   `$r` — applied one level further: **a guard must key on the *decision*, not
   on any single spelling of it.** Note what a behavioural test cannot do
   here: no loop-level `continue` is reachable (`batchResolveRegions()` throws
   first), so reverting one changes no observable behaviour and only this
   static rule can catch it.

The issue's own fourth finding, the transaction (`set(K); commit()` as a total
no-op reported as `Committed`), is the same defect at a different layer and was
**not** fixed by #187 alone, so it is worth stating where it is caught:
resolution happens at *commit*, not at `set()` — `TransactionState`'s write set
is filled with no region lookup at all — and
`TwoPhaseCommitter::groupMutationsByRegion()` → `RegionGrouper::groupItemsByRegion()`
→ `RegionResolver::batchResolveRegions()` throws naming the redacted key before
a single RPC is constructed. Below that sits #208's mutation-count guard
(`InvalidStateException: Not all transaction mutations were assigned to a
region`), which is defence in depth: it also catches a dropped key, but it
reports a *count mismatch* and does not name the key, so the resolver's message
is the one a caller can act on. A pessimistic transaction fails even earlier,
at the eager lock inside `set()` (#437). Both paths are pinned in
`UnresolvedRegionFailsClosedTest` — assert the throw, assert the status is not
`Committed`, assert nothing reached the wire. `CommitPrewriteKeyCoverageTest`
(#329) covers the *success*-path invariant (a write-set key must be prewritten)
and is deliberately not duplicated here.

## `batchResolveRegions()` reads the cache first, and a "run" is a provable gap (issue #288)

Closing #288 after #187/#244/#188, three decisions inside
`RegionResolver::batchResolveRegions()`:

1. **A run of misses is split where a cached key proves a boundary.** The
   cache answers nothing about a key it does not hold, so the grouping cannot
   ask "is there a cached region between these two misses?" — it asks the
   only question the key set can answer: *does a cache-served key of this
   batch sort between them?* If one does, its cached region contains that
   key, so it starts at or before it; the earlier miss cannot be inside it and
   the later miss (sorting after the key) is at or after its end. The region
   therefore lies strictly between the two misses, i.e. they are in two
   different cache gaps. That is a proof, and the rule is **one-sided on
   purpose**: a split only ever happens across a proven boundary, so it never
   splits a run that shares a gap, and every split saves a round trip. The
   other direction is deliberately unsplit — two gaps the batch happens to have
   no key between are merged into one wider window, which costs a page, not
   correctness, because every run is scanned as the `[firstKey,
   successor(lastKey))` envelope and every returned region is assigned by
   binary search. A cold batch is therefore still **one** scan for the whole
   key set. `getRegionsInRange()` cannot help here and is deliberately
   unused: it starts at `getByKey($startKey)`, which is `null` for every key
   of a run by definition, so it can only ever describe a cached chain — and
   a run lies entirely in cache *gaps*.
2. **The limit is 128 regions per page, with a 1024-page ceiling.**
   `ScanRegions` paginates on it, so it trades round trips against response
   size: a gap is a handful of regions and fits in one page at any value,
   while the motivating case (a cold cache, or a scattered batch, on a
   10 000-region cluster) answered with every region in the keyspace at
   `limit = 0`. 128 keeps a page in the tens of kilobytes and re-resolves such
   a cluster in 79 round trips. Every loop exit is unconditional — empty
   page, range reached (or an unbounded last region), non-advancing cursor,
   short page, and the ceiling — because a "does it advance?" test alone does
   not stop a PD that advances one byte per page. A run whose keys are not all
   covered fails closed on the unresolvable key, exactly as before.
3. **The miss path still needs the `[minKey, successor(maxKey))` window, and
   the read-through must not make it optional.** A cache read only removes
   the *need* to scan; where a scan does happen the half-open
   `ScanRegions` rule is unchanged, so the upper bound is still the
   immediate byte successor of the run's maximum key or the region that
   begins exactly there is excluded and its key is dropped. The trap is
   specific to the E2E vector: `testBatchRoundTripResolvesEveryKeyWhenTheLargestOneIsARegionStartKey`
   wrote its batch through `batchPut()`, which now warms the region cache for
   every key of that batch, so its `batchGet()` is a pure cache hit and a
   reverted `[minKey, maxKey)` window would have passed. It reads back
   through freshly created, cold clients for exactly that reason — one per
   read, because whichever ran first would warm the region the other one
   needs to scan for, and a warm client computes no window at all. The unit
   side asserts the window with a *partially* warm cache
   (`testMissPathKeepsTheSuccessorWindowEvenWithAPartiallyWarmCache()`),
   which is the only shape in which a warm client would expose a window
   regression. The skip of a re-insert keeps the identity at epoch + range +
   **leader**: an epoch-only test would keep serving a deposed leader out of
   the cache, because a TiKV leader transfer does not bump the epoch, and the
   ID needs no comparison at all because it *is* the lookup key.

   The identity is asked through `RegionCacheInterface::getById()` — new in
   this PR, and a **breaking** addition third-party caches must implement —
   because the O(1) lookup is the only reason the criterion pays. The first
   version of this fix asked `getByKey($region->startKey)`, which is the one
   shape of the question the interface could already answer, and *that* was a
   measured regression: on the post-#289 cache a key lookup runs ~10.4 µs
   against 10 000 entries (a treap descent plus a `KeyRedactor::redact()`
   string built for the debug line) while the `put()` it avoided — the
   same-id/same-range fast path — runs ~6.5 µs, so the "optimisation" cost
   1.6× the work it removed. `getById()` is one array lookup on the map the
   cache already keeps. In isolation that lookup measures 0.16 µs (half of
   it the `time()` the TTL check needs); the shipped method lands at ~2.5 µs
   once the leader-aware copy, the expiry check and the debug line are in, and
   still beats the `put()` it avoids by ~2.5×. The benchmark prints all three
   shapes side by side — unconditional `put()`, `getByKey()` probe,
   `getById()` probe — so the claim is checkable rather than asserted
   (`benchmarks/BatchResolveRegionsBenchmark.php`).

## A PD `ResponseHeader.error` is an error even with an empty message, checked at one choke point (issue #234)

PD reports application-level failures (`NOT_BOOTSTRAPPED`, `ErrNotLeader`,
`INVALID_VALUE`, …) inside a gRPC response with `OK` status and an **empty
payload**, so before #234 every method that read such a response as a success
degraded instead of failing: `getRegion()` threw "returned no region",
`getStore()` returned `null` (a `StoreNotFoundException` for a store that
exists) and `scanRegions()` returned `[]` — which made
`RawKvRangeOps::deleteRange()` return normally having deleted nothing. Three
decisions, each of which was available and was rejected:

1. **The check lives in one choke point, not in each method.**
   `checkHeader()` runs inside `callCurrentAddressWithClusterIdRetry()` (on
   both of its responses, so a cluster-id retry is checked too) and in
   `callGetMembers()` — the only two places the client obtains a PD response.
   A per-method call is the issue's fallback suggestion and is what the two
   GC-safe-point methods had; a method added later would then have to remember
   to call it, and the failure mode of forgetting is a *silent wrong result*,
   not a visible one. The per-method checks were removed, and the four
   existing message assertions in `PdClientGcSafePointTest` still pass because
   `PdException` renders as `PD <method> failed: <text>`.
2. **The presence of the `pdpb.Error` message is the signal, not its `type`.**
   `pdpb.ErrorType` has no case for "the error is present but the text is
   empty", and a `setType(0)` (`OK`) is indistinguishable from unset, so
   requiring `type !== OK` would re-open the typed-but-silent hole. The
   `type` is carried on `PdException::$errorType` (with `getErrorTypeName()`,
   which renders an unknown value as its number rather than throwing) for
   classification; an empty text renders as
   `PdException::EMPTY_MESSAGE`, so the message still names a reason.
3. **A not-leader header error rotates the endpoint instead of failing the
   call — and it reuses the transport failover machinery rather than getting
   its own.** `callWithClusterIdRetry()` catches `PdException` beside its
   existing `GrpcException` and routes a `isNotLeader()` error through the
   same `discoverLeaderAddress()` re-discovery and the same "at most once per
   configured endpoint" budget; a second rotation implementation would be a
   third copy of that rule (the lesson of the #233/#245 predicate in this
   file). `isNotLeader()` matches PD's *text* ("not leader", "not the leader",
   "no leader", "leader has changed") because `pdpb.ErrorType` has no
   not-leader member — `UNKNOWN` is PD's catch-all, so a type test would miss
   the exact case rotation exists for. Every other header error
   (`NOT_BOOTSTRAPPED`, `INVALID_VALUE`, …) is fatal here: another endpoint
   cannot satisfy it, and retrying would only multiply the RPC load on a
   cluster that is not coming up.

Two soft-fail paths had to be rewired rather than assumed to survive the move
of the check, and both are worth remembering as a shape:
`updateServiceGCSafePoint()` used to inspect the response itself and now
catches `GrpcException | PdException` instead (a cluster without service GC
safe points is a supported configuration, so the `isUnsupportedFeatureError`
soft-fail must catch the header form too — a `catch (GrpcException)` would have
turned a supported configuration into a hard failure); and `ping()`, whose
`GetMembers` response was previously read for the cluster ID only, now records
the leader URL that RPC exists to report — validated against the configured
endpoints, because adopting an address PD advertised but we were never given is
the rogue-PD redirect #306/SEC-03 exists to prevent.

`TimestampOracle::callTso()` is deliberately left alone: it does not go through
`callWithClusterIdRetry()`, and it does not need to — `extractTimestampRange()`
already fails closed on a timestamp-less response (`TSO response missing
timestamp`), so a TSO header error surfaces as an exception rather than as a
fabricated timestamp. Adding a second copy of the header reader there would
buy nothing and duplicate the rule.

`ErrorClassifier` is left alone too, which is a decision rather than an
omission. A `PdException` can reach the retry executor (every region-resolved
operation resolves through PD inside a retry closure), and it falls through to
the message-text fallback. The default verdict is `null` — fatal — which is
exactly the verdict the issue requires for `NOT_BOOTSTRAPPED` ("must not be
retried forever"), and `PdClient` has already spent the one rotation that can
help before the exception escapes. The only text matches that can reclassify it
are PD prose naming `RegionNotFound` / `NotLeader`, and retrying *those* is
defensible (a PD-side region miss is a routing miss). Adding an explicit
`PdException` arm would move that decision into a list #245 is about to
narrow, so it is recorded here instead: if #245 narrows the retryable path,
this is the place to decide whether a PD-level region miss stays retryable.

## Every gRPC call carries a finite deadline, and PD/TSO/lock-resolution time out separately from the store RPCs (issue #260)

Two decisions from #260 that should not be "simplified" away:

1. **`null` means "library default", not "no timeout".** `GrpcClient` used to
   resolve a `null` timeout to `Timeval::infFuture()`, which is the one
   deadline the gRPC C core never expires — and `Grpc\Call::startBatch()` is
   a blocking call *inside* the C extension, so neither `max_execution_time`
   nor `set_time_limit()` can rescue a worker pinned on a half-open
   connection. `null` now resolves to `GrpcClient::DEFAULT_TIMEOUT_MS` (30 s),
   and the only way to ask for an unbounded call is the explicit non-positive
   sentinel `0` — deliberately the same spelling `TimeoutConfig`'s
   `batchDeadlineMs = 0` already uses for "disabled", so there is one
   convention rather than two. The 30 s backstop is defence in depth, not the
   fix: the real deadlines are named per call site, so a 30 s default never
   has to be the one that fires.
2. **PD, TSO and lock resolution have their own `TimeoutConfig` fields**
   (`pdTimeoutMs` / `tsoTimeoutMs` / `lockResolveTimeoutMs`, all finite
   defaults) rather than borrowing a store field. They are separate because
   the failure modes are: a hung metadata plane takes every region lookup and
   every transaction begin down **simultaneously** — one blackholed PD, the
   whole process pool — whereas a slow store is a per-region problem the
   `RetryExecutor` already handles against a per-region budget. A shared
   field would force one value that is simultaneously too tight for a large
   scan and too loose for a TSO fetch. The values are threaded at the call
   site, not in `callWithClusterIdRetry()`'s signature: one helper, ten call
   sites, and the three classes disagree about which field is right, so the
   argument is the decision and it belongs where the call is made.

   The consequence to remember: `checkTxnStatus()` asks PD for a timestamp on
   its way to a store, so it consumes **two** deadlines — `tsoTimeoutMs` for
   the `Tso` fetch and `lockResolveTimeoutMs` for `KvCheckTxnStatus` itself.
   It previously passed the store `writeTimeoutMs` to `getLowResolutionTimestamp()`,
   which made a TSO deadline a function of the write timeout.

Enforcement is split by what each assertion can reach. `RpcDeadlineTest` drives
the real entry points through a recording `GrpcClientInterface` and asserts the
argument each one received — that is the only thing the pre-fix code got wrong.
Its companion assertion in the same class tokenises the three source files and
fails if *any* `->call()` / `->callAsync()` / `->callStreaming()` site lacks a
sixth argument, because a per-entry-point test can only cover the paths it
drives: it would not have caught a site added tomorrow. The `Timeval` itself
needs ext-grpc, so `GrpcClientTimevalDeadlineTest` lives in the `Grpc` suite
and asserts the object; the extension-free `GrpcClientDeadlineTest` pins the
`null → 30 s` resolution through the private `resolveTimeoutMs()` seam, the
same reflection-over-a-private-seam pattern as `channelArgs()` (see the FAQ).

## No code path in `src/Client` may derive an infinite gRPC deadline (issue #184)

Closing the root-cause issue #184, whose three mechanisms (#611/#260 for the
`null` timeout, #294 for `deadlineMs = 0`, #271 for the per-invocation retry
budget) were already fixed by merged work. The decision worth recording is the
rule that class is now held to:

1. **A gRPC deadline is derived from a strictly positive number of
   milliseconds, or not at all.** `Timeval::infFuture()` is the one deadline
   the gRPC C core never expires, and `Grpc\Call::startBatch()` blocks *inside*
   the extension, where `max_execution_time` and `set_time_limit()` cannot
   reach — so an unbounded deadline converts one slow store into an exhausted
   PHP-FPM pool. The class of bug is not a bad value but a **`null` that
   silently means "forever"**, and nothing in PHP's type system objects to a
   `?:` over a `?int` that produces it. So the nullable deadline helper is
   gone: `RawKvBatch::timeoutMs()` returns `int` with a `default => throw`, and
   a non-positive configured value resolves to
   `GrpcClient::DEFAULT_TIMEOUT_MS` — because `TimeoutConfig`'s `0` is a
   *transport-level* opt-out, and a hand-rolled `new Call(...)` is not the
   transport.
2. **Two sites may legitimately produce one, and they are enumerated, counted
   and justified**: `GrpcClient::deadline()`'s explicit `0` sentinel (the
   library's single "disabled" spelling, from #260) and
   `BatchCommandsConnection::open()`'s `$deadlineMs` stream-lifetime parameter
   (a bidirectional stream, not a unary call; production never passes `0`).
3. **`NoInfiniteDeadlineGuardTest` keeps it that way.** A guard is the right
   instrument precisely because the bug is an *absent* value: a behavioural
   test can only assert the paths it drives, and a value assertion says nothing
   about a site added tomorrow. It states the rule over
   `Timeval::infFuture()` **occurrences** rather than over `?:` and ternaries —
   every shape that can produce an unbounded deadline must mention it somewhere,
   so `match`, `??`, `if` and a hardcoded one are all caught, and a count-based
   allowlist (the `NoSilentRegionDropGuardTest` precedent) forces a conscious
   review whenever a site is added or removed. It carries an
   `assertSame(2, …)` witness so a scan that silently stops finding anything
   fails too.

The same reasoning is why the *retry* half is a test and not a refactor: a
shared `RetryExecutor` is the documented collaborator `Transaction` memoizes
for its whole lifetime, so "every invocation starts with a full budget" is a
contract with three independent bounds (backoff budget, server-busy budget,
wall-clock deadline) that no single behavioural test covers.
`RetryExecutorFreshBudgetTest` names the two already covered by #243 and adds
the wall-clock deadline, the #233 fatal-path interaction, and the attempt cap.
Do not collapse the three bounds into one value: the 30 s wall clock is the
binding one for a `ServerBusy` storm (1–2 s sleeps), the sleep budgets are what
bound a millisecond-scale retry storm that no wall clock would catch.

