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

## The coverage floor is enforced, and it is a ratchet (issue #362)

CI enforces a **minimum line coverage** for the `Unit` + `Grpc` suites. The threshold lives in
exactly one place, `DEFAULT_COVERAGE_FLOOR` in `bin/check-coverage.php`, and it is a **ratchet**:
it is set a little below the measured coverage and only ever moves up. `composer test:coverage`
runs the same suites under a coverage driver and applies the same threshold, so a red build is
reproducible locally; the CI job additionally uploads the Clover report as the `coverage-clover`
artifact.

Three decisions are worth keeping:

1. **The metric is the line coverage of the hand-written code, not of the whole tree.**
   `phpunit.xml` excludes the generated `src/Proto` classes from the `<source>` set. They are
   ~62 % of the executable lines of `src/`, they are not written by hand and nobody writes tests
   for them, so counting them would make the floor measure the generated code instead of the
   client (the same tree measures 42 % with them and 87 % without). The issue's own baselines
   (58.4 % for `Unit`, 70.2 % for `Unit` + `Grpc`) are only reproducible on this metric — measured
   on the audit-time tree (2026-08-08) the whole-tree numbers were 14.6 % and 18.3 %.
2. **The floor is checked in the existing `grpc-unit-tests` job, not in a new one.** That job
   already installs PCOV and `ext-grpc`, so it can run the combined suites in one process and
   produce one report covering the whole client; the `unit-tests` matrix keeps running the `Unit`
   suite on every PHP version without coverage, so the matrix is not slowed down.
3. **The threshold is below the measurement, never above it.** A floor above the current coverage
   fails the build immediately; the shipped floor is 87.0 %, set against a measured 87.6 % (PCOV,
   PHP 8.4 — the measurement moves with the tree, the floor only moves up). PCOV and Xdebug measure
   the same metric within a few tenths of a point (0.3 points apart on the tree the floor was set
   on), so the same floor holds whichever driver a contributor has installed.

The floor is deliberately *not* a quality target for new code: it is a regression guard. A change
that drops the coverage fails CI until either the missing tests are added or the floor is
explicitly lowered — and lowering it is a visible act, because the value lives in
`bin/check-coverage.php` and `tests/Unit/Bin/CoverageFloorCheckTest.php` reads it back from there
(a report a tenth of a point below it has to fail).

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

## A test runner that selects nothing must exit non-zero, and the E2E entry point runs the suites one after the other (issue #361)

Three decisions that belong together, because either one alone leaves the
other half of the bug open:

1. **Every PHPUnit invocation in the repository passes
   `--fail-on-empty-test-suite`.** PHPUnit answers a suite selector that
   matches nothing with "No tests executed!" and exit code **0**, so a
   renamed suite is indistinguishable from a passing run. The flag turns
   that into a non-zero exit code. It is deliberately *not*
   `--fail-on-skipped` for the suites as a whole: the E2E suites skip
   legitimately (no Docker socket for the PD-failover tests, no `proc_open`
   for the concurrency test), so that flag would make an ordinary developer
   machine red.
2. **`composer test:e2e` runs the two end-to-end suites one after the
   other, each against the cluster mode it needs** — it delegates to
   `scripts/test-e2e.sh` (`make test-e2e`), which starts the V1TTL cluster
   for `E2E-RawKV`, tears it down, starts the V1 cluster
   (`docker-compose.txnkv.yml`) for `E2E-TxnKV` and runs each suite in its
   own PHPUnit process. A single PHPUnit run cannot do that: the suites
   need cluster modes of their own (V1TTL for RawKV, V1 for TxnKV, API V2
   for ApiV2), so one cluster cannot serve two of them and a run that
   selects both would be red by construction. The per-mode scripts
   `composer test:e2e:raw`, `composer test:e2e:txn` and
   `composer test:e2e:apiv2` run one suite against a cluster that is
   already up; they share `scripts/test-e2e-suite.sh`, which owns the
   environment each suite needs.
3. **A missing cluster fails fast instead of failing late.** Without a
   cluster every end-to-end test used to spend the client's whole retry
   budget (measured: 18.8 s) before it reported the connection error, so
   the 242 end-to-end tests needed about an hour to print their summary.
   `tests/E2E/ClusterProbe.php` asks PD for its member list before the
   first test of a class and fails the class when PD does not answer — a
   failure, not a skip, because a skipped suite looks exactly like a suite
   that tested nothing.

The fourth decision closes the remaining hole of the first one: **a skipped
test counts as a test, so `--fail-on-empty-test-suite` does not fire on
it.** The API V2 suite skips every one of its tests unless
`TIKV_API_VERSION=2` is set, so a runner that leaves the variable out
turns the lane into a green no-op (measured: "Tests: 4, Skipped: 4", exit
**0**). The runners that own the lane therefore set the environment
themselves — `scripts/test-e2e-suite.sh` exports `TIKV_API_VERSION=2`,
requires `TIKV_ALT_KEYSPACE` (the second keyspace the suite compares
against) and adds `--fail-on-skipped`, which is safe there because the
suite has no legitimate skip once the environment is set;
`scripts/test-e2e-apiv2.sh` does the same for the Docker lane. The
alternative — dropping the per-mode command and pointing at the lane's
runner — would have left the host-side entry point unexplained.

The rules are machine-enforced, not a convention:
`tests/Unit/Ci/TestSuiteSelectorGuardTest.php` fails when a `--testsuite`
selector used in `composer.json`, in `scripts/*.sh`, in the `Makefile`, in
the `docker-compose*.yml` files or in the CI workflow names a suite
`phpunit.xml` does not declare, when a PHPUnit invocation runs without
`--fail-on-empty-test-suite`, when no runner selects an end-to-end suite,
when a single invocation selects two end-to-end suites that need different
cluster modes, or when a runner selects the API V2 suite without running it
with `TIKV_API_VERSION=2`.

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

## A scan that hits a cap throws; it never returns a partial range (issue #440)

Issue #440 (DIV-04) closed the remaining silent truncation in the scan
surface. The decision worth recording is the **shape of the contract**, not
the diff:

1. **A scan never truncates silently, and the return type does not change.**
   The alternative shapes the issue listed were a result object with a
   `truncated` flag and a per-call "did it truncate" method. Both change the
   public API of four methods, and the flag would have to be threaded through
   `RawKvScanner`, `RawKvClient` and `Transaction` for a fact the caller can
   already derive. The existing `ScanLimitExceededException` was reused
   instead: the return type stays `array<array{key: string, value: ?string}>`,
   and a truncated result is signalled by an exception the caller can catch.
   `ScanLimitExceededException::forTransactionScan()` exists only because the
   RawKV wording names `options['maxScanRows']`, which does not apply to the
   transactional cap; the type, the accessors (`getMaxRows()`,
   `getScannedRows()`) and the "never retry" verdict are shared.
2. **A bounded scan truncates by contract, an unbounded one may not.** With
   an explicit `limit > 0` the caller asked for at most that many rows, so
   the result is capped and the caller detects a possibly-truncated result by
   comparing the count with the limit — that is documented, not thrown. With
   `limit: 0` the call means "the whole range", so a cap that stops it early
   is a failure, not a result.
3. **The two scan surfaces keep their own cap, because they are different
   operations.** The RawKV path pages the range internally (issue #191) and is
   bounded by `options['maxScanRows']` (default 100000), a *memory* guard. The
   transactional path reads at the transaction's snapshot and does not page,
   so it is bounded by `Transaction::MAX_SCAN_LIMIT` (10240) — a *contract*
   cap. Unifying them (teaching the transactional scan to page) would change
   a snapshot read into a multi-RPC one, and the transaction's write-set merge
   and lock handling with it; that is a bigger change than this issue
   authorises, so the two caps stay and both are documented.
4. **"The cap was reached" and "rows are missing" are different facts.** The
   RawKV guard can compare `>` because the scan keeps reading until the range
   ends. The transactional scan stops at the cap, so it probes — one `KvScan`
   for a single row after the last key it returned — and throws only when
   that probe finds a row. A range with exactly 10240 rows is complete and is
   returned. The probe costs one extra RPC in the boundary case only.
5. **The write-set merge is not capped on the unbounded path.** `finalizeScanResults()`
   applied the scan limit to the *merged* result, so a transaction whose write
   set held in-range keys could have them dropped once the TiKV read had
   filled the budget — a second, quieter truncation of the same class. The
   unbounded path passes no limit to the merge: the cap bounds the TiKV read,
   and the transaction's own writes must stay visible (read-your-writes).

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
   connection. `null` now resolves to `GrpcClient::DEFAULT_TIMEOUT_MS` (30 s).
   The 30 s backstop is defence in depth, not the fix: the real deadlines are
   named per call site, so a 30 s default never has to be the one that fires.
   The "no deadline" sentinel `0` that used to sit next to it is gone — see
   the decision on a non-positive timeout below.
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
   gone: `RawKvBatch::timeoutMs()` returns `int` with a `default => throw`.
2. **No site may produce one.** The two that used to be enumerated — the
   unary transport's explicit `0` sentinel and
   `BatchCommandsConnection::open()`'s `$deadlineMs` stream-lifetime parameter
   — are both gone since #276 (GRPC-17): a non-positive value is a
   configuration error, so there is no way to ask for an unbounded call.
3. **`NoInfiniteDeadlineGuardTest` keeps it that way.** A guard is the right
   instrument precisely because the bug is an *absent* value: a behavioural
   test can only assert the paths it drives, and a value assertion says nothing
   about a site added tomorrow. It states the rule over
   `Timeval::infFuture()` **occurrences** rather than over `?:` and ternaries —
   every shape that can produce an unbounded deadline must mention it somewhere,
   so `match`, `??`, `if` and a hardcoded one are all caught, and a count-based
   allowlist (the `NoSilentRegionDropGuardTest` precedent) forces a conscious
   review whenever a site is added or removed. The allowlist is empty now, so
   the guard's witness is a **scan of a snippet** that contains an
   `infFuture()`: a scan that silently stops finding anything fails too.

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

## A non-positive timeout is a configuration error, not a "disabled" spelling (issue #276)

Issue #276 removed the last way to ask for an unbounded gRPC call. The
decision worth recording is *where* the rule is enforced, because the
alternative — validating only at the transport — was tried first and is worse:

1. **The rule is enforced where the value is configured, not only where it is
   used.** `TimeoutConfig`'s constructor rejects a non-positive value for
   every timeout field, and `ConnectionFactory` rejects a non-positive
   `options['timeout']` entry *before* the `TimeoutConfig` is built, so the
   error names the option key the user wrote. Validating only in
   `GrpcClient::deadline()` would have produced the same failure, but late: the
   error would surface on the first RPC instead of at construction, and the
   message would name an internal field rather than the configuration entry
   that carries the mistake.
2. **`0` is not a safe spelling for "disabled" in a timeout.** It was the
   library's documented "no deadline" sentinel, and it is also exactly what
   `(int) getenv('TIKV_READ_TIMEOUT_MS')` yields when the variable is unset —
   so the sentinel turned a configuration mistake into a call that never
   expires. The two knobs that still spell "disabled" as `0` are not timeouts
   in that sense: `batchDeadlineMs` is a wall-clock budget for the batch
   executor (it never reaches a gRPC deadline) and the slow-log thresholds
   disable a log line. Each config class states and enforces its own contract
   rather than leaving the meaning of `0` to be discovered at the transport.
3. **The change is a breaking one and says so.** A deployment that runs with a
   zero timeout now fails at construction — which is the desired outcome,
   because the previous behaviour was a latent outage. `CHANGELOG.md` carries
   the **BREAKING** marker and a migration note.

## The ext-grpc channel semantics are verified, not assumed (issue #283)

Issue #283 ("isolate `Grpc\Channel` instances across `GrpcClient` objects")
assumed that `Channel::close()` destroys the underlying channel, so that
closing one client invalidated the channel another client held. Verified
against the deployed extension (grpc 1.83.0, `src/php/ext/grpc/channel.c`),
that assumption is false: `close()` is a reference-count decrement
(`php_grpc_channel_unref()`), the process-global persistent table is itself a
holder of the channel, and a shared channel is always persisted — so closing
one client cannot destroy a channel another client holds.

The decisions that follow:

1. **Do not work around a hazard that does not exist.** `force_new` was
   rejected — it would give up the cross-request connection reuse for a
   hazard that does not exist — and dropping the PHP-side reference instead of
   `close()` was rejected as behaviourally identical to `close()` in that
   version (both are the same refcount decrement) while deleting the tests
   that cover the error handling of the close path.
2. **Pin the verified semantics with a regression test**
   (`tests/Unit/Grpc/GrpcClientChannelIsolationTest.php`), so a future
   extension that turns `close()` into a destroy fails the suite instead of
   silently breaking a second client.
3. **Document what the API does, not what it promises.** `closeChannel()`
   drops the cached handle but does not force a reconnect (the persistent
   entry survives, so the next call reuses the same underlying channel and
   the same TCP connection), and `close()` does not release the TCP
   connection (the persistent entry keeps it warm until the process ends).
   Both are now stated as they are.

The ownership model that answers "who closes what" — a transport belongs to
whoever created it, `options['grpcClient']` injects a borrowed one, and the
transport options are rejected next to it — was decided in issue #407 and is
documented in `README.md` and `docs/configuration.md`; it is not repeated
here.

## The four Percolator invariants of the TxnKV path (issue #182)

Closing the root-cause issue #182, whose four findings (TXN-01, TXN-02, TXN-04,
TXN-05 + DIV-01/DIV-02) were each already closed by merged work — #449/#270,
#586/#206 + #588/#209 + #595/#437, and #589/#210 + #590/#211 + #592/#263. The
decision worth recording is the invariant list, not the diffs, because each one
is a *silent* wrong-answer bug and a future change that looks like a
simplification is a regression:

1. **Every `*_ts` protobuf field is a PD TSO timestamp, never a monotonic
   clock.** `CheckTxnStatus.caller_start_ts` is the resolving transaction's own
   `start_ts` and `current_ts` comes from PD, so TiKV's
   `lock_ts.physical() + ttl < current_ts.physical()` can be true at all.
   Pinned by `LockResolverTest::testCheckTxnStatusSendsCallerStartTsAndFreshTsoFromPd`
   and `::testCheckTxnStatusSendsTsoMagnitudeTimestamps`; the rule itself is
   already in `faq.md` ("Every TiKV `*_ts` protobuf field…"), so do not restate
   it here — cross-link only.
2. **A live lock is retried, never rolled back.** `resolveLock()` throws
   `TxnRetryableException` for a lock whose `lock_ttl > 0` and lets the caller's
   `RetryExecutor` own the backoff; `commit_version = 0` is only ever sent when
   TiKV reported *no* commit and *no* live lock, and a `commit_ts > 0` is
   help-committed, never backed out. Pinned by
   `LockResolverTest::testResolveLockWithActiveLockThrowsRetryableWithoutResolving`
   (asserts **zero** `KvResolveLock` calls),
   `::testResolveLockWithCommitTsGetsCommitted` and
   `::testResolveLockDoesNotSleepForLiveLockRegardlessOfDeadline`. The
   `usleep(min($ttl, 20s))` before the unconditional rollback is the shape not
   to restore; a sleep inside a resolver is invisible to the caller's deadline.
3. **A pessimistic write is protected at write time, and prewrite still
   constraint-checks.** The default takes the physical lock inside
   `set()`/`delete()` carrying the preceding read's per-key `for_update_ts`
   (a fresh one only when there was no read), and the prewrite action is
   `DO_CONSTRAINT_CHECK` — `DO_PESSIMISTIC_CHECK` tells TiKV to *skip* the
   `start_ts` write-conflict check because it assumes the key is already locked,
   which is exactly the lost update. Pinned by
   `TransactionTest::testPessimisticSetAcquiresLockBeforeReturning` (eager lock
   RPC *and* the constraint-check action),
   `::testEagerPessimisticLocksSharePrimaryAndPerPassTimestamp`,
   `::testEagerPessimisticWriteReusesThePrecedingReadsTimestamp` and
   `OnePhaseAsyncCommitTest::testPessimisticPrewriteDefersConstraintCheckForBufferedWrites`.
   `begin()`'s `pessimistic` default is part of the invariant, not an
   implementation detail — `TxnKvClientTest::testBeginDefaultsToPessimistic` pins
   it, because when it was unpinned every other test in the suite still passed
   with the default flipped.
4. **A per-pair `KeyError` is never silently `null`.** `TxnReader` reads it for
   `batchGet()` and `scan()` and routes every variant through the single
   `handleReadKeyError()`. A locked key must *raise* (resolve, then retry), not
   come back as "not found", and a scan must not end at the first lock — both
   are snapshot-isolation violations the caller cannot see. Pinned by
   `TransactionTest::testBatchGetLockedPairResolvesLockAndRetriesInsteadOfReturningNull`,
   `::testBatchGetRejectsUnhandledPairKeyError` and
   `::testScanLockedPairResolvesLockRetriesAndReturnsNoMalformedRow`.

The closure did **not** implement the issue's "gate TxnKV behind an
experimental flag" recommendation: it was conditional on the defects being
open, and all four are closed in production *and* pinned, so a flag would gate a
path whose invariants are already verifiable. Do not add one without a new
finding — `BatchCommands` (#418) is the precedent for a genuinely experimental
surface, and it is opt-in per option rather than a client-wide gate.


## A scan's region list is loaded in bounded batches, and `ScanRegions` can only enumerate a range forwards (issue #282)

Two decisions inside the #282 fix (GRPC-23) that should not be "simplified" away:

1. **The number of regions a scan asks PD for is bounded, and the batch is
   `min(row budget, 32)`.** A scan used to pass `limit = 0` to
   `ScanRegions`, so a "first hundred rows" query over `['', '')` made PD
   enumerate the whole keyspace — a multi-megabyte response and tens of
   megabytes of `RegionInfo` objects for a query that reads one region. The
   bound is the *row budget* because a region that contributes to a scan holds
   at least one row, so a scan never needs more regions than it has rows left;
   the ceiling of 32 keeps the wide scans that really do span many regions
   within a few PD round trips. The region cache is still consulted first
   (#293), so a warm chain costs no PD round trip at all.

2. **The reverse scan still collects the whole chain of its range, and that is
   a limitation of `ScanRegions` plus this client, not a dead end in PD's
   API.** `ScanRegions` enumerates a range from its **lower** bound only —
   there is no "last N regions of a range" in that RPC — while a reverse scan
   consumes the regions from the **upper** bound downwards. The regions a
   reverse scan needs therefore cannot be fetched *with `ScanRegions`*: they
   are collected bottom-up (in bounded batches, so every PD response stays
   small) and then walked downwards. PD itself does offer a backwards
   lookup — `pdpb.PD/GetPrevRegion` (the vendored stub is
   `src/Proto/Pdpb/PDClient.php:205`) and `QueryRegion` with `prev_keys` —
   but this client does not implement it, so today the reverse scan pays for
   the chain. That is a follow-up, not a dead end: adding
   `getPrevRegion()` to `PdClientInterface`/`PdClient` would let
   `reverseScanPage()` resolve the top region with one lookup and continue
   downwards from there. Do not "fix" the current shape by asking PD for the
   region above the upper bound: that region does not exist when the upper
   bound sits exactly on a region boundary, which is the common case.

Two smaller rules the same fix settled:

- **The batch cursor is the last region's end key, and a region that starts
  below it is trimmed to it.** A *split* between two `ScanRegions` calls is
  harmless (the regions tile the keyspace and the half-open bounds mean the
  next batch starts at the next region's start), but a *merge* produces a
  region that spans the cursor — the part below it was read already and must
  not be read twice, so the region is trimmed to the cursor
  (`RegionScanBatch::trimToCursor()`). The trimmed region is a copy; the
  original is what the region cache holds.
- **The unbounded `array_push($results, ...$rows)` spreads are gone.** They
  pushed up to 10 240 elements onto the VM stack per region; a foreach append
  costs nothing and reads the same.

## The parallel prewrite reports its aggregated failure, it does not throw it (issue #431, GAP-17)

The prewrite fan-out (issue #291, PERF-04) collects every failing region into
a `BatchPartialFailureException` and rethrows the **first failing region's own
error**, so the sequential "abort at the first failing region" contract holds
and `docs/error-handling.md`'s exception list for `Transaction::commit()` stays
accurate — it does not mention `BatchPartialFailureException`, and callers
catching `GrpcException` or `RegionException` around `commit()` keep working.

Issue #431 asked for the partial failures to be "aggregated and retried per
region". Both halves were already in place: the fan-out waits for its
in-flight calls (so one region's failure neither cancels nor hides its peers'
responses) and each region retries on its own through its own `RetryExecutor`
loop. What was missing was the third step: the aggregate was **dropped**
together with the rethrown error, so a failure in several regions looked like a
single-region one — and the aggregate's own keys are group indices, which
identify no region. The aggregate is now logged with the identity of every
failed region before the first error is rethrown.

Do not "improve" this by throwing the aggregate: that changes the exception
type of every multi-region prewrite failure and breaks the callers the
first-error contract was written for. If the aggregate ever becomes part of the
public contract, it needs a documented accessor on the transaction path, a
`docs/error-handling.md` update and a migration note — not a silent change of
the thrown type. Pinned by `tests/Unit/TxnKv/TxnKvFanoutTest.php`
(`testFailingRegionsAreAllObservedAndTheFirstErrorSurfaces`,
`::testFailingRegionIsRetriedOnItsOwnWhilePeersAreNotResent`,
`::testAggregatedPrewriteFailureNamesEveryFailingRegion`).

## An option the client cannot apply is an error, not a no-op (issue #272)

The concrete symptom — `options['timeout']['ingestTimeoutMs']` was dropped on
the floor, so an operator who configured a five-minute SST ingest deadline got
the 60-second default and no warning — was fixed by #580, which re-added the
key to the hand-written list. Issue #272 records the decision that keeps it
fixed:

1. **The mapping is derived from the config class, not written out.** The
   factory reads `TimeoutConfig`'s and `SlowLogConfig`'s constructor
   parameters and maps `options['timeout']` / `options['slowLog']` onto them
   by name. A hand-written list is a list that drifts: the list had already
   lost one field, silently, and nothing noticed because the value it lost
   was only wrong on a slow store. A field that is added to the class is
   configurable the moment it exists.
2. **Three things are rejected: an unknown key, a value of the wrong type,
   and a group that is not a map.** A silently dropped option has no effect
   and the default applies — the user believes a deadline was raised and it
   was not. The same is true of a value that is not applied because it has
   the wrong type (`'readTimeoutMs' => '60000'` from an environment
   variable), and of a group that is not a map at all
   (`options['slowLog'] => 'x'` silently turned the slow logging *off*). The
   TLS options already failed closed on a mistyped value; the timeouts and
   the slow-log thresholds now do too. Rejecting what was never read is a
   behaviour change, but it converts a silent misconfiguration into a startup
   error, which is the correct trade. An option group that is absent — or
   explicitly `null` — is not a misconfiguration: it means "not configured"
   and keeps the default.
3. **The regression test is derived from the class as well.** The test that
   every field round-trips enumerates the constructor parameters, so a field
   that is added without a mapping fails the suite instead of the cluster.

## The transport belongs to whoever created it (issue #407)

The gRPC transport is **long-lived and shared** — one instance keeps the channel
cache that only pays off when it serves many requests — so "who closes it" has
to be unambiguous, and the answer is not "whoever calls `close()`":

1. **A transport the factory builds belongs to the client.** `create()` builds
   the transport, the store cache and the PD client, and `close()` closes the
   transport it built.
2. **A transport the application supplies belongs to the application.**
   `options['grpcClient']` hands a ready-made `GrpcClientInterface` to the
   factory; the client uses it as it is and **never** closes it, so one
   transport can serve a RawKV and a transactional client and closing one of
   them cannot break the other. The transport options (`tls`, `allowInsecure`,
   `maxChannels`, `idleTtlMs`, `grpc`) are rejected next to it instead of
   ignored — an ignored `allowInsecure: false` would leave the caller
   believing TLS is enforced while the transport is used without it.
3. **A client assembled by hand closes the transport it was given.** The public
   constructors of `RawKvClient` / `TxnKvClient` default `ownsGrpcClient` to
   `true`, so `new RawKvClient($pdClient, $grpc)` closes `$grpc` in `close()`
   and in the destructor. `ownsGrpcClient: false` is the documented escape
   hatch for an application that keeps the transport. Issue #204 tracks the
   change (a client should close only what it owns); until it lands the flag is
   the only way to say so.

The rule is published in three places, and a reader of any one of them has to
find the same answer: `GrpcClientInterface::close()` (the API contract), the
README ("Connection Lifetime and Transport Ownership") and
`docs/configuration.md` ("Sharing One Transport Between Clients" plus the
manual-construction snippet). Keep them in sync — the contradiction #407 had to
fix was a snippet sitting next to the rule and breaking it.

## The transaction mode is stated explicitly, but `begin()` keeps its pessimistic default (issue #439)

Issue #439 (DIV-03) recorded the divergence from the official clients:
`TxnKvClient::begin()` defaults `pessimistic` to **true**, while the TiKV
documentation, client-go and client-rust make **optimistic** the default. The
decision here is to keep the default and to remove the ambiguity from new code
instead of changing the behaviour:

1. **The default stays pessimistic.** Flipping it is a breaking change and
   belongs in a major release with a CHANGELOG migration note, not in a
   patch-level issue. The default is part of the Percolator invariants recorded
   above (`begin()`'s `pessimistic` default is pinned by
   `TxnKvClientTest::testBeginDefaultsToPessimistic`), so it is not an
   incidental implementation detail either.
2. **`beginOptimistic()` and `beginPessimistic()` are thin wrappers over
   `begin()`**, not a second implementation of it: the option handling, the
   validation and the transaction construction stay in one place, so the three
   entry points cannot drift apart. The mode is the only thing they fix.
3. **A contradicting `pessimistic` option is rejected, not overridden.** The
   mode belongs to the method that names it, so `beginOptimistic(['pessimistic'
   => true])` throws an `InvalidArgumentException` instead of silently
   ignoring half of its own arguments. A redundant, agreeing value is
   accepted.
4. **The default is documented where a reader meets it** — the README's
   "Optimistic vs pessimistic" section, `docs/getting-started.md` and the
   `begin()` docblock — including the note that it differs from the official
   clients, so the divergence is a documented decision rather than a surprise.

## The README project-structure tree is checked by a unit test, not by review (issue #410)

A documentation tree drifts silently: nothing fails when a package is added,
a class is renamed or a DTO is deleted, and the tree is the reader's first map
of the codebase. `tests/Unit/Docs/ReadmeProjectStructureTest.php` therefore
parses the tree out of `README.md` and compares it with the filesystem. Four
decisions inside it are worth keeping:

1. **The completeness check covers `src/Client/`, the test suites and the
   example scripts — and not the whole of `src/`.** The protobuf packages
   under `src/Proto/` are generated from the proto submodule and change with
   it, so demanding that the README list all of them would make the tree
   drift for a reason no reader cares about. The three packages the tree does
   name are still checked for existence, and the README marks the list as
   partial (an ellipsis in the tree line) so that a reader cannot take three
   packages for the whole set. The tree is a map, not a file inventory: it
   names the test suites and not the directories below `tests/Unit/`, and the
   README says so.
2. **A word in parentheses in a tree comment is a class name** (when it starts
   with an uppercase letter and contains a lowercase one) and has to exist
   under `src/Client/` or be a generated protobuf package. This is what keeps
   the tree from advertising a type the API does not have — the defect
   `Dto/KeyValue` was: a class that existed on disk but that nothing used,
   advertised as a shared DTO. Prose in parentheses stays lowercase, and an
   acronym ("PD", "TLS", "GC") is ignored.
3. **The comparison itself is tested.** A comparison that always answers "no
   drift" would keep the suite green while the tree rots, so
   `testTheGuardReportsDriftInBothDirections()` drives it with a synthetic
   tree and asserts that both an omitted and a stale directory are reported.
4. **The removal of `Dto/KeyValue` is announced as a breaking change.** The
   class was public API of seven released versions (v0.1.0 … v0.8.0), so the
   `### Removed` entry in `CHANGELOG.md` says both that the class was public
   and that its removal breaks a consumer that instantiated it directly. No
   deprecation cycle precedes it: no API ever returned the class, and
   `docs/release-workflow.md` allows breaking changes before `1.0.0`.

## ScanIterator enforces cursor progress but has no page ceiling (issue #312)

`ScanIterator` throws `TiKvException` when a page does not progress: the
first key lies before the cursor, or a key is not strictly greater than
the key before it. Together these keep every key at or after the cursor,
so the next cursor (`lastKey . "\x00"`) always moves forward. All
checks use `KeyOrder`, so the comparison is bytewise (`"99"` is greater
than `"100"`; see the #232 and #180 entries). Any forward scan can hang
without the checks, bounded ones too: a constant last key keeps the cursor
below `endKey`. The cost is O(batchSize) compares per page.

There is deliberately **no page or row ceiling**. The iterator promises
constant-memory streaming of ranges of any size. A server that fabricates
a growing key space is not distinguishable from a large real one, so a
ceiling would break legitimate scans.

One case stays open. A server that answers a one-row page equal to the
cursor key (`batchSize = 1`) makes the cursor grow by one `"\x00"` per
page. It passes the checks, as in the one-byte advance of the #288 entry.
The caller bounds the iteration (a row count, a deadline, `break`).

## An empty KvScan batch is re-asked once, outside RetryExecutor (#512)

`TxnReader::executeScanForRegion()` re-asks a sub-range once when it
answers an empty batch while the limit is unsatisfied and the scan would
end there. The probe of `assertRangeNotTruncated()` is covered too: a
transient empty probe would hide a truncated whole-range scan (#440).
Trade-offs:

- `MAX_EMPTY_SCAN_REATTEMPTS = 1`, counted per sub-range: the counter
  resets when a batch is non-empty and when the scan moves to the next
  region after a split.
- Limit 0 normalizes to 10240, so every empty sub-range now costs two
  KvScan RPCs plus a `BackoffType::RegionMiss` sleep of about 2 ms.
- The sleep is a plain `usleep()` outside `RetryExecutor`: it does not
  count against `maxBackoffMs` or the retry deadline and emits no
  retry metric.
- A persistent empty answer is accepted after the re-ask and logged at
  debug level with a redacted key.

## `composer.lock` is committed and pinned to the lowest PHP (#311)

The lock file is tracked and CI and the Docker image install from it.
Before this, every run resolved the newest dev tools. A new Rector,
PHPStan or `php-code-coverage` release could turn `master` red, and a
vulnerable `squizlabs/php_codesniffer` went unnoticed.

`composer.json` sets `config.platform.php` to `8.2.0`, the lowest
supported PHP. Without it, a resolve on a newer host PHP (8.5) picks
packages such as `symfony/string` 8.x that need PHP 8.4.1 or later. The
lock then breaks the PHP 8.2 and 8.3 CI jobs and the PHP 8.3 Docker
image. Raise the pin only when `require.php` drops a PHP version.

The CI `lint` job runs `composer audit --locked`, so a new advisory
fails the build. Downstream users of the library are not affected:
Composer ignores the lock file of a dependency.

## The idle sweep touches the returned channel before it sweeps, and `reapIdleChannels()` is public on the clients (#277)

Issue #277 asks that the sweep run **before** the cache lookup, so a channel
that is idle past the TTL is reclaimed and not refreshed by the call that
finds it. The code does the opposite: on a cache hit it sets `lastUsed` of the
returned channel first and then sweeps. Reason: a recreate does not give a new
connection. ext-grpc keeps the channel in its persistent table, so the new
handle resolves to the same `grpc_channel` (see the #283 entries). Reclaiming
the requested channel and building it again would add work and no benefit, and
it would risk closing the handle that the caller is about to use. The sweep
still reclaims every **other** idle channel, which is what the issue needs.

Other decisions of #277:

1. The sweep interval is `min(idleTtl, max(1 s, idleTtl / 10))`, so it scales
   with the TTL. A fixed 30 s would exceed a small TTL.
2. The clock is a `?\Closure` constructor argument (default `microtime(true)`).
3. `reapIdleChannels()` is on `GrpcClientInterface` (workers may hold the
   interface) and on `RawKvClient` and `TxnKvClient`, because a worker holds
   those and cannot reach the `GrpcClient` that the factory builds. It is a
   no-op after `close()`, like `closeChannel()`.
4. `maxChannels` eviction runs down to the cap in one call. The soft-cap
   warning is logged once per overshoot episode (re-armed when the cache is
   back under the cap), not on every growth of the maximum: a fan-out over
   1 000 stores raises the maximum on every new address and would still log
   once per address.
5. #658 (BatchCommands stream on an unprotected channel) is not fixed here: it
   needs a way to hold a channel in flight for the lifetime of a stream, on
   the interface, and the transport is experimental and off by default.

