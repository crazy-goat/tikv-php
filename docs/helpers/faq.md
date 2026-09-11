# FAQ — Recurring Pitfalls

Frequently asked questions and recurring pitfalls in crazy-goat/tikv-php.
Ordered roughly by how often they bite.

## RetryExecutor unit tests that need zero-sleep retries: use DataIsNotReady or an explicit classifier

Since issue #241 (REG-10) `EpochNotMatch` classifies as `BackoffType::EpochNotMatch`
(2 ms base, 500 ms cap, equal jitter) instead of `None` — so a test that threw
`TiKvException('EpochNotMatch …')` 29 times to hit the attempt cap now sleeps
~10 s of real wall clock and burns ~11 s of budget. To keep a retry test fast
and deterministic: (a) throw `DataIsNotReady` — still the only zero-sleep class,
so the attempt cap fires in milliseconds; (b) pass an explicit
`fn () => BackoffType::None` classifier; or (c) when you WANT the new behavior,
use a small `maxBackoffMs` (e.g. 50) so the sleep budget exhausts after a
handful of 2/4/8/16 ms retries — a regression test can then assert
`attempts < DEFAULT_MAX_ATTEMPTS` and a minimum wall-clock elapsed time.
Remember that budget exhaustion for sleep-budget classes **rethrows the
original TiKvException**, not `RetryBudgetExhaustedException`, and that
`Backoff::exponential(..., equalJitter: true)` returns `[base/2, base]` — so
`sleepMs(100)` for a cap of 500 returns in `[250, 500]`, never exactly the cap.

## Sizing a retry-budget regression test under equal jitter: do the min-cumulative-sleep math

`BackoffType::sleepMs()` delegates to `Backoff::exponential()`, which takes
`equalJitter` **per call site** — the enum's `equalJitter()` match decides it.
Any NEW `BackoffType` case must opt into jitter explicitly or it silently
gets full (non-jittered) exponential sleeps; there is no default jitter.

When a regression test must prove "budget exhausts before the attempt cap",
size `maxBackoffMs` from the worst case, not the happy path: with equal
jitter on base `b`, attempt n sleeps in `[b·2^n/2, b·2^n]` (capped), so the
**minimum** cumulative sleep after k retries is `b·(2^k − 1)` — for
`EpochNotMatch` (b=2): 1, 3, 7, 15, 31, 63. A `maxBackoffMs: 50` budget
therefore exhausts by retry ~6 under ANY jitter draw (63 > 50), which is what
makes `assertLessThan(DEFAULT_MAX_ATTEMPTS, $attempts)` un-flakeable.
Conversely a wall-clock `elapsedMs` lower bound must be provable from the
sleeps alone (budget exhaustion implies > 50 ms of sleeps), not from
scheduling overhead. The #241 tests in `RetryExecutorTest` are the reference
implementation of this sizing.

## PHPStan: `$this->fail()` directly after a call whose closure always throws is dead code

A test like `try { $executor->execute($k, fn () => throw …); $this->fail(...); } catch (TiKvException) {}`
gets `deadCode.unreachable` at PHPStan L9: the closure's unconditional `throw`
makes `execute()`'s success path unreachable, so PHPStan proves the statement
after the call never runs. Do NOT "fix" it with a by-ref `$exhausted = false`
flag set in the catch and `assertTrue($exhausted)` after — that trips the
other trap (`method.alreadyNarrowedType`: PHPStan proves the flag is always
true, the exact escape hatch documented for #503 does not apply here because
the flag is set in a catch, not a sibling closure). Just drop the flag and the
`fail()`: keep the empty catch with a comment, and assert on observable state
after the try/catch (attempt counts, elapsed time) — those assertions fail
anyway if the code under test misbehaves (issue #241).

## E2E tests need a running TiKV cluster (Docker)

The `E2E-RawKV` and `E2E-TxnKV` testsuites require real TiKV nodes. Start
the cluster with `make up` (PD on 2379, tikv1/2/3 on 20160/20161/20162),
stop with `make down`. If state gets corrupted: `make clean && make up`.

## Protobuf `RepeatedField` is not a PHP array — `iterator_to_array()` before `array_map()`

Proto getters returning repeated fields (`PrewriteRequest::getMutations()`,
`BatchGetResponse` pairs, …) return a `Google\Protobuf\Internal\RepeatedField`
which is iterable but not an `array`: `array_map()`/`array_keys()` on it
throw a `TypeError` under `declare(strict_types=1)`. Wrap with
`iterator_to_array()` first (or index into it / foreach), and remember PHP
coerces numeric-string keys, so `(string)`-cast keys taken from proto fields.

## Unit tests don't need TiKV — gRPC tests do

- `composer test:unit` (`--testsuite Unit`) mocks gRPC calls — fast, no
  cluster, no `grpc` extension needed.
- `composer test:grpc` (`--testsuite Grpc`) exercises real gRPC connections
  missing extension fails the run locally.

## PHP coerces numeric-string array keys to int — string-cast at every typed consumption point

When a string that parses as an integer is used as an array key (`$map['12345']`,
`$map['0']`), PHP stores it as an **int** key; there is no way to keep
`'12345'` as a string key. Under `declare(strict_types=1)` this silently breaks
every typed consumer of that key: `validateKeyNotEmpty(string $key)`, proto
`setKey(string $var)`, `scanRegions(string $startKey)` all throw a TypeError
that `catch (TiKvException)` does not catch, and `getPrimaryKey(): string`
throws when the stored key is returned. Rules learned fixing it (issue #322):
key-returning accessors (`getPrimaryKey()`, `getWriteKeys()`) must cast
`(string)` before returning; every point that hands a foreach key to a
string-typed parameter or setter must cast `(string) $key`; and lists built
with `array_keys()` from such maps are normalized to strings at the public
API boundary — `Transaction::batchGet()`, the transaction scan result merge,
and `RawKvClient::batchGet()/batchDelete()` accept `string|int` elements,
cast ints back to strings, and reject anything else with
`InvalidArgumentException` (never `strval()` arbitrary elements silently).
The same applies to keys *returned* by TiKV: scan responses and
`BatchGetResponse` pairs store `'12345'` as int 12345 inside PHP maps, so
`array_keys()` results must be string-cast before `hasWriteSetKey()` lookups,
and merged result maps must not be combined with `array_merge()` (it
renumbers int keys and drops numeric-string entries). A unit test that builds
the map with a *literal* key also
hides the bug in reverse: PHPStan infers the literal as `array<int, string>`
and rejects the `array<string, string>` parameter, so construct the map via a
string-typed key parameter (helper method) to test the real-world contract.

## There is no pre-push hook in this repo

Lint is only enforced in CI. Run `composer lint` locally before pushing to
avoid wasting a CI cycle.

## CI is skipped entirely for non-collaborators

`.github/workflows/ci.yml` starts with a `check-actor` job: only the repo
owner or collaborators with admin/maintain/write permission trigger CI.
External contributors must ask a maintainer to review and run the workflow.

## gh issue list returns at most 30 issues by default

Always pass `--limit` (e.g. `--limit 150`) when triaging issues or searching
for duplicates, otherwise issues beyond the first page are silently missed.
Same applies to `gh pr list`.

## No `gh milestone` subcommand — use the API

This gh version has no `milestone` command. List milestones via the API:

```bash
gh api "repos/crazy-goat/tikv-php/milestones?state=open&per_page=100" \
  --jq '.[] | "\(.title)\topen:\(.open_issues)"'
```

Filter issues by milestone with `gh issue list --milestone "<title>"`
(and `gh issue create --milestone "<title>"`).

## Work starts from the lowest open version milestone

Issues are grouped into version milestones (`v0.4.0` … `v0.14.0` open;
`v0.3.0` and lower closed). Pick the next issue only from the **lowest-version
milestone that still has open issues**; higher milestones wait. Within the
milestone, severity labels decide: `severity:critical` → `high` → `medium` →
`low`.

## E2E job runs two clusters, one at a time

CI's `e2e-tests` job first boots a V1ttl cluster (`docker-compose.yml`)
for RawKV, tears it down with `-v`, then boots a V1 cluster
(`docker-compose.txnkv.yml`) for TxnKV. Locally the TxnKV setup is also
available via `docker-compose -f docker-compose.yml -f
docker-compose.txnkv.yml up`.

## grpc-unit-tests collects coverage, but there is no gate

CI runs the Grpc testsuite with `--coverage-xml` under PCOV, but no
coverage floor is enforced anywhere (`composer.json` has no
`coverage:check`). Don't block PRs on coverage percentages.

## Every TiKV `*_ts` protobuf field must be a PD TSO timestamp, never a monotonic-clock value

TiKV interprets every timestamp protobuf field (`caller_start_ts`,
`current_ts`, `start_version`, `commit_version`, `for_update_ts`, …) as a
PD TSO timestamp: `physical_ms_since_epoch << 18 | logical`, on the order of
`1e17`. `hrtime()`/`microtime()` return boot/process-relative values (~`1e9`)
that are orders of magnitude smaller, are not comparable across processes,
and reset on reboot. Sending them in a timestamp field breaks TiKV's lock
TTL-expiry and min-commit-ts logic (issue #270: abandoned locks were never
detected as expired). Always obtain timestamps from
`PdClientInterface::getTimestamp()` (PD TSO, fails closed). The only
legitimate uses of `hrtime`/`microtime` in timestamp positions are duration
measurements (differences) and logging — and `TimestampOracle::getTimestamp()`
accepts an optional `$timeoutMs` so TSO fetches can carry a finite deadline.

## gRPC target strings accept more than host:port — always validate PD-supplied addresses

The grpc-core channel constructor (`Grpc\Channel`) treats the target string
as a URI: besides `host:port` it also accepts `unix:/path/to.sock`,
`unix-abstract:<name>`, `dns:///host:port`, `ipv4:` and `ipv6:` schemes, and
an empty check on the address lets all of them through. Since store addresses
arrive from PD (a network peer, plaintext by default), every address used as a
channel target must be validated before it reaches `new Channel()`. In this
repo `RegionResolver::resolveStoreAddress()` enforces a strict `host:port`
regex unconditionally and throws the distinct `InvalidStoreAddressException`
(logged) instead of `StoreNotFoundException` when PD returns something else
(issue #306, SEC-03).

## PHP properties can never be typed `callable` (even nullable) — use `Closure`

`private ?callable $x` and `private callable $x` are fatal errors in every
PHP version, including 8.5 (only parameters and return types accept
`callable`). When a class needs to hold a callable, type the property
`?\Closure` and convert user-supplied callables at the boundary with
`Closure::fromCallable()` (see `ConnectionFactory::resolveStoreHostValidation()`,
added for issue #306). PHPStan level 9 also rejects casting `mixed` to string
(`(string) $level` in a PSR-3 `log($level, …)` implementation) — narrow with
`is_string()` instead.

## `$` in PCRE matches before a trailing newline — anchor with `\A…\z` for strict string validation

In PHP, `preg_match('/^...$/', $s)` returns 1 for `"evil:20160\n"` because `$`
also matches immediately before a final newline. Any strict string-format
check (store addresses, identifiers, ports) must use `\A…\z` instead — and
when the validated value is numeric, also range-check it (`0` or `99999` pass
`\d{1,5}`). This bit the SEC-03 store-address validation in issue #306: the
original `/^[A-Za-z0-9._-]+:\d{1,5}$/` accepted a trailing-newline address
(the gRPC target parser tolerates it) and out-of-range ports; the fixed
`RegionResolver::parseHostPort()` parses host/port explicitly with `\A…\z`
anchors and a 1–65535 port range, and additionally accepts bracketed IPv6
(`[2001:db8::1]:20160`) with an `inet_pton` check on the host.

## Classify the store-address host before policy matching — IPs are not DNS names

The default PD-derived store-host policy (issue #306, SEC-03 round 2) must
classify the host before applying any rule; DNS-style suffix matching on an
IP literal is a security hole. Bracketed IPv6 literals are trusted only when
byte-identical (`inet_pton`) to a configured PD IPv6 endpoint — zone-id forms
(`[fe80::1%eth0]:20160`; PHP ≥ 8.2 `inet_pton` accepts them) and IPv4-mapped
forms (`[::ffff:10.0.0.1]:20160`) are rejected, no subnet/suffix rules apply.
IPv4 literals only match by byte equality or /16 subnet (first two octets) —
`10.0.0.1` shares the textual suffix `.0.1` with `127.0.0.1` and must NOT
match it. Digit-leading hosts (`2130706433`, `017700000001`, `0x7f000001`)
are system-resolver numeric-IP aliases and are rejected. Separately, a host
that is itself a reserved gRPC/URI scheme name (`unix:20160`, `dns:20160`,
`ipv4:20160`, `vsock:20160`, …) is rejected case-insensitively in
`RegionResolver::validateStoreAddress()` before the policy runs, because
grpc-core treats the prefix as a URI scheme at `new Channel()` time.

## Store ports are part of the trust decision — the default policy rejects privileged ports

A store host that passes the default PD-derived policy is only half the
trust question: with PD at `10.0.0.1:2379` the /16 rule admits
`10.0.0.2:1`, and an exact trusted host with port `1` is equally
dangerous — a compromised PD could redirect traffic to an arbitrary
service on the same host or subnet. Since round 3 of SEC-03 (issue #306)
the default policy therefore requires the store port to be `>= 1024`
unless it is explicitly listed in the new `options['allowedStorePorts']`;
when that option is set, the port must be in the list (it narrows or
relaxes the guard). On the explicit `allowedStoreHosts` path ports stay
unrestricted unless `allowedStorePorts` is set (backward compatible).
`storeHostPolicy` receives the full `host:port` and is never touched by
the port policy.

## The shared-suffix rule must be derived from DNS-name PD hosts only

Round 3 of SEC-03 (issue #306) closed a second suffix bypass: the default
policy derives the last-two-DNS-label suffix from the configured PD
hosts, but with PD at `10.0.0.1:2379` the textual suffix `.0.1` admitted
`attacker.0.1:20160` even though the PD host is an IP literal. The suffix
rule now runs only when the PD host is a real dotted DNS name
(`isDottedDnsName()`): entries that parse via `inet_pton` (IPv4/IPv6
literals, including IPv4-mapped forms like `::ffff:127.0.0.1`), that are
digit-leading (`123.456.789`), or single-label never contribute a suffix.
Exact-match and /16 rules are unchanged.

## grpc-core 1.80 registers more resolver schemes than the classic set

Besides `unix`, `unix-abstract`, `dns`, `ipv4`, `ipv6`, `vsock`, `http`,
`https`, `tcp`, `tls`, grpc-core 1.80 also treats `xds`,
`google-c2p` and `google-c2p-experimental` as URI schemes when they
appear as the host part of a channel target (`xds:20160` etc.). The
reserved-scheme rejection set in `RegionResolver::validateStoreAddress()`
must keep up with the grpc-core release that ships with the runtime —
when bumping the `grpc` extension, re-check the scheme list added for
SEC-03 (issue #306).

## The pessimistic-lock retry loop usually exits via the do-while condition, not the budget guard

In `TwoPhaseCommitter::pessimisticLockBatch()` the per-attempt `remainingMs
<= 0` guard looks like the retry-budget exit, but it is almost unreachable:
`delayMs` is capped by `remainingMs` before sleeping, so `elapsedMs` lands
exactly on `maxBackoffMs` and the loop leaves through the
`while ($elapsedMs < maxBackoffMs)` condition instead (issue #219, TXN-14).
Both exits must be treated as "lock acquisition failed" — the fix throws
`LockWaitTimeoutException` after the loop whenever `$needRetry` is still
true. Unit-test tip: pass a small `maxBackoffMs` (e.g. 100) to
`createTransaction(['maxBackoffMs' => …])` to exercise budget exhaustion
without sleeping the full default 20 s budget — and `maxBackoffMs = 0`
hits the `remainingMs <= 0` guard after the very first locked response.

## Retry closures must re-resolve the region inside the loop — scans resolve on the sub-range start, reverse scans on the sub-range end

`RetryExecutor` invalidates the region cache and switches leader hints on
`NotLeader`/`EpochNotMatch`, but those updates only take effect if the
retried closure asks `RegionResolver::getRegionInfo()` again on every
attempt. Scan closures (issue #267, GRPC-08) used to capture the
`RegionInfo` resolved before the loop, so every retry re-sent the same
stale region/epoch/leader up to the attempt cap. The fix moves resolution
inside the closure AND pre-populates the cache with the `scanRegions()`
result (otherwise every sub-range does an extra PD `getRegion` round trip
per attempt and `switchLeader` hints are ignored). Subtleties: the forward
scan resolves on the sub-range *start* (strictly inside the region, so
`getByKey` hits); the reverse scan resolves on the sub-range *end* (the
lower bound), because the wire start key can sit exactly on the region's
end boundary where the cache lookup misses and PD answers with the
*neighbouring* region. After a split the fresh region is smaller, so the
wire range must be re-clipped on every attempt (end key for forward,
start/upper key for reverse) — TiKV rejects ranges that cross region
boundaries. Same stale-capture bug remains in `RawKvRangeOps`
(deleteRange/checksum) as of this fix. The rollback closures
(`batchRollback()`, `pessimisticRollbackAll()` in `TwoPhaseCommitter`)
had the same bug and were fixed the same way (#502): `getRegionInfo()`
inside the closure on every attempt — there the group's
`batchResolveRegions()` already populated the cache, so the first
attempt is a cache hit and no extra PD round trip is added. Unlike the
scan fix, the retry does not re-clip or re-group: after a split the
re-resolved region may not cover the whole key group and the server
keeps erroring until the budget is exhausted (documented limitation,
same as the #500 lock path).

## PHPStan `ternary.alwaysFalse` fires for by-ref bool flags mutated in a sibling closure — sequence mocks instead

When a test shares state between mocked-method callbacks through a
by-ref bool (`$flag = false; A sets $flag; B reads $flag ? x : y`),
PHPStan analyses each closure in isolation, proves the flag is always
false inside B, and reports `ternary.alwaysFalse` at level 9
(`if.alwaysFalse` for the equivalent if). For mock call sequences with a
deterministic order (e.g. stale region served for the closure's first
resolution and the executor's invalidation lookup, then a miss) use
`willReturnOnConsecutiveCalls($stale, $stale, null)` instead of a shared
flag — it models the same state machine, is order-visible in the test,
and is PHPStan-clean.

## Bash 3.2 portability: process substitution, tab-IFS, BSD/GNU sed/date

Lessons from building `bin/pick-issue.sh` (#457); the script itself is the
reference implementation of each workaround.

- **Process substitution leaks file descriptors inside long loops on bash
  3.2.** A `<( ... )` used inside a `while ... done` loop keeps its
  descriptor open until the *loop* ends, not the iteration: after a few
  hundred lookups the fd limit is exhausted and a later `read` blocks
  forever on a pipe nobody writes (hangs with no error). Split the data
  once per item with parameter expansion (`${var//"$SEP"/$'\n'}`) and
  iterate the pieces with a here-string (`<<<`).
- **`IFS=$'\t' read` collapses empty fields.** Tab is IFS whitespace, so
  `a<TAB><TAB>b` parses as two fields, not three — an issue without
  labels shifted `created_at`/`comments` and produced invalid JSON. Use a
  non-whitespace separator (e.g. `\x1f`, stripped from the data in the
  `gh api --jq` projection) or put the variable-length part in a trailing
  field.
- **GNU and BSD ports of sed/date differ.** BSD `sed` has no `:a;N;$!ba`
  newline-joining idiom — park newlines on a control char with `tr`
  first; `date -d <ts>` is GNU, `date -j -f '%Y-%m-%dT%H:%M:%SZ' <ts>` is
  BSD — detect the implementation at runtime.

## A negative scan limit is a uint32 overflow, not a no-op — reject it before the wire

`RawKvScanner::validateScanLimit(int $limit)` handled `0` (→ `MAX_SCAN_LIMIT`)
and `> MAX_SCAN_LIMIT` (→ throw) but passed any negative value straight
through to `RawScanRequest::setLimit()`. That protobuf field is `uint32`,
so `setLimit(-1)` serialises to `4294967295` and TiKV runs an effectively
unbounded scan (OOM in the PHP worker, sustained load on the cluster) —
`MAX_SCAN_LIMIT` exists precisely to prevent this and was bypassed. The
fix (issue #332) rejects negatives with
`InvalidArgumentException('Scan limit must be 0 or greater')` placed
*before* the `=== 0` check, using the exact message `TxnReader::normalizeScanLimit()`
already used so the RawKV and TxnKV paths agree. Lesson: any signed PHP int
that feeds a `uint*` protobuf field must be range-checked at the entry
point, not just for the obvious upper bound — a negative wraps to a huge
unsigned value. `batchScan`/`scanIterator` use a `<= 0` guard with their
own ('eachLimit'/'batchSize' must be greater than 0) messages; the scan
limit itself uses the 'Scan limit must be 0 or greater' message.

## Metrics on a shared collaborator: nullable + accessor + in-place attach, and one sole owner per emission reason

Issue #474 (regionInvalidated()) settled three rules worth reusing:

1. **Attach-if-absent wiring needs a nullable collaborator.** Do not default
   the constructor param to a live instance (`NoOpMetrics`) — every cache then
   looks "already wired" and the client-side check can never fire. Declare the
   property/param `?MetricsInterface = null`, expose a
   `metrics(): ?MetricsInterface` accessor, emit via `$this->metrics?->...`.
2. **Never clone or rebind a user-supplied shared object.** The first cut used
   a clone-returning `withMetrics()`, which silently dropped the wiring for
   every component holding the original cache (the clone went into one client,
   the rest kept the un-wired original). The final shape is an in-place
   mutator: `attachMetricsIfAbsent()` assigns only when the backend is null;
   client constructors call it on the promoted readonly property — mutation,
   not rebinding.
3. **One sole owner per metric reason.** NotLeader region errors flow through
   BOTH RegionErrorHandler::check() and RetryExecutor::handleNotLeader(); if
   both invalidate, each attempt double-counts and — worse — check() drops the
   region before handleNotLeader can switchLeader() to a still-valid hint.
   check() now skips NotLeader oneofs entirely; handleNotLeader is the only
   code path that invalidates with 'not_leader'. Gate any choke-point emission
   on an actual state change (`removeById(): bool`) so retry storms count one
   real drop instead of one per attempt.

## Error-handling docs must be derived from source, not from the issue text

Issue #394 [DOC-28] (docs/error-handling.md) said "fifteen exception classes";
the real count was **sixteen** — PR #472 had added
`BatchPartialFailureException` after the issue was written. When a docs task
involves enumerating classes/methods/exceptions, grep the source tree at
implementation time and treat the issue's numbers as stale-by-default. Same
class of trap as #376: an audit issue's premise can silently expire when a
later PR changes the code it describes.

## TTL mode and TxnKV are mutually exclusive — say it at every enable-ttl recommendation

TiKV with `[storage] enable-ttl = true` runs in V1TTL storage mode, which
serves RawKV-with-TTL but **not** transactional requests; a single cluster
cannot serve both (see `src/Proto/Kvrpcpb/APIVersion.php`: "V1TTL is only
available to RawKV"). The repo itself splits E2E by mode: `tikv.toml` has
`enable-ttl = true` (RawKV suite), `tikv-v1.toml` omits it (TxnKV suite via
the `docker-compose.txnkv.yml` override). Lesson from issue #405 [DOC-39]:
every doc/example that recommends `enable-ttl` must carry the exclusivity
note, the TxnKV docs must state the default-V1-mode prerequisite, and there
is **no verbatim server error string** to quote for "TxnKV fails on a
TTL-enabled cluster" — a troubleshooting entry for it must use prose
diagnosis, not a fake `**Error:**` code block (the file's convention is
that quoted strings are verifiable verbatim). Switching `enable-ttl` on a
live cluster requires wiping data — an operational migration.

## PHPUnit `--fail-on-skipped`: any skip fails, and mocking `\Grpc\Call` without ext-grpc ERRORs (not skips)

Facts verified empirically (PHPUnit 11.5.55, PR #484 for #323/#358/#359):

1. `--fail-on-skipped` / `failOnSkipped="true"` makes **any** skipped test
   (runtime `markTestSkipped()` AND `@requires`-based skips) exit 1. There is
   no "only runtime skips" escape hatch.
2. `createMock(\Grpc\Call::class)` with **no** `grpc` extension is a hard
   `UnknownTypeException` **error**, not a skip — so a per-method
   `@requires extension grpc` cannot keep a `Call`-mocking test in a job
   without ext: the test skips (fails under `--fail-on-skipped`) if the
   annotation fires, or errors if it doesn't.
3. Consequence: in a suite that runs without ext (the `unit-tests` CI job),
   **any** test that mocks `\Grpc\Call` must live in a different suite, not be
   gated. The pattern used in PR #484: whole classes that mock `Call` move to
   the `Grpc` testsuite (`phpunit.xml` `<file>` entries + `<exclude>` from
   `Unit`); tests that are pure PHP (drive the executor with
   `CheckedGrpcFuture::fromCallable`, no `Call`) stay in `Unit` ungated and
   also join the Grpc suite as `<file>` entries so the Grpc coverage run
   (PCOV) exercises the code paths they cover.
4. `php -n vendor/bin/phpunit --testsuite Unit --fail-on-skipped` is the
   faithful local simulation of CI's unit job (php -n = no extensions).
5. XML comment gotcha hit while documenting this: XML comments cannot
   contain `--` — the phrase `--testsuite` inside an XML comment breaks
   parsing with "Double hyphen within comment". Use "testsuite selector".

## Rector version drift: local rector ≠ CI rector (no committed composer.lock)

The repo has **no committed `composer.lock`**, so CI's `composer install`
resolves the latest rector within `^2.3` every run, while local devs use
whatever `vendor/` happens to hold (this repo historically pinned 2.4.3).
When rector 2.6.x shipped (2026-08), CI's Lint job started flagging 8 files
that local rector didn't, failing **every** PR — filed as #485, fixed in
PR #486. Lessons:

1. `composer rector` local green does NOT mean CI lint green. Reproduce CI
   with a scratch rector: `cd /tmp && composer require rector/rector:^2.6`
   then `/tmp/rector26/vendor/bin/rector process --dry-run`.
2. Rector 2.6's `ReadOnlyPropertyRector` + `RemoveUnusedPrivateMethodRector`
   chain can **delete `setUp()`** that initialises the now-readonly props,
   leaving them uninitialised → every test in the class errors "must not be
   accessed before initialization". And `PrivatizeFinalClassMethodRector`
   making `setUp()` private is a PHPUnit **fatal error** ("must be protected
   or weaker") — PHPUnit requires ≥ protected. Neither transform is safe for
   PHPUnit lifecycle methods: skip such files wholesale in `rector.php`
   (`->withSkip([__DIR__ . '/tests/…'])`) rather than adopt a broken
   transform.
3. The safe part of the 2.6 drift is `IfToNullCoalescingAssignRector`
   (`if (isset($x[$k])) { $x[$k] = init; }` → `$x[$k] ??= init;`) — apply it
   to src files freely; it is semantics-preserving.

Also: XML comments must not contain `--` (see above).

## PHPUnit attributes: docblock must IMMEDIATELY precede the declaration (issue #452)

Converting doc-comment metadata to PHPUnit 11 attributes (PR #490, issue #452)
hit one trap worth remembering:

1. PHPStan L9 associates a `/** @param ... */` docblock with the function only
   if it IMMEDIATELY precedes it. An attribute placed between the docblock and
   the function (`#[DataProvider(...)]` above the docblock) silently breaks the
   association → `missingType.iterableValue`. Canonical order is
   **docblock → attribute → function** (see tests/Unit/Grpc/GrpcResponseParserTest.php).
   The remaining docblock is still needed: attributes cannot express
   `@param array{...}` shapes for PHPStan.
2. Method-level `@covers \Foo::bar` maps to `#[CoversMethod(Foo::class, 'bar')]`
   (attribute exists in PHPUnit 11.5; argument order is className, methodName).
3. After conversion verify with `vendor/bin/phpunit --testsuite Unit
   --no-coverage --display-phpunit-deprecations` → must print
   `PHPUnit Deprecations: 0`; baseline comparison counts are Tests: 743,
   Warnings: 5 (pre-existing; the run still exits 1 because of them).

## Don't test generated proto setter behavior in unit tests (PR #498)

`PdClientGcSafePointTest::testUpdateServiceGcSafePointRejectsOutOfRangeUint64`
asserted that the generated `UpdateServiceGCSafePointResponse::setMinSafePoint()`
throws for an out-of-range uint64 string. That behavior depends on the
protobuf extension version: it threw locally but silently accepted the value
on CI (PHP 8.2–8.4) → unit tests failed only in CI. Rule: never assert on
gencode internals; test OUR code instead — here `PdClient::uint64ToInt()` was
strengthened (string→int round-trip check catches clamped out-of-range
values) and the test calls it via Reflection, which is deterministic across
platforms.

## Catching a RegionException around `RegionErrorHandler::check()` is safe because check() invalidates BEFORE throwing

`RegionErrorHandler::check()` (with `notLeaderOwnedByRetryExecutor: false`) invalidates the
region from the cache and THEN throws — so a call site that wraps `grpc->call()` +
`check()` in `try { … } catch (RegionException $e)` gets cache invalidation for free and
can simply retry: the next attempt's `RegionResolver::getRegionInfo()` sees a cache miss,
reaches PD, and picks up the new epoch/leader. The re-resolve must happen INSIDE the retry
loop (stale-capture class, #267/#500) — a region/peers pair hoisted above the loop would
replay the stale epoch on every attempt. Do not catch and ignore a NotLeader hint without
any invalidation: without `notLeaderOwnedByRetryExecutor: false` (or an owner like
`RetryExecutor::handleNotLeader()`) the stale entry would survive up to the ~600 s TTL.
Budget note (#500): retrying region errors inside `pessimisticLockBatch()`'s do-while
charges the existing backoff schedule to the lock-wait budget, so exhaustion surfaces as
the last RegionException — not `LockWaitTimeoutException` (no lock conflict was reported).

## Replica reads: selection must live inside the retry closure, and DataIsNotReady is an exclusion signal

Three rules from issue #421 (`RegionContextFactory::resolveTarget()`):

1. **Peer selection is per-attempt state, like region resolution.** The
   `ReplicaReadPolicy` selects a peer for every retry attempt inside the
   read closure (the same stale-capture lesson as #267); never hoist a
   resolved `ReplicaReadTarget` above the loop. The selected store id (not
   just the leader's) must also feed `resolveStoreAddress()`.
2. **`DataIsNotReady` is a replica-lag signal, not a region miss.** It is
   classified as `BackoffType::None` (immediate retry); the read closure
   catches the `RegionException`, remembers the failing store id in a
   by-ref local (`$excludedStore`) and the next attempt excludes it — with
   a single follower left this degrades to the leader. Sync paths
   (get/getKeyTTL/scan/txn reads) do this; the async batchGet dispatch
   cannot (errors surface at wait time outside the retry closure), so
   there a re-pick relies on the random choice among replicas.
3. **No new store-label cache is needed.** `StoreCache` already caches the
   full `metapb.Store` message *including* its labels, and
   `RegionResolver::getStore()` exposes it; the issue's "extend StoreCache
   to retain labels" premise had already expired (it retains the whole
   proto). `RegionContextFactory::resolveTarget()` takes a
   `Closure(int): ?Store` instead of the resolver, so it is unit-testable
   without mocking the final `RegionResolver`.

Review note (PR review of the #421 branch): a `$leaderFallback`/`forceLeader`
parameter was removed from `resolveTarget()` — it was never set to `true`;
the actual leader fallback is the exclusion path (`excludedStoreId` → empty
candidate list → degrade to leader inside `resolveTarget()`). If you ever add
a real leader fallback trigger, remember a leader that answers DataIsNotReady
under Leader+staleRead otherwise loops on itself at zero backoff until the
attempt cap. The async batchScan path likewise never tracked exclusions: it
declared `$excludedStore` with no `DataIsNotReady` catch inside its retry
closure, so the unused plumbing was removed there too.

## Re-grouping a key batch restores the retry budget — cap the regroups

`pessimisticLockBatch()` re-grouped its keys against the fresh region layout
after a split (issue #503), but each re-group also reset the per-group
`$elapsedMs` budget declared inside the group loop — a pathological repeated
split/region-error loop would retry forever. A cap (`PESSIMISTIC_LOCK_MAX_REGROUPS`,
10) now bounds the re-groups; exceeding it rethrows the last `RegionException`.
PHPStan L9 trap hit while adding the cap: a local `$regroupsUsed = 0` counter
incremented only inside the regroup branch is proven always-0 at the check
site (single-pass narrowing) → `identical.alwaysFalse` / `alwaysFalse`.
Workaround: collect regroups into an array and test
`count($regroups) >= CAP` (same escape hatch as the by-ref-flag lesson above).

## Channel arguments change ext-grpc's persistent-channel key (#265)

`Grpc\Channel` keys its shared-channel cache on the target + the options
array, so once `GrpcClient` started passing `grpc.*` channel arguments
(issue #265) two `GrpcClient` instances with different
`maxReceiveMessageBytes`/keepalive settings get *distinct* underlying
channels even for the same address. Consequence to remember: any future
per-instance channel option added to `createChannel()` must flow through
the same argument array (and its defaults must be deterministic, not
environment-dependent), otherwise instances that should share a channel
silently fork extra TCP connections. `GrpcClient::channelArgs()` is the
single seam — its output is the effective channel key material — and it is
unit-tested via reflection in the Unit suite (`GrpcClientChannelArgsTest`)
without ext-grpc, since no real channel is created.

## The issue's suggested fix can silently differ from what the test suite needs — reflection seams over protected methods

Issue #265 suggested asserting channel args "via a seam or a protected
factory method". `GrpcClient` is `final` and creates `Grpc\Channel`
directly, so a protected seam was not viable without opening the class;
the implemented seam is a `private channelArgs()` method inspected with
`\ReflectionMethod` from a pure-PHP Unit test (no ext-grpc needed —
`createChannel()` is never invoked). Same reflection style the existing
`GrpcClientTest` uses for private properties; keeping these tests out of
the `Grpc` suite means the arg wiring is covered by the no-extension CI job.

## Escaping a RetryExecutor retry loop for control flow: throw a non-TiKvException

The rollback regroup fix (#505) needed to break out of
`RetryExecutor::execute()` when the re-resolved region no longer covers the
key group — but `execute()` catches `TiKvException` and retries it, so a
region-style signal would be swallowed. The trick: throw an exception that
extends plain `\RuntimeException`, not `TiKvException` (`RollbackRegroupSignal`,
`src/Client/TxnKv/RollbackRegroupSignal.php`) — it escapes the executor and is
caught in `TwoPhaseCommitter`'s new `while ($pendingKeys !== [])` loop, which
re-groups this group's keys plus all not-yet-processed groups' keys. The
signal is a deliberate control-flow exception, marked `@internal`; keep it out
of the `TiKvException` hierarchy or the executor will retry it forever.

## TxnKV E2E can flake with a one-off empty scan on a fresh CI cluster

PR #506's CI failed once with `TxnKvE2ETest::testScanWithLimitOneReturnsSingleKey`
asserting `actual size 0` for keys committed moments earlier — no exception, just
an empty `KvScan` answer. The PR's replica-read changes are no-ops in leader mode
(`RegionContextFactory::resolveTarget()` returns the identical leader context), and
the suite passes repeatedly on a fresh local cluster (full suite ×3, targeted
test ×10). Re-running the failed CI job went green. Diagnosis: transient infra
flake, not a code regression. If it recurs, suspect the scan loop in
`TxnReader::executeScanForRegion()` — an empty batch with `$pending > 0` and no
split exits silently (see `while (true)` loop, `break` conditions); a one-off
empty answer for non-empty data is never retried.

E2E note: `docker-compose run --rm php-test …` recreates the tikv
containers it depends on; if the cluster was just (re)started the run
container's DNS can fail with "Name does not resolve tikv1:20160" and the
whole suite errors — wait ~20 s after `make up` before running E2E.

## `git log master..HEAD --oneline` is the definitive "what's in this PR" check

When asked whether a branch contains an unrelated commit, the commit existing
somewhere in the repo (`git show <sha>` works) proves nothing: e.g. during
review of fix/237-region-retry-deadline, commit 144f864 (pdEndpoints
validation, #51) looked suspicious but was already merged into master, so
`git log master..HEAD` contained only the one relevant commit. Always diff
against the merge base (`master...branch`), not against the repo's commit
graph.

## Service safe-point E2E tests must assert invariants, never exact values

PD registers its own `gc_worker` service safe point and advances the GC
safe point at a rate limited by `gc_life_time`, so exact-value assertions
in a `holdGcSafePoint → getGCSafePoint → release` round-trip flake against
a real cluster. Assert only invariants (learned in issue #499,
`tests/E2E/GcSafePointE2ETest.php`): (1) `updateServiceGCSafePoint()`
returns the min across all services, which can never exceed *our* held
point; (2) `getGCSafePoint()` (cluster GC safe point) is capped by the min
service safe point, so while held at a fresh TSO it must be ≤ that TSO;
(3) the GC safe point is monotonic — release must not move it backwards.
Also: wrap the release in `try`/`finally` — a leaked registration with a
600 s TTL keeps holding back GC on the test cluster after the run.

## The commit-point guard for rollback() must be on the status, not commitTs (TXN-10)

Issue #215's suggested fix guards `TwoPhaseCommitter::rollback()` on
`$state->getCommitTs() !== null`, but `commitTs` is set in `commit()` *before*
the primary region is committed — a failed primary commit leaves a transaction
that is not committed in TiKV and is legitimately still rollback-able. Guard on
`$state->getStatus() === TransactionStatus::Committed` instead; the status is
set to Committed right after the primary commit returns (the commit point) and
secondary commit failures are logged and swallowed there. Related expiry: the
#217 test (`testCommitRetryReusesCommitTimestampAfterSecondaryFailure`) was
written when a secondary commit failure threw out of `commit()` and left the
status Active so a second `commit()` could reuse the timestamp — that premise
expired with #215, and the test now asserts the failure is swallowed and
exactly one timestamp is minted.

## The issue's suggested classifier fix would silently fall back to the default classifier — return a decision or rethrow

The RetryExecutor's classifier chain is `handleNotLeader → classifier → ErrorClassifier`; a classifier that returns `null` is NOT a "do not retry" verdict — the executor falls through to the default `ErrorClassifier::classify()`, which retries every `GrpcException` as `TiKvRpc`. Issue #239's suggested snippet (`if ($e instanceof GrpcException) { return null; }`) would therefore have retried CAS exactly as before. To make an exception non-retriable from a custom classifier without changing `RetryExecutor`'s signature, **rethrow** it inside the classifier (the throw escapes the executor's catch and propagates with the original type/trace) — that is what `RawKvAtomic::compareAndSwap()` now does for `GrpcException` (#239). Side effect: the executor's fatal path (`'Fatal error, not retrying'` log) and its post-classification region invalidation + `closeChannel()` on `GrpcException` are skipped for the rethrown error; if a CAS transport error needs channel cleanup, that logic would have to move. If this pattern recurs, consider promoting it: a classifier result type with an explicit `Rethrow`/`Fatal` variant instead of the throw-from-classifier trick.

## A rebase can leave stray conflict markers in docs — grep before finishing

All three of fix/216, fix/237 and fix/238 carried a leftover
`their rebase (and #238 also in `docs/helpers/faq.md`), silently committed
by the follow-up fix. A squash/fixup during a rebase can resurrect markers
from an earlier conflict that was "resolved" by keeping both sides. Before
every push, grep the whole diff for conflict markers:
`git diff master...HEAD | grep -n ">>>>>>>\|<<<<<<<"`. PHPCS/PHPStan do
not catch these in Markdown files.

## RegionCache overlap removal on put() — touching ranges are not overlapping

`RegionCache::put()` (issue #238, REG-07) removes every cached entry whose
`[startKey, endKey)` range overlaps the incoming range before inserting, and
recomputes the insert position afterwards. Boundaries: an empty endKey means
unbounded, and ranges that merely *touch* (`endKey` of one equals `startKey`
of the other) do NOT overlap — TiKV regions are half-open `[start, end)`, so
a split `[a,m)` + `[m,z)` over an old `[a,z)` keeps both halves. Two entries
genuinely sharing a start key cannot coexist: the newest put wins (the
overlap removal also deletes the equal-startKey stale entry that the
insert-position/binary-search tie used to prefer). Caveat: the removal does
## A PdClientInterface mock auto-returns [] from scanRegions — grouping silently becomes empty

`RegionResolver::batchResolveRegions()` calls `$pdClient->scanRegions()` (typed
`array` return). A PHPUnit mock without an explicit stub auto-returns the type
default — `[]` — so `RegionGrouper::groupKeysByRegionBatch()` returns `[]`,
**no gRPC call is ever made**, and `commit()`/`rollback()` "succeed" doing
nothing. A test that mocks only `getByKey`/`getRegion`/`getStore` and asserts
`grpc->call()` behaviour can be silently vacuous: the existing
`TransactionTest::testCommitPessimisticWithKeys()` and
`testRollbackWithKeysCallsBatchRollback()` were passing without the grpc mock
ever being reached (discovered in issue #216). Always stub
`$pdClient->method('scanRegions')->willReturn([$region])` when a test must
actually reach the RPC layer. Related latent bug: when a real `scanRegions()`
returns nothing (or a key falls outside all returned regions),
`TwoPhaseCommitter::commit()` / `rollback()` complete "successfully" without
sending any RPC — no error is raised for keys that could not be grouped.

## Adding a `TiKvException` subclass means updating `docs/error-handling.md` in the same commit

The doc enumerates the exception tree with hard counts ("All fifteen
`TiKvException` subclasses", "the sixteen classes above"). Every new subclass
shifts the counts and must be added to the tree — issue #216 added
`UndeterminedCommitException` and initially left the counts stale. This is the
proactive corollary of the #394 lesson (counts derived from source, not issue
text): when *writing* a subclass, update the enumeration in the same commit.

## A rebase can leave stray conflict markers in docs — grep before finishing

All three of fix/216, fix/237 and fix/238 carried a leftover
`their rebase (and #238 also in `docs/helpers/faq.md`), silently committed
by the follow-up fix. A squash/fixup during a rebase can resurrect markers
from an earlier conflict that was "resolved" by keeping both sides. Before
every push, grep the whole diff for conflict markers:
`git diff master...HEAD | grep -n ">>>>>>>\|<<<<<<<"`. PHPCS/PHPStan do
not catch these in Markdown files.



## gRPC status codes, not exception type, decide GrpcException retries — and detail text must not leak into classification

`ErrorClassifier` used to return `BackoffType::TiKvRpc` for every
`GrpcException`, so a permanent status (`UNAUTHENTICATED` after a bad TLS
rollout, `CANCELLED`, `INVALID_ARGUMENT`) burned ~30 retries / ~30 s per
request — a self-inflicted ~30x load multiplier exactly when everything was
failing. Since issue #240 the `GrpcException::$grpcStatusCode` field drives
the decision via `ErrorClassifier::classifyGrpcStatus()`: UNAVAILABLE,
ABORTED, INTERNAL, UNKNOWN, DEADLINE_EXCEEDED → TiKvRpc;
RESOURCE_EXHAUSTED → ServerBusy (its own budget); everything else
(including unknown codes, fail-closed) → null. Two implementation lessons:
(1) place the GrpcException branch BEFORE the message-text fallback in
`classify()` — gRPC `details` strings are free text and must never be able
to match `str_contains($message, 'NotLeader')`-style rules;
(2) the codes are wrapped in the int-backed enum
`GrpcStatusCode` (src/Client/Grpc/) declared locally, because the
`\Grpc\STATUS_*` constants only exist with the extension loaded and unit
tests run with `php -n` (no ext-grpc). Deadlines: DEADLINE_EXCEEDED stays
retryable because the outcome is indeterminate; the idempotency split is
REG-08's job, not the classifier's.


## Jittered backoff makes "exact retry count" budget tests flaky — assert ranges with a shared-budget lower bound

When all `BackoffType`s became jittered (issue #242, equal jitter `[expo/2, expo]`), unit
tests that asserted an **exact** retry/call count against a `maxBackoffMs` budget broke:
randomly halved sleeps can squeeze one extra attempt into the same budget
(`tests/Unit/RawKv/RetryBudgetSharedAcrossRegionsTest.php` went from always 6 calls to
6–8). Fix pattern: assert the invariant the test actually cares about — the *lower* bound
that distinguishes per-region from shared budgets (`>= 6`) plus a derived upper bound
(`<= 8`) — not the exact count. Anything comparing budget-exhaustion attempt counts must
now tolerate ±1 attempt per jittered exponential step. Also: `1 << $attempt` instead of
`2 ** $attempt` keeps the exponent math int-typed so Rector's `RecastingRemovalRector`
doesn't strip a needed `(int)` cast before `intdiv()`.

## Budget carry-over detection becomes probabilistic under jittered backoff

Follow-up to the #242 jitter entry: once every `BackoffType` is jittered, a
test that detects retry-budget carry-over between `execute()` calls by making
the second call cross a tight budget can no longer be made deterministic. The
budget must be ≥ the first call's *worst-case* spend (else the first call
fails) but < best-case spend + the second call's minimum retry (else the bug
is undetectable) — with equal jitter the best/worst-case gap is 2x, so these
constraints are unsatisfiable (`tests/Unit/Retry/RetryExecutorTest.php`
`testReusedExecutorRetriesNormallyOnSecondCallAfterFirstConsumesBudget`,
comment documents the math). Accept probabilistic detection: the
no-carry-over invariant ("second call succeeds") still holds deterministically.

## Issue duplication: a fix's refactor can silently close an older issue — grep the source before coding

Issue #243 (REG-12) reported per-executor retry-budget fields surviving across
`execute()` calls, exhausting a Transaction's budget permanently. The code fix
had already shipped in ee4c0f6 (issue #271), which moved `totalBackoffMs` /
`serverBusyBackoffMs` / `attempt` from instance fields to locals inside
`execute()` (also for reentrancy) — #243's remaining value was only the
missing regression tests (executor-level sequential-call budget reset, plus
two sequential `Transaction::get()` calls each getting a usable budget, per
the acceptance criteria). Lesson: read `git log <file>` for the issue's cited
lines before implementing; an audit finding can be a duplicate of a later
refactor that fixed it as a side effect. Also: when writing budget-reset
tests, remember `maxBackoffMs` is an *upper bound on the accumulated total* —
with RegionMiss backoff 2,4,8 a budget of N allows exactly ⌊N/2⌋-ish retries,
so compute the attempt count from the exponential sequence (2+4=6 ≤ 6 allows
two retries; 6+8=14 > 6 aborts the third), and an operation that "fails twice
then succeeds" needs a per-call failure counter (a shared `$calls % 3`
pattern silently gives the second execute() zero retries).

## Equal-jitter randomness can collapse the separating boundary in budget assertions — assert `<=` over more iterations, never strict `<`

`Backoff::exponential()` with equal jitter returns `half + random_int(0, half)`
(inclusive), so a ServerBusy sleep is `1000–2000 ms` **inclusive of both ends**.
When a test distinguishes "fresh budget" (one sleep: 1000–2000) from
"carried-over budget" (sum of N sleeps: 2000–4000) via a threshold, the ranges
touch at exactly 2000 ms — a strict `<2000` assertion flakes whenever all
jitter draws are maximal (~1/1001 per draw, `random_int` is inclusive and not
mockable; `RetryExecutor` is `final readonly`, so it can't be subclassed
either). The robust pattern (used in `RetryExecutorTest::
testServerBusyBudgetResetsPerExecuteCall` for issue #243): assert
`assertLessThanOrEqual(2000)` across **three** sequential `execute()` calls —
a fresh budget can never exceed 2000 (zero false failures), while a
carried-over budget exceeds it unless *every* draw is maximal (~1e-9 false
passes, vanishing with more iterations). General rule: with inclusive random
ranges, prefer `<=` at the shared boundary plus more samples over a strict
inequality that can reject a legal value. Bonus: these tests never actually
sleep, because `RetryExecutor` checks the budget **before** `usleep()`
(issue #237) — an over-budget ServerBusy error throws without sleeping, so
budget-exhaustion tests are fast.

## `ingest()` silently drops keys that cannot be resolved to a region

`SstIngestor::groupPairsByRegion()` skips pairs whose `batchResolveRegions()`
entry is `null` (`continue`, no error) — an `ingest()` run can "succeed" while
never writing some keys. Verify the imported key count when completeness
matters; the same no-throw-on-unresolvable-keys class of bug exists in
`TwoPhaseCommitter` (issue #216). Separately, `ingest()` is not retried at all:
any region/transport error aborts the remaining regions (the `finally` still
switches all stores back to normal mode), and a killed process leaves the
cluster in import mode — see the [DOC-24] section in `docs/operations.md`.

## Doc enum/const lists drift — regenerate them from code, never hand-write

Hand-written doc lists of enum cases drift: the docs described invented
`BackoffType::Fast/Medium/Slow` cases while the real enum has fourteen
different cases (issue #375). When documenting an enum or constant table,
copy the cases and values straight from the source file (`baseMs()` /
`capMs()` values) and link to the file as the source of truth, rather than
paraphrasing. Related trap: `docs/configuration.md`'s custom-classifier
example still returns a non-existent `BackoffType::Custom` case — doc
examples that subclass and override must match the real class signatures
(`classifyError()` lives on `ErrorClassifier`, not `RawKvClient`).

## PHPUnit mock `willReturnCallback` closures on PHP 8.5 must use explicit `use` — implicit capture yields null

A callback passed to `willReturnCallback()` that relies on implicit
(auto-bound) variable capture can read captured variables as `null` on
PHP 8.5 with PHPUnit 11.5.56: `match ($method) { 'KvPrewrite' => $resp, … }`
silently returned `null` and surfaced as a baffling
"Return value must be of type object, null returned" TypeError pointing into
`ReturnCallback.php`, with the real value (`$resp`) non-null at the call
site. Explicit `use ($resp)` (by value) or `use (&$queue, &$index)` (by
reference) behaves correctly. Every existing test in this repo already
passes captured variables explicitly — keep that style; found while writing
the #419 one-phase/async-commit tests (see `OnePhaseAsyncCommitTest`).

## A constant named "capacity" used as a shift amount shifts away the whole timestamp — name bit counts explicitly

Adding the #420 batch TSO handout, `($physical << self::LOGICAL_CAPACITY)` with
`LOGICAL_CAPACITY = 1 << 18` shifted by **262144** bits instead of 18, silently
zeroing the physical part (`1715000000000 << 262144 === 0`) — and the whole
existing composed-timestamp test suite failed at once, which is the only reason
it was caught. Rule: constants feeding `<<`/`>>` must be named `*_SHIFT` (bit
count) or `*_CAPACITY` (value used with `&`/`+`), never interchangeably; when a
constant is used as a shift amount, prefer an arithmetic identity the tests
can check (`base = physical * capacity + logical` would have been shift-free).
A failed-by-zero composition looks like "PD returned physical=0" — hard to
attribute to the constant, easy to blame the mock.

## TSO batching / low-res cache semantics (#420)

Facts fixed by the implementation, worth not re-deriving:

- A `TsoResponse` grant of $count timestamps hands out as consecutive
  integers `base - ($count - 1) … base`, **not** `base … base + $count - 1`:
  PD's response timestamp is the *highest* of the granted range
  (client-rust's `allocate_timestamps`), so the oracle walks backwards by
  `$count - 1`. Plain integer arithmetic wraps the 18-bit logical counter
  into the next physical millisecond correctly (no manual wrap logic
  needed). PD may grant *fewer* than requested — never hand out beyond
  the grant (`TsoResponse.count`), and a missing count (0) is treated as
  a single-timestamp grant with a warning.
- `getLowResolutionTimestamp()` must treat a **negative clock age** as
  stale: a backwards clock jump otherwise serves a timestamp that is
  already older than the bound (the `ageMs >= 0` guard). `0` staleness
  only ever reuses a timestamp fetched in the same millisecond.
- `TimestampOracle` dropped `final readonly` → `final` because the
  low-res cache lazily mutates state; readonly properties cannot be
  reassigned after construction. Cache-carrying services can't be
  `readonly` classes.

## BatchPartialFailureException's array keys are sub-batch indices, not region IDs or user keys

`BatchPartialFailureException::getRegionErrors()` is documented in several
places as "regionId → exception", but the keys are actually indices into the
internal list of sub-batch callables (`RawKvBatch` groups keys per region and
splits each region into ≤512-key/16 KB sub-batches before dispatch, so the
count of units is larger than the region count whenever any region's keys
exceed one sub-batch). There is no public
mapping from a failed index back to the user keys it contained. Practical
consequence for recovery advice: you cannot "re-drive only the affected keys"
through the exception alone — for idempotent work retry the whole batch; for
non-idempotent work re-read and recompute, or use a transaction. (Documented
in docs/operations.md, issue #393; `docs/troubleshooting.md`'s "regionId →
exception" wording is a candidate for the same correction.)

## Capturing gRPC requests in a helper-scoped PHPUnit mock callback — closure must take `$requests` by reference

When a test helper builds the mocked `GrpcClientInterface` and returns the
captured requests array, two PHP semantics bite: destructuring `[$txn,
$requests] = helper()` copies the array at destructure time (before the RPCs
happen, so the copy is empty), and arrow functions (`fn () => $requests`)
capture by value. Return `[$txn, static function () use (&$requests): array {
return $requests; }]` and call the getter after the operation
(used in `CommitPrewriteKeyCoverageTest`, issue #329).

## Tests pinning a not-yet-merged fail-closed contract — guard with a behavior probe, not an env flag

When an audit issue's tests assert behavior another (open) PR implements
(e.g. #329's tests assert the fail-closed contract of unmerged #244/PR #531),
CI must stay green without weakening the pin: probe the contract at runtime
(call the resolver with an unresolvable key; if it doesn't throw, the fix is
absent) and `trigger_error(..., E_USER_WARNING)` with a message naming the blocking PR (NOT `markTestSkipped()`: the `unit-tests` CI job runs with `--fail-on-skipped`, so a skip breaks the build; PHPUnit 11 has no add-warning API, and an `E_USER_WARNING` during a test is recorded as a non-failing warning that still names the pending contract). The
tests then fail against master if the guard is removed (verify once by
temporarily replacing the warning-early-return with `self::fail()`) and activate
automatically when the fix merges — no env var to clean up later, and no
duplicate tests alongside the fix PR's own coverage.

## RawKvBatch hardcodes `new Call(...)` — region errors inside batch responses can only be tested at the wait-boundary composition

`RawKvBatch::execute*ForRegionAsync()` constructs `\Grpc\Call` directly, so
the transport cannot be injected with a mocked Call (which `GrpcFutureTest`
does for the future itself). Consequently a `NotLeader`/`EpochNotMatch`
returned inside a `RawBatchGetResponse` cannot be delivered end-to-end
through `batchGet()` in unit tests. The pinned tests for issue #330
(`RawKvBatchTest`) instead drive the **exact composition RawKvBatch builds**
— `CheckedGrpcFuture::fromGrpcFuture(new GrpcFuture($mockCall, ...))` (fast
path) or `fromCallable()` wrapping `RegionErrorHandler::check()` (multi-region
waiter) — through `BatchAsyncExecutor::executeParallel()`, which is where the
error is actually classified and reported. Split-limit tests count RPCs via
the `GrpcClientInterface::getChannel` mock against a dead `127.0.0.1:1`
channel and expect `BatchPartialFailureException` (issue #330).

## Several legacy RawKvBatchTest TTL tests silently dispatch nothing

`testBatchPutWithIntTtl` and friends mock `getByKey` but **not**
`scanRegions`, so `batchResolveRegions()` returns an empty map, every key is
silently skipped, `regionCalls` is empty, `executeParallel([])` returns
immediately and the test passes without a single RPC. If you need such a
test to actually dispatch, also mock
`$this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()])`
and `$this->pdClient->method('getStore')->willReturn($this->defaultStore())`
(missing getStore makes dispatch fail early with "Store 1 not found in PD"
before `getChannel()` is reached).

## Unit tests can silently pass because batch grouping dropped every key — check for a missing scanRegions mock

Before #244, `batchResolveRegions()`/`RegionGrouper` silently skipped keys with no region, so a
unit test for a pessimistic commit/rollback/batch path could pass while **every** key was dropped
before any RPC was sent — the mock gRPC client was never called and no assertion noticed. Four
`TransactionTest` tests were in exactly that state: they never mocked
`PdClientInterface::scanRegions()`, and the "success" they asserted came from the silent-drop
path. Now that the grouper fails closed (#244), a missing `scanRegions` mock surfaces as
`TiKvException: PD could not resolve the region for key ...` — fix the test by mocking
`scanRegions` to return regions covering all batch keys, not by weakening the contract. When
writing a new batch-path test, assert the gRPC mock actually received the request (call-count
expectation), otherwise silent key drops remain invisible.

## PD `scanRegions(start, end)` is half-open — the maximum batch key needs an appended `\x00`

`PdClientInterface::scanRegions($start, $end)` stops before the first region whose `startKey >= $end`.
Passing a batch's maximum key verbatim as `$end` therefore excludes the region that begins exactly
at that key, and `findRegionForKey()` returns null for it — the classic boundary off-by-one fixed in
#244 by using `$maxKey . "\x00"` as the upper bound (smallest string strictly greater than `$maxKey`).
Any new caller of `scanRegions()` used for batch resolution must remember this; client-go resolves the
last key inclusively for the same reason. Note `TxnReader::scan()` is different: there `$endKey` is the
scan range's exclusive end by definition, and `RegionRangeClipper` handles the clipping.
## Byte order for TiKV key ordering — never PHP's default `sort()` or loose `<`/`>=`

TiKV orders keys bytewise, but PHP's default `sort()` (`SORT_REGULAR`) compares
numeric strings numerically (`sort(["b","a","10","9"])` → `["9","10","a","b"]`)
and the loose `<` / `>=` operators do the same. Any place the client orders,
filters or clips key ranges must use `strcmp()` / `sort($keys, SORT_STRING)`
(issue #331 fixed `TxnReader::finalizeScanResults()`: the merged scan output
sorted with plain `sort()` and the write-set in-range filter used `>=` / `<`;
both silently broke `scan()` pagination on the last returned key for numeric
keys). Existing ordering helpers already follow this (`RegionRangeClipper`
uses `strcmp()`). Watch for the inverse bug when writing tests: a scan *range*
that is only valid numerically (`['9', '11')`) is invalid bytewise — `9` >
`11` — and `RegionRangeClipper` drops it, returning nothing; use ranges like
`['2', '3')` with key `19` to exercise "in range numerically, out bytewise".
Note also that `Transaction::__destruct()` fires a `KvBatchRollback` via the
gRPC mock when a test leaves a non-empty write set — `expects($this->once())`
on `call()` then fails with "called 2 times"; use `method()` without an
invocation count or expect one extra rollback call.
## Stubbing rollback tests: batchResolveRegions() goes through PD scanRegions, not the region cache

When a `TransactionTest` mock sets up `regionCache->getByKey()` but NOT
`pdClient->scanRegions()`, `TwoPhaseCommitter::batchRollback()` /
`pessimisticRollbackAll()` silently group the write-set keys into **zero**
regions: `RegionResolver::batchResolveRegions()` calls
`pdClient->scanRegions($minKey, $maxKey)` (a PHPUnit array-typed mock
returns `[]`) and skips unresolved keys, so `RetryExecutor::execute()` never
runs, no `KvBatchRollback`/`KVPessimisticRollback` RPC is issued, and
`rollback()` returns `RolledBack` having done nothing — the test "passes"
its status assertion and fails only on the RPC-count assertion (issue
#333 debugging). Any test that drives rollback/commit grouping must stub
`pdClient->scanRegions()` to return the test region (plus
`regionCache->put()`), not only `getByKey()`. The same silent-drop
behaviour is the subject of issue #244/#329 for writes; for mocks it just
means: stub scanRegions.

Also: `LockResolver::resolveLock()` on a `CheckTxnStatusResponse` with
`commitVersion=0` runs CheckTxnStatus **twice** (it re-checks after the
lock-TTL wait, even with `lockTtl=0` and no actual sleep) before issuing
`KvResolveLock` — assert the full method sequence, not a single
CheckTxnStatus.
## Bounding a fan-out: reuse executeParallelCapped() before building a new windowed executor

Issue #264 asked for a windowed dispatch loop inside `BatchAsyncExecutor`, but the
`executeParallelCapped()` windowed executor already existed since #295 — the actual gap was
that `RawKvBatch` (batchGet/batchPut/batchDelete) still called the unbounded
`executeParallel()` and was never wired to `RawKvClient`'s `options['maxConcurrency']`
(the #295 option covered only range ops/SstIngestor). Lesson: before implementing an
executor-level mechanism from an audit issue, grep for existing capped paths — wiring a
stray call site to the existing knob is the whole fix. When an audit issue also asks for
"reject batches above a documented key-count maximum", that is a separate, breaking API
decision (needs a chosen cap + semver note) — do not silently fold it into a concurrency fix.

Testing the cap through `RawKvBatch` (Grpc suite only — the wire path constructs real
`\Grpc\Call` objects from `GrpcClientInterface::getChannel()`, so no ext-grpc = no test):
mock `getChannel()` to count concurrent dispatches and return a real channel to a dead
endpoint (`127.0.0.1:1`); the sends succeed, the waits fail, and the test asserts the
peak in-flight count equals the window size while expecting
`BatchPartialFailureException` (which also proves partial-failure semantics survive
windowing). See `tests/Unit/RawKv/RawKvBatchConcurrencyCapTest.php`.
## Rector's PrivatizeFinalClassMethodRector also demotes private static test helpers to instance methods

While adding a private `static` helper to `ConnectionFactoryTest` (#304),
rector's dry run demanded `private function` (non-static) and
`$this->helper()` call sites — `PrivatizeFinalClassMethodRector` treats
private static methods in final classes as removable indirection. Match the
existing test-helper style (instance methods) instead of fighting rector.
Also remember `composer lint` = phpcs + phpstan + rector dry-run: a lint
failure in a test file can come from rector, not phpcs/phpstan.

## Pooling TSO timestamps: keep the low-res path off the pool, and bound the pool by our own clock (#292)

Issue #292 added a per-client timestamp pool to `TimestampOracle::getTimestamp()`
(one `Tso` request with `count = poolSize`, default 64, served consecutively).
Design points that are easy to get wrong on a later refactor:

- **Separate the raw request from the pool.** `getTimestampBatch()` (public)
  and `getLowResolutionTimestamp()` must not share the pooled stream: an
  explicit batch advances PD *beyond* the pooled range, so a later pooled
  value could be lower than one already returned (ordering break), and the
  low-res contract is a *fresh* fetch. The implementation therefore has a
  private pool-unaware `requestTimestampRange()`; `getTimestampBatch()`
  resets the pool then calls it, `getLowResolutionTimestamp()` calls it
  directly (so its existing "fresh fetch" tests still pass), and only
  `refillPool()` stores into the pool.
- **Bound the pool by the injected wall clock, not by the TSO physical.**
  A `now - poolFetchedAtMs > poolMaxAgeMs` (default 5 ms) check discards a
  pool that outlived its physical window; a *negative* age (clock jump
  backwards) is treated as uncertain and refills. Comparing the pooled
  `physical` against local `microtime()` instead would make pooling
  unusable under PD/local clock skew (PD is routinely ahead), so the age
  is measured locally from fetch time.
- **Fork guard = PID.** PHP-FPM is process-per-request, but a `pcntl_fork`
  child would otherwise inherit the pool and hand out timestamps the parent
  already used; the pool records `getmypid()` and is dropped when it changes.
- **Monotonic across refills is PD's job, not ours.** Each refill asks PD
  for a fresh grant at the front of the physical window; PD never
  re-allocates a value, so the new range starts above the last handed-out
  timestamp. Do not "fix" the sequence locally (e.g. `max(ts, last+1)`) —
  that would fabricate a timestamp outside PD's grant, exactly what the
  fail-closed rule forbids.

Test seam: `TimestampOracle`'s constructor takes optional `$poolSize`,
`$poolMaxAgeMs`, `$clock` and `$pid` closures; unit tests inject a mutable
`$pid`/`$clock` to prove the pool is discarded on fork/age change without
`pcntl_fork` or real sleeping. The `TransactionTest` spy for the `#292`
acceptance criterion counts `PdClientInterface::getTimestamp()` calls (not
`Tso` RPCs) so it pins the caller contract independently of the pool.

## Adding a method to `RegionCacheInterface` is how you keep old scan tests passing (#293)

The #293 fix makes `RawKvScanner` resolve a scan's region chain from the
region cache instead of calling `pdClient->scanRegions()` per page. The
tempting implementation — walk the chain with the existing
`RegionCacheInterface::getByKey()` — breaks the retry tests in
`RawKvScannerTest`, which pin exact `getByKey` call sequences with
`willReturnOnConsecutiveCalls($pre, $pre, null, null)`: an extra cache lookup
during enumeration shifts the sequence and the test fails. Adding
`getRegionsInRange(string $startKey, string $endKey): array` to the
interface solves both problems at once: PHPUnit's auto-generated mock
returns `[]` for an array-returning method, so every existing test's
`resolveScanRegions()` takes the PD fallback and behaves exactly as before,
while a real `RegionCache` (used by the new spy test) serves the chain
locally. The trade-off is a BC break for third-party cache implementations
(pre-1.0, noted in the CHANGELOG); the alternative was an
`instanceof`-checked sub-interface, rejected as more machinery for the same
effect.

`RegionCache::getRegionsInRange()` must return `[]` — not a partial prefix —
the moment the chain does not reach `$endKey` (gap, expired entry, or a
non-advancing end key), otherwise a caller would silently scan only part of
the requested range. Consequently the *first* page of a cold scan still
costs one `ScanRegions` (which caches every region in the range), and only
later pages are free. The unbounded fan-out (`scan(..., limit: 0)` over
multiple regions) routes through `BatchAsyncExecutor` with
`sendSubRangeScanAsync()`; that path does **not** run
`RegionErrorHandler::check()` (the send is awaited outside the per-region
retry loop), so the implementation inspects `getRegionError()` and falls
back to the sequential retrying loop on any error — reads are idempotent, so
discarding the partial page is safe.

## `executeScanForRegion()`'s `$limit <= 0` sentinel means "unbounded" — continuation callers must pass the real budget (#293)

`RawKvScanner::executeScanForRegion()` only calls
`RawScanRequest::setLimit()` when `$pending > 0`; a `0`/negative `$limit`
therefore omits the field and TiKV runs an *unbounded* scan of the whole
region. The fan-out's split-after-enumeration continuation used to call it
with `0`, so a bounded page could read the entire remainder over the wire
before `array_slice()` trimmed it. Any internal caller that already knows
the remaining page budget must pass it (and skip the call entirely when it
is `<= 0`); reserve `0` for a genuinely unbounded request. The same applies
to the batchScan waiter, which passes `$remaining` into the region-error
fallback.

## PHP arrow functions capture by value once — a `fn() => array_shift($seq)` stub replays the first element

An arrow function (`fn () => ...`) captures surrounding variables **by
value at definition time**, and mutations to those captured copies do not
persist between invocations. A sequence-mock such as
`$grpc->method('callAsync')->willReturnCallback(fn () => $this->okFuture(array_shift($responses)))`
therefore shifts the *same* original array on every call and returns the
first response every time — the test silently mis-modelled the sequence
(observed as `['k1','k1']` instead of `['k1','k2']` in the #293 continuation
test). Use a full closure with `use (&$responses)` for per-call mutable
state, exactly like the pre-existing fan-out tests. (PHPStan then types
`array_shift()` as nullable, so keep a `?? new RawScanResponse()` default.)

## The unbounded scan fan-out is windowed, and the batchScan sub-response region error is now guarded (#293)

`scanSegmentsInParallel()` processes regions in windows of at most
`options['maxConcurrency']` (default 16), dispatching each window with the
*remaining* page budget, trimming in region order, and stopping as soon as
the budget is spent. This bounds peak memory at `maxConcurrency × limit`
rows instead of the region count × limit; the previous implementation built
one callable per region (each with the full limit) and only trimmed after
`BatchAsyncExecutor` had awaited every window. Any non-final region that
shrinks after enumeration now falls back to the sequential retrying path
instead of dropping `[freshEnd, segmentEnd)`, and the `batchScan()` waiter
inspects `getRegionError()` on each sub-response and re-runs that sub-range
through `executeScanForRegion()` (the concurrent send is awaited outside the
per-range retry loop, so a region error there is neither retried nor
invalidated otherwise).

## TxnKV region lookups must be MCE-encoded (GAP-01)

Validated empirically against TiKV v8.5.5 / PD v8.5.5 (V1 txn cluster) while
implementing issue #415:

1. **PD reports transactional region boundaries in an encoded form.** TiKV /
   PD store txn region boundaries using TiDB's `codec.EncodeBytes` (the
   memory-comparable encoding, MCE). The encoding walks the key in 8-byte
   groups: each full group is followed by marker `0xFF`; the final partial
   group is padded with `0x00` to 8 bytes and followed by marker
   `0xFF - padCount`, where `padCount = 8 - remaining`. The loop runs for
   `idx = 0; idx <= len; idx += 8`, so a key whose length is an exact multiple
   of 8 also gets a trailing all-zero group. Examples:
   `EncodeBytes("") = 0000000000000000f7`,
   `EncodeBytes("\x01\x00") = 0100000000000000f9`,
   `EncodeBytes("mrr") = 6d72720000000000fa`,
   `EncodeBytes("hello") = 68656c6c6f000000fc`.
   Querying `GetRegion(enc(user_key))` with the MCE encoding separates keys
   correctly across a split, while raw user keys misroute: the A/B test wrote
   binary + boundary keys across three regions — the MCE client
   committed/read everything, the raw client failed with
   `Key … is out of [region …]` (KeyNotInRegion-class).
2. **The first implementation used the wrong encoding.** It escaped `0x00` /
   `0xFF` and used a `0x00 0x00` terminator, which is not TiDB's
   `EncodeBytes`. Its `decode` was also lossy: it stopped at the first
   `0x00 0x00` pair inside the MCE bytes, so `0100000000000000f9` decoded to
   `\x01` instead of the true split key `\x01\x00`. That broke multi-region
   `scan()`: TiKV validates the range start against the region's lower bound
   and rejects `\x01` (`InvalidReqRange`, one byte below the bound), while the
   true key `\x01\x00` is accepted. `MemComparableCodec` now implements TiDB
   `EncodeBytes` / `DecodeBytes` faithfully, so `decode` is lossless and a
   decoded boundary is a valid txn scan start key. The GAP-01 E2E test
   (`TxnKvE2ETest::testTxnWorkloadAcrossPreSplitRegions`) drives writes, reads,
   `batchGet()` **and a forward `scan()`** across the split regions. (TxnKV
   exposes no reverse-scan API — only RawKV does — so a forward scan is the
   strongest cross-region scan coverage available for the transactional path.)
3. Manual region pre-split for E2E is reliable on v8.5.5 via the raw gRPC
   call `tikvpb.Tikv/SplitRegion` (service string `tikvpb.Tikv`, method
   `SplitRegion`) with a `kvrpcpb.SplitRegionRequest` carrying
   `region_context` (region id + epoch + leader peer), a raw `split_key`
   (`is_raw_kv` defaults to false) and the store address from
   `PdClient::getStore()`. A fresh (empty-volume) V1 cluster reports 5
   pre-created namespace regions (raw keyspace 0 / 1, txn keyspace 0 / 1),
   so "region count" must be counted over the workload key range
   (`scanRegions("\x00", "\x72")`), not over the whole keyspace. Region
   discovery and `SplitRegion` targeting go through the same `Mode::Txn`
   codec as the production client; the `split_key` stays a raw user key.

## Prewrite `KeyError` handling must be exhaustive and fail-closed (TXN-09, issue #214)

`KeyError` is a flat protobuf message whose variant fields are independently
nullable (no `oneof` is involved); TiKV sets at most one variant field per
response. The optional `debug_info` field (100) is a `kvrpcpb.DebugInfo`
message that may accompany it. `getX() !== null` / `getX() !== ''` is how the
variant is identified. (In `kvrpcpb.proto` the only `oneof` is
`CompactError.error`; `CheckTxnStatusResponse.error` is a plain `KeyError`
field.) The
original `TwoPhaseCommitter::handlePrewriteErrors()` checked only `locked`,
`conflict`, `retryable` and `abort`, so a `deadlock` (reachable on pessimistic
prewrite) or `primary_mismatch`/`txn_not_found` payload fell off the end of the
loop and prewrite was treated as successful — `commit()` then ran `KvCommit`
against keys that held no lock. The handler now checks `deadlock` first (→
`DeadlockException` carrying key/hash/lockTs, mirroring the pessimistic-lock
path), maps `already_exist`/`assertion_failed`/`primary_mismatch`/
`txn_not_found`/`commit_ts_too_large` to `TransactionConflictException`, and
**ends each iteration with an unconditional `throw new TiKvException(...)`** —
so a future/unknown variant can never be treated as success. Reuse
`KeyErrorDescriber::describe()` for the fallback message, not
`serializeToJsonString()`: the describer names the variant without emitting the
raw lock key (redaction convention). Variants deliberately left to the
fail-closed fallback today: `commit_ts_expired`, `txn_lock_not_found`. Add a
regression test in `TransactionTest` that records the gRPC method sequence and
asserts `['KvPrewrite']` — no `KvCommit` — for both the deadlock and the
unrecognised-variant cases.

## Raw user keys must not appear in exception messages either — redact and expose via a typed accessor (GRPC-10)

Exception messages are a logging channel: Monolog/Sentry/Bugsnag index
`$e->getMessage()`, uncaught-exception handlers write it to stderr, and
framework error pages render it. So the same rule that already applied to
`LoggerInterface` contexts applies to every thrown message. Issue #269 found
four sites that redacted for the log but interpolated the raw key into the
exception thrown immediately afterwards (`RetryExecutor`'s attempt-cap and
deadline messages, `RegionErrorHandler`'s two per-pair messages). Always pass
keys through `KeyRedactor::redact()` in messages, and when code genuinely
needs the raw key, add a typed accessor on the exception instead of embedding
it in the message (`RetryBudgetExhaustedException::getRawKey()`) — callers
must never parse keys out of `getMessage()`. A guard test
(`tests/Unit/Security/ExceptionMessageRedactionGuardTest.php`) tokenises
`src/Client` and fails any `sprintf()` call whose format string carries a
key-bearing message (`for key`) but whose arguments lack a
`KeyRedactor::redact()` invocation, so the pattern cannot silently return.

## `KeyRedactor::redact()` output already contains quotes — pass it to an unquoted `%s`, and anchor key-redaction guards to the `sprintf()` argument list

Two lessons from the #269 (GRPC-10) review:

1. **Doubled quotes in exception messages.** `KeyRedactor::redact()` already
   starts with `"` and, for short keys (up to 8 bytes), also ends with `"`:
   `"6b65795f61" (5 bytes)` vs `"757365725f656d61... (28 bytes)`. A template that also
   quotes the placeholder (`for key "%s"`) renders
   `... for key ""757365725f656d61... (28 bytes)"`. Pass the redacted value to an
   *unquoted* `%s` (`for key %s`). Do not change `KeyRedactor`'s own output —
   log contexts store the value unwrapped and depend on the current shape.
2. **Guard tests must scan the `sprintf()` argument list, not raw text.** The
   first cut matched the raw form with a text regex keyed off a positional
   bare `$key`; it stayed green only because the raw key was passed as the
   named `rawKey: $key` argument, and a legal refactor to positional
   constructor arguments would have made it fail on the *correctly redacted*
   code. The final guard tokenises the file, locates each `sprintf()` call,
   and requires a `KeyRedactor::redact()` token inside that call, which is
   immune to quoting and to unrelated `$key` arguments on adjacent
   constructors. Any future source-scanning guard should follow the same
   "inspect the call, not the file text" rule.
