# FAQ — Recurring Pitfalls

Frequently asked questions and recurring pitfalls in crazy-goat/tikv-php.
Ordered roughly by how often they bite.

## E2E tests need a running TiKV cluster (Docker)

The `E2E-RawKV` and `E2E-TxnKV` testsuites require real TiKV nodes. Start
the cluster with `make up` (PD on 2379, tikv1/2/3 on 20160/20161/20162),
stop with `make down`. If state gets corrupted: `make clean && make up`.

## Unit tests don't need TiKV — gRPC tests do

- `composer test:unit` (`--testsuite Unit`) mocks gRPC calls — fast, no
  cluster, no `grpc` extension needed.
- `composer test:grpc` (`--testsuite Grpc`) exercises real gRPC connections
  and requires the `grpc` PHP extension; runs with `--fail-on-skipped`, so a
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
(deleteRange/checksum) as of this fix.

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
