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
with `array_keys()` from such maps must be normalized with `array_map(strval(...))`
before validation. A unit test that builds the map with a *literal* key also
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
