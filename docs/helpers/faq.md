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
