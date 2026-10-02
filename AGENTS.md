# AGENTS.md

Project commands and specifics for tikv-php, a PHP client for TiKV (RawKV and TxnKV) over gRPC.
The development process (issue, worktree, review, PR, merge) is in
[docs/workflow.md](docs/workflow.md), the release process in
[docs/release-workflow.md](docs/release-workflow.md). The default branch is `master`.

Everything is written in English (code, comments, docs, commits, issues).

## Layout

| Path | Content |
|---|---|
| `src/Client/` | Library, namespace `CrazyGoat\TiKV\Client\` (`RawKv`, `TxnKv`, `Region`, `Cache`, `Retry`, `Connection`, `Grpc`, `Batch`, `Codec`, `Tls`, `Observability`, `Exception`, `Util`) |
| `src/Proto/` | Generated protobuf and gRPC classes (namespace `CrazyGoat\Proto\`); excluded from lint |
| `proto/` | Proto sources (git submodule, see `.gitmodules`) |
| `tests/Unit/` | Unit suites `Unit` and `Grpc` (`Grpc` needs `ext-grpc`) |
| `tests/Integration/` | `Integration` suite, no cluster needed |
| `tests/E2E/` | End-to-end suites, need a TiKV cluster (Docker) |
| `phpstan/` | Custom PHPStan rules |
| `scripts/` | `test-e2e.sh`, `test-e2e-apiv2.sh`, `generate-proto.sh`, `test.sh` |
| `bin/` | `pick-issue.sh`, `worktree*.sh` |
| `docs/` | User and developer docs, `docs/helpers/` knowledge base (FAQ, decisions), process docs |
| `benchmarks/`, `examples/` | Benchmarks and runnable examples |

Read `docs/helpers/faq.md` and `docs/helpers/decisions.md` before changing behavior: they record
recurring pitfalls and project decisions. Add a short entry when you learn something non-obvious.

## Commands

PHP 8.2+ (CI runs 8.2, 8.3 and 8.4) with `ext-grpc` for the `Grpc` suite and the linters.

```bash
composer install

# Lint: PHPCS + Rector (dry run) + PHPStan level 9
composer lint
composer lint:fix          # Rector + phpcbf
composer cs | cs-fix | phpstan | rector | rector:fix

# Tests
composer test:unit         # Unit suite, no cluster, no ext-grpc
composer test:grpc         # Grpc suite, needs ext-grpc, skips are failures
vendor/bin/phpunit --testsuite Integration

# E2E (Docker, see below)
make test-e2e              # RawKV on the V1TTL cluster, then TxnKV on the V1 cluster
make test-e2e-apiv2        # API V2 suite on its own cluster

make help                  # all make targets (install, up, down, clean, shell, example, proto-generate)
```

The Makefile calls `docker-compose` (v1 syntax); CI and the scripts use `docker compose`.

## Docker Compose

Three files, one project name:

| Files | Cluster | Used for |
|---|---|---|
| `docker-compose.yml` | PD + 3 TiKV, `tikv.toml` (V1TTL, `enable-ttl`) | RawKV E2E, `make up` |
| `docker-compose.yml` + `docker-compose.txnkv.yml` | `tikv-v1.toml` (V1, no TTL) | TxnKV E2E |
| `docker-compose.yml` + `docker-compose.apiv2.yml` | `tikv-apiv2.toml` (API V2) | API V2 E2E (`scripts/test-e2e-apiv2.sh`) |

The clusters are mutually exclusive in time: tear one down (`docker compose down -v`) before
starting the next, because TiKV cannot switch API mode on an existing volume.

The E2E tests run **inside the compose network** (service `php-client` or `php-test`) and reach PD
at `pd:2379` through the `PD_ENDPOINTS` environment variable. They never dial the published host
ports. TiKV and PD advertise their compose service names (`tikv1:20160`, `pd:2379`), which only
resolve inside the network, so the published host ports are for manual use of PD only (HTTP API,
`curl`, gRPC to PD).

Host ports are variables: `PD_PORT` (2379), `TIKV1_PORT` (20160), `TIKV2_PORT` (20161),
`TIKV3_PORT` (20162). `bin/worktree.sh` writes free ports and a unique `COMPOSE_PROJECT_NAME` to
`.env.worktree` in each worktree. Load it before running compose or the E2E scripts:

```bash
set -a && . ./.env.worktree && set +a
make test-e2e
```

Because the project name is unique per worktree and the tests run in the compose network, E2E runs
of several worktrees do not collide. `bin/worktree-teardown.sh` stops the stacks of all three file
combinations (and the `test` profile); `bin/worktree-done.sh` calls it.

## CI

`.github/workflows/ci.yml` runs on pull requests to `master` and on pushes to `master`. The
`changes` job detects documentation-only changes and the `docs` job checks them fast. `lint`,
the unit matrix, `grpc-unit-tests`, `integration-tests`, `e2e-tests` and `e2e-tests-apiv2` run only
for code changes. `ci-ok` aggregates the results and is the check to require. The `check-actor` job
allows the owner, collaborators with write access and `dependabot[bot]`.

## Conventions

- `declare(strict_types=1)` in every file, PSR-12 via `phpcs.xml.dist`, PHPStan level 9,
  Rector for PHP 8.2. All key ordering goes through `Client\Util\KeyOrder` (enforced by a PHPStan rule).
- Commit style: `type: subject (#N)` (`feat`, `fix`, `perf`, `test`, `docs`, `chore`, `ci`).
  Branches and worktrees follow `docs/workflow.md`.
- New code gets tests: unit tests in `tests/Unit/` mirroring `src/Client/`. Tests that need
  `ext-grpc` are listed in the `Grpc` suite in `phpunit.xml`, not in `Unit`.
- Update `CHANGELOG.md` under `## [Unreleased]` in every user-visible change.
- `composer.lock` is not committed; CI resolves the newest dev dependencies on every run.
