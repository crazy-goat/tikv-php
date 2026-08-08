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

## Tests are delegated / E2E needs Docker

E2E suites (`E2E-RawKV`, `E2E-TxnKV`) spin up real TiKV clusters via docker
compose — they only run when relevant paths change (`src/`, `tests/E2E/`,
docker/composer files, CI workflow).
