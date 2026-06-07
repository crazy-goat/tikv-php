# Workflow: Issue → Feature Branch → Implementation → Code Review → PR → CI → Merge

This document describes the complete workflow for handling issues in the
[crazy-goat/tikv-php](https://github.com/crazy-goat/tikv-php) repository
using `gh` and `git`.

---

## 1. Browse Open Issues

```bash
# List open issues (title, number, labels)
gh issue list --state open --limit 30

# View a specific issue (description, labels, state)
gh issue view <NUMBER> --json title,body,labels,state
```

**Criteria for selecting the most impactful issue:**
- Issues labeled `enhancement`, `code-quality`, `good-first-issue`
- Issues about stability, data correctness, performance
- Issues blocking other tasks
- Issues most relevant to users (README, API documentation)

---

## 2. Create a Fresh Feature Branch

```bash
# Make sure you're on master with the latest changes
git checkout master
git pull origin master

# Create a feature branch
git checkout -b fix/<NUMBER>-<short-description>
```

**Branch naming convention:** `feature/<NUMBER>-<kebab-case>`,
`fix/<NUMBER>-<kebab-case>`, `docs/<NUMBER>-<kebab-case>`,
or `refactor/<NUMBER>-<kebab-case>`.

Existing examples in this repository:
- `fix/96-n-plus-one-pd-region-lookups`
- `fix/delete-prefix-0xff-safety`
- `feature/295-servermanager-magic-timeout-constants` (early example)

---

## 3. Implement the Change

```bash
# Edit files, then commit and push
git add -A
git commit -m "feat: implement <short description> (closes #<NUMBER>)"
git push origin fix/<NUMBER>-<description>
```

**Commit message convention:**
- Type: `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `chore`
- Scope: optional `(rawkv)`, `(txnkv)`, `(retry)`, `(grpc)`, `(cache)`, `(connection)`, `(docs)` etc.
- Reference to issue: `(closes #<NUMBER>)` or `(#<NUMBER>)`

Examples from this project's history:
```
feat: batch pessimistic lock RPCs by region (#98)
fix: replace array_merge in scan loops with array_push for O(n) performance (#97)
fix: deletePrefix() now rejects prefixes consisting entirely of 0xFF bytes (#105)
```

---

## 4. Code Review via Subagent

After implementation, run a code review using a subagent (separate agent with
its own context). The subagent checks:

- Alignment with project structure (PSR-4: `src/Client/`, `src/Proto/`, `tests/`)
- Type correctness and signatures (PHPStan level 9)
- Error handling and edge cases
- Coding style (PSR-12, Slevomat coding standard)
- Strict types: `declare(strict_types=1);` on every file
- Test coverage (unit + E2E where applicable)
- Security (gRPC input validation, TiKV error handling, TLS usage)

```bash
# The subagent receives a task like:
# "Code review the changes in files: <list of files>.
#  Check: type correctness, error handling, PSR-12 compliance,
#  strict types declaration, missing tests, outdated documentation.
#  List all issues to fix."
```

---

## 5. Fix Issues Found in Code Review

```bash
# For each problem found:
# 1. Apply the fix
# 2. Commit with a descriptive message
git add -A
git commit -m "fix: <description of fix>"
git push origin <branch-name>
```

**All issues must be fixed – even the least significant ones.**

---

## 6. Repeat Code Review

After fixing, invoke the subagent for another code review.

Repeat steps 5→6 until the subagent reports no issues.

> **Acceptance criteria:** The subagent responds: "Code looks good, no issues
> to fix."

---

## 7. Run Linters and Tests Locally

Before opening a PR, verify that all linters and tests pass on your machine:

```bash
# Run all linters (PHPCS, Rector dry-run, PHPStan)
composer lint

# Auto-fix fixable issues (Rector, PHPCS fix)
composer lint:fix

# Run unit tests (fast, no TiKV required)
make test-unit
# or
./vendor/bin/phpunit --testsuite Unit --testdox

# Run E2E tests (requires TiKV cluster - start with `make up`)
make test-e2e

# Run all tests
make test
```

> **Note:** E2E tests require a running TiKV cluster. Start it with:
> ```bash
> make up
> ```
> This starts PD + 3 TiKV nodes on ports 2379, 20160, 20161, 20162.
> Stop with `make down`. If tests are interrupted, run `make down -v` to
> clean volumes.

After `composer lint:fix`, commit any fixes:

```bash
git add -A
git commit -m "style: auto-fix lint issues"
```

**Only create the PR when all lints and tests pass locally.**

---

## 8. Update CHANGELOG.md

```bash
# Edit CHANGELOG.md:
# - Add entry under [Unreleased] section
# - Follow Keep a Changelog format (https://keepachangelog.com/en/1.0.0/)
# - Use appropriate section: Added, Changed, Fixed, Removed, Deprecated
# - Include issue number, e.g. (#105)
```

---

## 9. Create a Pull Request

```bash
# Create a PR from the feature branch to master
gh pr create \
  --title "feat: <short description> (closes #<NUMBER>)" \
  --body "## Description

Closes #<NUMBER>

## Changes

- <list of changes>

## Changelog

<!-- Describe the changelog entry for this PR -->

## Code Review

- [ ] Passed subagent code review
- [ ] All review comments addressed" \
  --base master \
  --assignee @me
```

> **Note:** If you don't use `gh`, create the PR manually via GitHub UI.
> Only collaborators with write/admin permissions can trigger CI.
> External contributors will need a maintainer to approve the run.

---

## 10. Wait for CI

```bash
# Check PR status
gh pr view --json statusCheckRollup

# Wait for all checks to finish
gh pr checks --watch
```

CI workflow (`.github/workflows/ci.yml`) runs:

1. **check-actor** – verifies the PR author is a repo collaborator (write+)
2. **lint** – `composer cs` (PHPCS), `composer rector` (Rector dry-run),
   `composer phpstan` (PHPStan level 9)
3. **unit-tests** (PHP 8.2, 8.3, 8.4) – `vendor/bin/phpunit --testsuite Unit`
4. **e2e-tests** – RawKV E2E + TxnKV E2E (only if `src/` or `tests/E2E/`
   or Docker/Composer files changed). Spins up real TiKV clusters.

> **Note:** CI will be skipped entirely if the PR author is not a
> collaborator (admin/maintain/write). In that case, ask a maintainer to
> review and run CI.

---

## 11. Handle CI Failures

If CI fails:

```bash
# 1. See which checks failed
gh pr checks

# 2. View logs
gh run view --log --job <job-name>

# 3. Fix the issues locally
# 4. Run code review via subagent again (repeat steps 4-6)
# 5. Commit the fixes
git add -A
git commit -m "fix: <description of CI fix>"
git push origin <branch-name>

# 6. Wait for CI to re-run
gh pr checks --watch
```

> **Note:** There is no pre-push hook in this project. The `composer lint`
> check is run in CI, so run it locally before pushing to avoid CI failures.

**Repeat until all CI checks pass.**

---

## 12. Merge PR and Close Issue

```bash
# Merge PR (squash merge recommended for clean history)
gh pr merge --squash --delete-branch

# Close the issue (automatic if commit contains "closes #<NUMBER>")
# Alternatively:
gh issue close <NUMBER>
```

> **Note:** If branch protection requires a review, `gh pr merge` may be
> blocked. In that case, use the GitHub UI to squash-merge after obtaining
> approval.

---

## 13. Switch Back to master

```bash
git checkout master
git pull origin master
```

Done. Ready to start the next cycle from step 1.

---

## Quick Reference – Full Cycle

```bash
# 1. Pick an issue
gh issue list --state open --limit 30
gh issue view <NUMBER>

# 2. Feature branch
git checkout master && git pull origin master
git checkout -b fix/<NUMBER>-<short-description>

# 3. Implementation
# ... coding ...
git add -A && git commit -m "feat: implement <desc> (closes #<NUMBER>)"
git push origin fix/<NUMBER>-<description>

# 4. Code Review (subagent)
# ... fix issues ... (repeat until clean)

# 5. Run linters and tests locally
composer lint
make test-unit
# make test-e2e   # only if TiKV cluster is up

# 6. Update CHANGELOG.md

# 7. PR
gh pr create --title "feat: <desc> (closes #<NUMBER>)" --body "..." --base master

# 8. CI
gh pr checks --watch
# ... if failures → fix, code review, push → wait for CI (repeat)

# 9. Merge
gh pr merge --squash --delete-branch
gh issue close <NUMBER>

# 10. Switch back to master
git checkout master && git pull origin master
```

---

## Notes

- **gh** must be configured and authenticated (`gh auth status`).
- **Default branch is `master`** – don't commit directly to it.
- Branch protection on `master` may require:
  - at least 1 approving review before merge
  - All status checks passing (lint, unit tests, E2E)
- CI only runs for collaborators (admin/maintain/write). External
  contributors need a maintainer to approve the workflow run.
- Keep feature branches short-lived. If a rebase is needed:
  ```bash
  git fetch origin master
  git rebase origin/master
  git push --force-with-lease origin <branch-name>
  ```
- Code review via subagent runs locally – the subagent has access to
  read/write/edit/bash tools. Give it clear instructions on what to check.
- E2E tests require Docker. Use `make up` to start TiKV and `make down`
  to stop it. If state gets corrupted: `make clean && make up`.

### Useful make targets

| Command | Description |
|---------|-------------|
| `make install` | Install PHP dependencies |
| `make test` | Run all tests (unit + E2E) |
| `make test-unit` | Run unit tests only (no TiKV needed) |
| `make test-e2e` | Run E2E tests (requires TiKV cluster) |
| `make up` | Start TiKV cluster (PD + tikv1/2/3) |
| `make down` | Stop TiKV cluster |
| `make clean` | Destroy everything (containers + volumes) |
| `make shell` | Open dev shell in Docker container |
| `make example` | Run basic example |
| `make proto-generate` | Regenerate PHP classes from proto files |
| `make proto-clean` | Remove generated proto classes |

### Useful composer scripts

| Command | Description |
|---------|-------------|
| `composer lint` | Run all linters: PHPCS + Rector + PHPStan |
| `composer lint:fix` | Auto-fix fixable issues (Rector + PHPCS) |
| `composer cs` | Code style check (PHPCS) |
| `composer cs-fix` | Auto-fix code style (PHPCS) |
| `composer phpstan` | Static analysis (level 9) |
| `composer rector` | Rector dry-run |
| `composer rector:fix` | Apply Rector rules |
| `composer test` | Run PHPUnit (all tests) |
| `composer test:unit` | Run unit tests only |
| `composer test:e2e` | Run E2E tests only |
