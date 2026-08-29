# Workflow: Issue → Feature Branch → Implementation → Code Review → PR → CI → Merge

This document describes the complete workflow for handling issues in the
[crazy-goat/tikv-php](https://github.com/crazy-goat/tikv-php) repository
using `gh` and `git`.

---

## 1. Browse Open Issues via Subagent (Milestone First)

Browsing and triaging open issues is token-heavy (titles, bodies, labels,
comments, related code). Delegate it to a subagent with its own context.

**Milestones:** issues are organized into **version milestones** (currently
open: `v0.4.0` – `v0.14.0`; `v0.3.0` and lower are closed/released). Work
**always starts from the lowest version milestone that still has open
issues** — issues from higher milestones wait until every lower one is
empty. Within the chosen milestone, order by severity label
(`severity:critical` → `severity:high` → `severity:medium` →
`severity:low`), then by impact.

```bash
# The subagent receives a task like:
# "In crazy-goat/tikv-php, list the open milestones, pick the lowest-version
#  milestone that still has open issues, and list its top 5 most impactful
#  open issues. For each, return: number, title, labels (esp. severity:*),
#  one-paragraph rationale, and a recommended branch name
#  (fix/<N>-<kebab> or feature/<N>-<kebab>).
#  Within the milestone prioritize: severity (critical > high > medium > low),
#  then bugs over enhancements/documentation, then user-facing impact."
```

```bash
# There is no `gh milestone` subcommand in this gh version — use the API:
gh api "repos/crazy-goat/tikv-php/milestones?state=open&per_page=100" \
  --jq '.[] | "\(.title)\topen:\(.open_issues)"'

# Then list the issues of the lowest open milestone:
gh issue list --state open --milestone "v0.4.0" --limit 100 \
  --json number,title,labels
gh issue view <NUMBER> --json title,body,labels,state,milestone
```

> **Note:** `gh issue list` returns **at most 30 issues by default** — the
> triage task must explicitly raise `--limit` (e.g. `--limit 100`, max 1000)
> so issues beyond the first page are not missed.

> **Why a subagent:** issue bodies, comments, and related code can easily
> exceed thousands of tokens. Keeping this in a separate context protects the
> main session's budget for implementation and review.

### Fast path: ranked candidates via `bin/pick-issue.sh`

The workflow is milestone-driven, so before delegating triage to a subagent,
run the ranking script — it costs a few tokens instead of thousands:

```bash
bin/pick-issue.sh                             # top 5 of the lowest open milestone
bin/pick-issue.sh --milestone=v0.5.0 --top=5  # explicit milestone
bin/pick-issue.sh --json                      # machine-readable, for scripting
```

The script lists open milestones, picks the **lowest** one (semver), scores
its open issues — severity labels (`severity:critical`/`high`/`medium`/`low`),
type labels (`bug`/`security`/`data-loss`/`enhancement`/`performance`/
`documentation`), meta labels (`good first issue`/`help wanted`/`question`),
title signals (leak/crash/security/performance/dead code), age and comment
count — and prints the top N with an explicit per-issue score breakdown. It
never reads issue bodies or comment text, and it always paginates (`gh` caps
lists at 30 per page). The LLM/user still makes the final pick from the
ranked candidates; the script only narrows the pool. If desired, a subagent
then deep-dives into the bodies of the top candidates before the final pick.

> **Release gate:** when the target milestone has 0 open issues left, the
> script exits with code **3** and prints the release steps instead of
> candidates — the workflow STOPS, a release must be cut and the milestone
> closed before picking again (see "Release Gate" below).

**Selection criteria (applied by the subagent, within the current
milestone):**
- Severity: `severity:critical` first, then `high`, `medium`, `low`
- `bug` label before `enhancement` / `documentation`
- Issues about stability, data correctness, performance
- Issues blocking other tasks
- Issues most relevant to users (README, API documentation)

---

## Release Gate

Work is driven by the **lowest open milestone**: a milestone is a release
candidate, not a bottomless backlog. When the current milestone has no open
issues left, the workflow must **stop** — do not silently pick an issue from
a higher milestone.

`bin/pick-issue.sh` enforces this:

- exit code `0` → candidates printed, proceed to step 2 with the ranked list
- exit code `1` → `gh`/API error, retry
- exit code `2` → usage error (unknown option or milestone)
- exit code `3` (`RELEASE NEEDED`) → the target milestone is empty; cut the
  release, close the milestone, then re-run the script so the next milestone
  becomes the target

```bash
# When the picker says RELEASE NEEDED:
git checkout master && git pull origin master

# 1. Finalize CHANGELOG.md (per step 8, Keep a Changelog):
#    - every entry under [Unreleased] moves into a new versioned section:
#      ## [vX.Y.Z] - YYYY-MM-DD   (today's date, ISO 8601)
#    - [Unreleased] stays at the top, empty, ready for the next cycle
#    - drop any "### Added/Changed/Fixed/Removed" headings that ended up empty
#    - keep one section per change type; entries within a section keep
#      their issue references, e.g. (#105)
#    - commit this on master: "chore: finalize CHANGELOG for vX.Y.Z"

# 2. Tag + publish the release. Notes follow the existing tag structure
#    (see v0.2.0, which uses "### Added" + "### Removed" + "### Full
#    Changelog"): a one-line title summarising the theme, then
#    "### Added" / "### Changed" / "### Fixed" / "### Removed" sections
#    summarising the CHANGELOG entries for humans (short bullets, not the
#    full entries), then a compare link:
#      https://github.com/crazy-goat/tikv-php/compare/v<prev>...v<new>
#    (e.g. compare/v0.1.0...v0.2.0 — the first release of a line, like
#    v0.1.0, may instead use free-form "### What's New" sections; match
#    the dominant convention of the previous tags).
gh release create vX.Y.Z --title "vX.Y.Z" --notes "…"
#    (The release for tag vX.Y.Z must not already exist — `gh release
#    create` fails with 'already_exists' otherwise. An existing git tag
#    without a release is reused, and the tag is created from the latest
#    state of the default branch unless --target says otherwise — after
#    the `git checkout master && git pull` above, that is the local HEAD.)

# 3. Close the finished milestone (no `gh milestone` subcommand — use the API)
gh api --method PATCH repos/crazy-goat/tikv-php/milestones/<NUMBER> -f state=closed

# 4. Re-run the picker — the next milestone becomes the target
bin/pick-issue.sh
```

> **Note:** the CHANGELOG finalization commit (step 1) must land on master
> **before** `gh release create` tags it — the release is cut from the tag,
> so a changelog committed afterwards is not part of the release's source
> tree. The release notes themselves (`--notes`) live outside the repo, so
> they can be drafted any time, but are supplied at or before step 2.

---

## 2. Create a Fresh Feature Branch

```bash
# Make sure you're on master with the latest changes
git checkout master
git pull origin master

# Create a feature branch
git checkout -b fix/<NUMBER>-<short-description>
```

**Branch naming convention:** `fix/<NUMBER>-<kebab-case>` (preferred),
also `feature/<NUMBER>-<kebab-case>`, `docs/<NUMBER>-<kebab-case>`,
or `refactor/<NUMBER>-<kebab-case>`.

Existing examples in this repository:
- `fix/96-n-plus-one-pd-region-lookups`
- `fix/104-pdclient-region-leader-fail-closed`
- `fix/80-plaintext-grpc-tls-downgrade`
- `feature/295-servermanager-magic-timeout-constants` (early example)

---

## 3. Implement the Change (via Worker/Coder Subagent)

Implementation is delegated to a subagent (`worker` or `coder`) so the main
session stays free to orchestrate, review findings, and handle the next steps.

```bash
# The subagent receives a task like:
# "Implement issue #<NUMBER> on branch fix/<NUMBER>-<description>.
#  Read docs/helpers/ (faq.md, decisions.md) first — it documents
#  recurring pitfalls and project decisions that apply to this task.
#  Read docs/contributing.md and docs/development.md for project
#  conventions, then read the issue body and make the smallest correct change.
#  Run the relevant tests for the changed behavior.
#  Commit and push when done.
#
#  Your report must ALWAYS contain:
#  1. Files changed and why
#  2. What was the BIGGEST problem or obstacle during implementation
#     (with details: where, why it was hard, how you solved it)
#  3. Any bugs or places to improve you discovered along the way
#     (also outside the scope of this issue) - each with file/line,
#     short description, and suggested fix"
```

After the subagent reports, commit and push if it did not do so already:

```bash
# Ensure everything is committed and pushed
git add -A
git commit -m "feat: implement <short description> (#<NUMBER>)"
git push origin fix/<NUMBER>-<description>
```

**Commit message convention:**
- Type: `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `chore`
- Scope: optional `(rawkv)`, `(txnkv)`, `(retry)`, `(grpc)`, `(cache)`, `(connection)`, `(docs)` etc.
- Reference to issue: `(#<NUMBER>)` or `(closes #<NUMBER>)`

Examples from this project's history:
```
feat: batch pessimistic lock RPCs by region (#98)
fix: replace array_merge in scan loops with array_push for O(n) performance (#97)
fix: deletePrefix() now rejects prefixes consisting entirely of 0xFF bytes (#105)
```

> **Coder output contract (non-negotiable):** the subagent must always return
> (1) changed files, (2) the biggest problem it faced with details, and
> (3) any discovered bugs / places to improve - even ones outside the current
> issue's scope. The main session stores these findings for the final report
> (step 14).

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
#  Read docs/helpers/ (faq.md, decisions.md) first and flag any
#  violations of documented decisions.
#  Check: type correctness, error handling, PSR-12 compliance,
#  strict types declaration, missing tests, outdated documentation.
#  List all issues to fix."

After the review, the subagent should append any non-obvious findings to
`docs/helpers/` (see "Knowledge Base (docs/helpers/)" below) — typically
as part of the fix commits that follow.
```

> **Why a subagent:** code review reads the full diff plus surrounding code,
> runs static analysis, and produces a structured findings list. Delegating
> keeps the main session focused on fixes and the next workflow step.

---

## 5. Fix Issues Found in Code Review

```bash
# For each problem found:
# 1. Apply the fix
# 2. Commit with a descriptive message
git add -A
git commit -m "fix: <description of fix>"
git push origin fix/<NUMBER>-<description>
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
# Run all linters (PHPCS, Rector dry-run, PHPStan level 9)
composer lint

# Auto-fix fixable issues (Rector, PHPCS)
composer lint:fix

# Run unit tests (fast, no TiKV required)
composer test:unit

# Run gRPC unit tests (needs PHP grpc extension; skips are failures)
composer test:grpc

# Run E2E tests (requires TiKV cluster - start with `make up`)
make test-e2e
# or directly:
composer test:e2e
```

> **Note:** E2E tests require a running TiKV cluster. Start it with:
> ```bash
> make up
> ```
> This starts PD + 3 TiKV nodes on ports 2379, 20160, 20161, 20162.
> Stop with `make down`. If tests are interrupted or state is corrupted,
> run `make clean` (containers + volumes) and `make up` again.

> **Note:** `composer test:grpc` runs the `Grpc` testsuite, which exercises
> real gRPC connections and requires the `grpc` PHP extension
> (`--fail-on-skipped`). CI always has the extension; locally it must be
> installed (e.g. via pecl) or skipped fails the run.

> **Note:** CI does **not** enforce a coverage floor. The `grpc-unit-tests`
> job collects coverage with PCOV (`--coverage-xml`), but there is no
> coverage gate in `composer.json` — don't block on coverage numbers.

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
# - Follow Keep a Changelog format (https://keepachangelog.com/en/1.1.0/)
# - Use appropriate section: Added, Changed, Fixed, Removed, Deprecated
# - Include issue number, e.g. (#105)
```

> **Note:** the `[Unreleased]` section is finalized at release time by the
> **Release Gate** (before tagging): its entries move into a new
> `## [vX.Y.Z] - YYYY-MM-DD` section. Until then every merged PR appends
> under `[Unreleased]` — never under a version heading.

---

## 9. Create a Pull Request

```bash
# Create a PR from the feature branch to master
gh pr create \
  --title "feat: <short description> (#<NUMBER>)" \
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
> Only the repo owner or collaborators with admin/maintain/write permission
> can trigger CI. External contributors need a maintainer to approve the run
> (see step 10).

---

## 10. Wait for CI

```bash
# Check PR status
gh pr view --json statusCheckRollup

# Wait for all checks to finish
gh pr checks --watch
```

CI workflow (`.github/workflows/ci.yml`) runs:

1. **check-actor** – verifies the PR author is the repo owner or a
   collaborator with admin/maintain/write permission; otherwise **all CI is
   skipped** (ask a maintainer to run it)
2. **changes** – detects whether paths affecting E2E changed
3. **lint** (PHP 8.4) – `composer cs` (PHPCS), `composer rector` (Rector
   dry-run), `composer phpstan` (PHPStan level 9)
4. **unit-tests** (PHP 8.2, 8.3, 8.4) – `phpunit --testsuite Unit`
5. **grpc-unit-tests** (PHP 8.4, grpc extension, PCOV) –
   `phpunit --testsuite Grpc --fail-on-skipped`
6. **e2e-tests** – RawKV E2E (V1ttl cluster) + TxnKV E2E (V1 cluster), only
   when `src/`, `tests/E2E/`, docker/composer files, or the CI workflow
   itself changed. Spins up real TiKV clusters via docker compose.

> **Note:** CI will be skipped entirely if the PR author is not an owner or
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
git push origin fix/<NUMBER>-<description>

# 6. Wait for CI to re-run
gh pr checks --watch
```

> **Note:** There is no pre-push hook in this project. The lint check runs
> in CI, so run `composer lint` locally before pushing to avoid CI failures.

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

> **Note:** When the default branch (`master`) is checked out in another
> linked git worktree, `gh pr merge --squash --delete-branch` fails at its
> local branch-deletion step with
> `fatal: '<branch>' is already used by worktree at '<path>'` — the merge
> itself succeeds. Delete the remote branch manually instead:
>
> ```bash
> git push origin --delete <branch>
> git worktree remove <path>
> ```

---

## 13. Switch Back to master

```bash
git checkout master
git pull origin master
```

---

## 14. Report Implementation Problems and Offer a GitHub Issue

At the end of the workflow, present the findings collected from the
implementation subagent(s) and decide with the user whether they deserve a
dedicated GitHub issue.

**Display to the user:**

1. **Biggest problem(s) faced during implementation** - as reported by the
   worker/coder subagent in step 3.
2. **Discovered bugs / places to improve** - each with file/line, short
   description, and suggested fix (including findings outside the scope of the
   issue just closed).

**Verify each candidate finding with a review subagent (read-only) before
offering or creating an issue.** For every candidate finding the subagent
must confirm:

1. **The finding is real** - read the cited file/line(s) on the current
   branch and confirm the behavior actually occurs and is reachable; check
   whether it is by-design and already documented (those are skipped, not
   filed).
2. **No similar issue exists on GitHub** - search open *and* closed issues.
   `gh issue list` returns at most 30 issues by default, so always pass an
   explicit limit:

   ```bash
   gh issue list --state open --limit 150 --json number,title,labels,body
   gh issue list --state closed --limit 150 --json number,title,labels
   gh search issues --repo crazy-goat/tikv-php --state open --limit 50 "<keyword>"
   ```

   Same or overlapping scope counts as tracked; known related issues (e.g.
   referenced from CHANGELOG entries) must be checked explicitly.
3. **A recommendation per finding**: (a) create a new issue - with proposed
   title and labels per the project's conventions (`bug` / `enhancement` /
   `documentation`), (b) skip - already tracked (cite the issue number), or
   (c) skip - not real or by-design and documented.

The verification subagent must not modify files and must not create/close/
edit issues itself. Like steps 3 and 4, it reads `docs/helpers/`
(faq.md, decisions.md) first. Only findings that pass verification (real +
untracked) are offered to the user / created.

**Then ask:** "Create GitHub issue(s) for these findings?"

- If yes, create an issue via `gh` (adjust labels to the project's conventions):

```bash
gh issue create \
  --title "<short title of the discovered problem>" \
  --body "## Description

<what was found>

## Where

- <file:line>

## Suggested fix

<short description>" \
  --label bug \
  --milestone "v0.4.0"
```

- Assign `--label bug` for confirmed bugs, `enhancement` for feature
  requests, or `documentation` for doc improvements. Assign a milestone with
  `--milestone "v<n>"` — by default the **current working milestone** (the
  lowest open version); use a higher one only when the fix clearly targets a
  later release. One issue per distinct finding keeps them actionable.
- If the user declines or the findings are already tracked, just record the
  outcome and finish.

> **Note:** findings that were already fixed as part of this workflow do not
> need an issue - only newly discovered, still-open problems should be
> reported.

---

## Knowledge Base (docs/helpers/)

`worker`/`coder` (implementation) and `review` (code review) subagents
maintain a persistent knowledge base in `docs/helpers/` so lessons learned
carry over to future tasks:

- `docs/helpers/faq.md` — frequently asked questions, recurring pitfalls
  (E2E cluster ports, `grpc` extension for the Grpc testsuite, CI actor
  gate, `gh` default limits) and their solutions
- `docs/helpers/decisions.md` — important project decisions with rationale
  (branch naming, commit style, PHPStan level 9, no coverage floor)
- `docs/helpers/README.md` — structure and rules for the knowledge base

Subagents **read** the knowledge base before starting a task and **append**
short entries after finishing (one topic: the problem, the
solution/decision, optionally an issue/commit reference). Entries are
committed as part of the regular fix/feat commits — no extra PRs. In doubt,
extend `docs/troubleshooting.md` or ask the user before adding a new entry.

---

## Quick Reference – Full Cycle

```bash
# 1. Pick an issue
#    bin/pick-issue.sh → ranked top-5 of the LOWEST open milestone
#    (exit code 3 = RELEASE NEEDED: finalize CHANGELOG [Unreleased] →
#    [vX.Y.Z], cut release, close milestone, re-run — see Release Gate)
#    if needed, subagent deep-dives into top candidates' bodies
#    then: "Implement issue #N..."

# 2. Feature branch
git checkout master && git pull origin master
git checkout -b fix/<NUMBER>-<description>

# 3. Implementation (worker/coder subagent)
#    subagent: "Implement issue #<NUMBER>..."
#    report must include: files changed, BIGGEST problem, discovered bugs
#    / places to improve (also out of scope)
git add -A && git commit -m "feat: implement <desc> (#<NUMBER>)"
git push origin fix/<NUMBER>-<description>

# 4. Code Review (subagent)
# ... fix issues ... (repeat until clean)

# 5. Run linters and tests locally
composer lint
composer test:unit
composer test:grpc    # needs grpc PHP extension
make test-e2e         # requires `make up` (TiKV cluster)

# 6. Update CHANGELOG.md

# 7. PR
gh pr create --title "feat: <desc> (#<NUMBER>)" --body "..." --base master

# 8. CI
gh pr checks --watch
# ... if failures → fix, code review, push → wait for CI (repeat)

# 9. Merge
gh pr merge --squash --delete-branch
#    (merging from a linked worktree? `--delete-branch` fails there — see step 12)
gh issue close <NUMBER>

# 10. Switch back to master
git checkout master && git pull origin master

# 11. Report + offer GitHub issue for discovered problems
#    show: biggest problem(s), discovered bugs / places to improve
#    verify each candidate with a review subagent (finding is real?
#    no duplicate on GitHub? use --limit >30 in issue lists)
#    then ask: "Create GitHub issue(s)?" → if yes: gh issue create ...
```

---

## Subagent Usage Summary

Four steps of this workflow are delegated to subagents to keep the main
session's context lean:

| Step | Subagent task                              | Why delegate                          |
| ---- | ------------------------------------------ | ------------------------------------- |
| 1    | Triage open issues — lowest open version milestone first, ranked shortlist | Issue bodies + comments are token-heavy |
| 3    | Implement the issue (worker/coder)         | Coding context is token-heavy; agent returns structured report (files, biggest problem, discovered bugs) |
| 4    | Code review of the implementation diff     | Full diff + surrounding code is token-heavy |
| 14   | Verify candidate findings before creating GitHub issues (read-only: is the finding real? is it already tracked?) | GitHub duplicate search (open + closed, `--limit` > 30) plus code verification across several findings is query-heavy |

All subagents have read/write/edit/bash tools and operate on the same
repository (the step-14 verifier is instructed to run read-only). Give each
one a clear, scoped instruction and a defined output format (ranked list
with rationale / numbered findings list / coder report with biggest problem
+ discovered bugs / per-finding verification verdict).

**Knowledge base:** implementation and review subagents read
`docs/helpers/` before starting and append learnings after finishing
(see "Knowledge Base (docs/helpers/)" above).

---

## Notes

- **gh** must be configured and authenticated (`gh auth status`).
- **Default branch is `master`** – don't commit directly to it.
- **Issues are organized into version milestones** (`v0.4.0` … `v0.14.0`
  open). Work always starts from the **lowest version milestone with open
  issues**; a milestone is "done" when it has no open issues left, then the
  next version becomes the working target. Run `bin/pick-issue.sh` to get
  the ranked shortlist automatically; its exit code **3** triggers the
  Release Gate above (finalize CHANGELOG, cut the release, close the
  milestone, re-run).
- Branch protection on `master` may require:
  - at least 1 approving review before merge
  - All status checks passing (lint, unit tests, grpc unit tests, E2E)
- No pre-push hook in this project – run `composer lint` locally before
  pushing.
- CI only runs for the repo owner and collaborators (admin/maintain/write).
  External contributors need a maintainer to approve the workflow run.
- Keep feature branches short-lived. If a rebase is needed:
  ```bash
  git fetch origin master
  git rebase origin/master
  git push --force-with-lease origin fix/<NUMBER>-<description>
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
| `composer test:grpc` | Run gRPC unit tests (needs grpc extension) |
| `composer test:e2e` | Run E2E tests only (requires TiKV cluster) |
