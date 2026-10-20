# Release workflow

One milestone = one release. Versions follow
[Semantic Versioning](https://semver.org/) and the changelog follows
[Keep a Changelog](https://keepachangelog.com/). Tags are `vX.Y.Z`.

Everything is written in **English**, including release notes.

## 1. Release gate

A release is ready when the milestone has **no open issues**:

```bash
gh api repos/{owner}/{repo}/milestones --jq '.[] | select(.title=="vX.Y.Z") | {title, open_issues, closed_issues}'
```

- Open issues that will not make it: move them to the next milestone
  (`gh issue edit <N> --milestone vX.Y.(Z+1)`).
- The default branch must have a green `ci-ok`.

## 2. Choose the version

| Change | Bump |
|---|---|
| Bug fixes only | patch (`1.2.3` → `1.2.4`) |
| New, backward compatible features | minor (`1.2.3` → `1.3.0`) |
| Breaking changes | major (`1.2.3` → `2.0.0`) |

Before `1.0.0`, breaking changes bump the minor version. The milestone title
already holds the planned version. Change the milestone title if the plan changed.

Mark every breaking entry with a **BREAKING** prefix and a migration note —
see the [stability policy](../README.md#stability-and-versioning) for the
public API surface the promise covers. A change that breaks a class outside
that surface is announced as **Breaking change:** with a migration note.
That section also names the versions of the release, so update them here.

## 3. Prepare the CHANGELOG (pull request)

```bash
git switch -c chore/release-vX.Y.Z
```

In `CHANGELOG.md`:

- Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`.
- Add a fresh empty `## [Unreleased]` above it.
- Group entries under Added, Changed, Deprecated, Removed, Fixed, Security.
- Update the compare links at the bottom, if the file has them.
- Bump the version in files that carry it (`composer.json` does not need it, but
  `package.json`, `version.go`, extension headers do). See `AGENTS.md`.
- Cross-check the README against the section you are writing: every capability
  in the new `CHANGELOG.md` section has to appear under **Implemented
  Operations** in `README.md` (or already under **Completed** in its
  **Roadmap**), and nothing that is still planned may be listed there as
  shipped. Every item of the **Planned** list of the roadmap must have an
  open issue and must not be shipped yet: drop an item from the list once its
  issue is closed, and add an item when a new feature-gap issue is opened
  whose work is still to be done. The list is curated, not a mirror of the
  open issues — it holds the open feature-gap issues, and an issue whose
  work already shipped stays out of it even while the issue is still open.

- Update `README.md` ("Stability and versioning"): **every** version of the
  whole "Stability and versioning" section has to follow the release — the
  current release becomes `vX.Y.Z`, the recommended Composer constraint
  becomes `~X.Y.Z`, the range that constraint resolves to becomes
  `>=X.Y.Z <X.(Y+1).0`, and the next minor `X.(Y+1).0` replaces the old one
  in the two paragraphs below the JSON block and in the intro of the section
  (the intro names the pair `X.Y.Z → X.(Y+1).0`, and it is part of the
  section too).
  The stability guard test (`tests/Unit/Docs/StabilityPolicyGuardTest.php`)
  reads the latest released version from the changelog and fails while the
  section names another one — so the check follows the release instead of
  surfacing months later. Touching `README.md` also makes this pull request a
  code change, so the unit tests run on it.
Open a PR titled `chore: release vX.Y.Z`, wait for `ci-ok`, squash merge
(`gh pr merge --squash --delete-branch`). Push the branch with an explicit ref:

```bash
git push -u origin refs/heads/chore/release-vX.Y.Z
```

tikv-php carries no version number in source files (Composer takes it from the git tag).
The `CHANGELOG.md` section is the only thing to prepare. Older sections are written as
`## [vX.Y.Z] — date`; the release workflow accepts both `[X.Y.Z]` and `[vX.Y.Z]`.

## 4. Tag

Tag the merge commit on the default branch with an **annotated** tag:

```bash
git switch master && git pull --ff-only
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin refs/tags/vX.Y.Z
```

## 5. GitHub Release

Pushing the tag starts `.github/workflows/release.yaml`. GitHub runs the workflow
file from the **tagged commit**, so the release PR with the `## [X.Y.Z]` section
must be **merged before** you tag.

The workflow creates the GitHub Release with the notes from the matching `CHANGELOG.md`
section. It fails when the section is missing or empty, and it ignores the link references
at the bottom of the file. Tags with a `-` (for example `v0.9.0-rc.1`) become pre-releases.

GitHub rejects release notes longer than 125000 characters. The workflow cuts the notes
below 120000 characters at a line boundary and adds a link to `CHANGELOG.md` at the tag.

```bash
gh run watch
gh release view vX.Y.Z
```

## 6. Close the milestone

```bash
gh api -X PATCH repos/{owner}/{repo}/milestones/<number> -f state=closed
```

Make sure the next milestone exists. Milestones in this repository are minor versions
(`v0.9.0`, `v0.10.0`, ...).

## 7. After the release

- Check that install instructions work with the new version (Packagist: `crazy-goat/tikv-client`).
- If something is wrong, do not move the tag. Fix forward with a patch release.

## Checklist

- [ ] Milestone has no open issues, CI is green
- [ ] CHANGELOG section `[X.Y.Z] - date` written, `[Unreleased]` is empty
- [ ] README "Implemented Operations" and "Roadmap" cross-checked against the new CHANGELOG section

- [ ] README.md "Stability and versioning" updated: **every** version of the
      whole section follows the release — the current release `vX.Y.Z` (also in
      the intro of the section, next to the previous one), the recommended
      constraint `~X.Y.Z`, the exact version `"X.Y.Z"` recommended for a
      reproducible build, the range `>=X.Y.Z <X.(Y+1).0` that constraint
      resolves to, and the next minor `X.(Y+1).0`- [ ] Release PR merged
- [ ] Annotated tag `vX.Y.Z` pushed
- [ ] GitHub Release exists with the CHANGELOG notes
- [ ] Milestone closed, next milestone exists
