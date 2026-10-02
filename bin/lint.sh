#!/usr/bin/env bash
# Run all static analysis, linters and formatter checks. --fix applies fixes first.
# Needs composer dependencies, shellcheck and hadolint (CI installs them).
set -uo pipefail
cd "$(dirname "$0")/.."

FIX=0
[ "${1:-}" = "--fix" ] && FIX=1
failed=()

step() {
    local name="$1"; shift
    echo "==> $name"
    "$@" || failed+=("$name")
}

if [ "$FIX" = 1 ]; then
    step "rector --fix" vendor/bin/rector process
    step "phpcbf" vendor/bin/phpcbf --standard=phpcs.xml.dist
fi

step "phpcs" vendor/bin/phpcs --standard=phpcs.xml.dist
step "rector" vendor/bin/rector process --dry-run
step "phpstan" vendor/bin/phpstan analyse --memory-limit=1G --no-progress
# bin/pick-issue.sh is a byte-identical copy of the shared standard script and is linted there.
step "shellcheck" bash -c 'git ls-files -z "*.sh" | grep -zv "^bin/pick-issue\.sh$" | xargs -0 -r shellcheck'
step "hadolint" hadolint Dockerfile

if [ "${#failed[@]}" -gt 0 ]; then
    echo "Failed: ${failed[*]}" >&2
    exit 1
fi
echo "All checks passed."
