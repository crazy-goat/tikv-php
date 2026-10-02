#!/usr/bin/env bash
# Install Composer dependencies in a fresh worktree.
# Called by bin/worktree.sh after a new worktree is created.
# The TiKV cluster is not started here: E2E tests start it (make test-e2e, scripts/test-e2e*.sh).
# src/Proto/ is committed, so protoc is not needed unless the proto files change.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

composer install --no-interaction --prefer-dist
