#!/usr/bin/env bash
# Stop and remove the Docker test stacks of this worktree.
# Called by bin/worktree-done.sh before the worktree is removed.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [[ -f .env.worktree ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env.worktree
  set +a
  export COMPOSE_ENV_FILES="$PWD/.env.worktree"
else
  unset COMPOSE_ENV_FILES
fi

if [[ -z "${COMPOSE_PROJECT_NAME:-}" ]]; then
  echo "COMPOSE_PROJECT_NAME is empty (no .env.worktree); refusing to stop the stacks of another checkout." >&2
  exit 1
fi

# All compose file combinations used by the repo (they share one project name).
# `--profile '*'` also removes the php-test container of the "test" profile.
combos=(
  "-f docker-compose.yml"
  "-f docker-compose.yml -f docker-compose.apiv2.yml"
  "-f docker-compose.yml -f docker-compose.txnkv.yml"
)
for files in "${combos[@]}"; do
  echo "docker compose down: $files"
  # shellcheck disable=SC2086
  docker compose $files --profile '*' down -v --remove-orphans || true
done
