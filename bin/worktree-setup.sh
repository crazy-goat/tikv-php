#!/usr/bin/env bash
# Install Composer dependencies in a fresh worktree.
# Called by bin/worktree.sh after a new worktree is created.
# The TiKV cluster is not started here: E2E tests start it (make test-e2e, scripts/test-e2e*.sh).
# src/Proto/ is committed, so protoc is not needed unless the proto files change.
#
# composer.json requires the ext-grpc extension (">=1.57"), but the Unit
# suite and the linters do not need it. The platform requirement is therefore
# ignored for the install only when the extension is really missing: on a
# machine that has it, Composer still enforces the version. See
# docs/helpers/faq.md, "composer.json requires ext-grpc, but the unit job
# installs without it".
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

install_args=(--no-interaction --prefer-dist)

if ! php -r 'exit(extension_loaded("grpc") ? 0 : 1);'; then
    echo "ext-grpc is not installed - installing without the platform check."
    echo "The Grpc suite, the E2E tests and the client itself need it:"
    echo "docs/getting-started.md#installing-the-grpc-extension"
    install_args+=(--ignore-platform-req=ext-grpc)
fi

composer install "${install_args[@]}"
