#!/bin/bash
#
# Docker Compose command detection, shared by the end-to-end runner scripts.
#
# Compose v2 ships as a "docker" plugin; the v1 binary is the fallback for
# installations that still have it. Without either one the scripts cannot
# start a cluster, so they stop here with a clear message instead of failing
# later with a confusing "command not found".
#
# The sourcing script runs the command through the COMPOSE array:
#
#   . "$(dirname "$0")/lib/compose.sh"
#   "${COMPOSE[@]}" up -d pd tikv1 tikv2 tikv3
#
# A script that needs extra compose files or profiles defines its own
# `compose` wrapper after sourcing this file; the default below covers the
# plain case.

# The php-client/php-test images run as an unprivileged user and bind-mount
# the checkout, so their UID/GID must match the host owner (docker-compose.yml
# reads HOST_UID/HOST_GID as build args). Exporting them here keeps every
# compose call in the runner scripts consistent; an explicit value wins.
export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

# shellcheck disable=SC2034 # COMPOSE is read by the script that sources this file
if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
else
    echo "Neither 'docker compose' nor 'docker-compose' is available." >&2
    echo "Install Docker Compose and run this script again." >&2
    exit 1
fi

# shellcheck disable=SC2329 # invoked by the script that sources this file
compose() {
    "${COMPOSE[@]}" "$@"
}

# Lists the services that have a running container. `ps --format` is a v2-only
# option (v1's `ps` knows only -q/--services/--filter), so this uses the
# options both versions support. A non-zero exit means "cannot tell", which
# the readiness gate treats as not ready.
# shellcheck disable=SC2329 # invoked by the script that sources this file
compose_running_services() {
    compose ps --services --filter status=running
}
