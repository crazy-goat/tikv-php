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
