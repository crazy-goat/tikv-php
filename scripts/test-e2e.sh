#!/bin/bash
#
# The complete end-to-end run: the RawKV suite against the V1TTL cluster,
# then the TxnKV suite against the V1 cluster (the two cluster modes are
# mutually exclusive, see AGENTS.md).
#
# `composer test:e2e` calls this script.
set -e

echo "=========================================="
echo "TiKV PHP Client - E2E Test Runner"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Docker Compose v2 ships as a `docker` plugin; the v1 binary is the
# fallback for installations that still have it. Without either one the run
# cannot start a cluster, so it stops here instead of failing later with a
# confusing "command not found".
# shellcheck source=scripts/lib/compose.sh
. "$(dirname "$0")/lib/compose.sh"

# The compose files in effect. Every invocation goes through compose(), so
# the TxnKV override is applied to the switch, the readiness check and the
# test run alike (issue #514).
COMPOSE_FILES=()
compose() {
    "${COMPOSE[@]}" "${COMPOSE_FILES[@]}" "$@"
}

# shellcheck source=scripts/lib/wait-for-cluster.sh
. "$(dirname "$0")/lib/wait-for-cluster.sh"

# Function to cleanup. On a failed run the cluster state and the container
# logs are printed before the volumes go away: a `down -v` would otherwise
# leave nothing to debug a mid-run failure with (issue #514).
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
    local status=$?
    echo ""
    if [ "$status" -ne 0 ]; then
        echo -e "${YELLOW}Exit code $status: dumping cluster state before teardown...${NC}"
        compose ps || true
        compose logs --tail=100 || true
    fi
    echo -e "${YELLOW}Cleaning up...${NC}"
    compose down -v --remove-orphans 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# An interrupted run must still dump the diagnostics: without these the EXIT
# trap sees status 0 and goes straight to `down -v`, erasing the artifacts.
trap 'exit 130' INT
trap 'exit 143' TERM

echo -e "${YELLOW}Step 1: Starting TiKV cluster...${NC}"
compose up -d pd tikv1 tikv2 tikv3

echo ""
echo -e "${YELLOW}Step 2: Waiting for cluster to be ready...${NC}"
# A healthy PD is not enough: the test container reaches the stores by their
# Compose service names, so every container has to be running and PD has to
# see all three stores Up before the first test starts (issue #514).
wait_for_cluster "cluster"

echo ""
echo -e "${YELLOW}Step 3: Installing dependencies...${NC}"
compose run --rm --no-deps php-client composer install --no-interaction

echo ""
echo -e "${YELLOW}Step 4: Running RawKV E2E tests (V1ttl cluster)...${NC}"
# --fail-on-empty-test-suite: a selection that matches no test must be a
# failure, not a green run that tested nothing (issue #361).
RAWKV_EXIT_CODE=0
compose run --rm --no-deps -e PD_ENDPOINTS=pd:2379 php-client vendor/bin/phpunit --testsuite E2E-RawKV --testdox --fail-on-empty-test-suite --no-coverage || RAWKV_EXIT_CODE=$?

if [ $RAWKV_EXIT_CODE -ne 0 ]; then
    # cleanup happens via the EXIT trap
    exit $RAWKV_EXIT_CODE
fi

echo ""
echo -e "${YELLOW}Step 5: Switching to V1 cluster (txnkv override) for TxnKV E2E tests...${NC}"
# TiKV cannot switch API mode on an existing volume, so the V1TTL volumes
# have to go before the V1 cluster starts.
COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.txnkv.yml)
compose down -v --remove-orphans 2>/dev/null || true
compose up -d pd tikv1 tikv2 tikv3

echo ""
echo -e "${YELLOW}Waiting for V1 cluster to be ready...${NC}"
wait_for_cluster "V1 cluster"

echo ""
echo -e "${YELLOW}Step 6: Running TxnKV E2E tests (V1 cluster)...${NC}"
TEST_EXIT_CODE=0
compose run --rm --no-deps \
    -e PD_ENDPOINTS=pd:2379 php-client vendor/bin/phpunit --testsuite E2E-TxnKV --testdox --fail-on-empty-test-suite --no-coverage || TEST_EXIT_CODE=$?

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}==========================================${NC}"
    echo -e "${GREEN}All E2E tests passed!${NC}"
    echo -e "${GREEN}==========================================${NC}"
else
    echo -e "${RED}==========================================${NC}"
    echo -e "${RED}Some tests failed!${NC}"
    echo -e "${RED}==========================================${NC}"
fi

exit $TEST_EXIT_CODE
