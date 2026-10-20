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

# Function to cleanup
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    "${COMPOSE[@]}" down -v --remove-orphans 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo -e "${YELLOW}Step 1: Starting TiKV cluster...${NC}"
"${COMPOSE[@]}" up -d pd tikv1 tikv2 tikv3

echo ""
echo -e "${YELLOW}Step 2: Waiting for cluster to be ready...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if "${COMPOSE[@]}" ps | grep -q "healthy"; then
        echo -e "${GREEN}Cluster is ready!${NC}"
        break
    fi
    echo "Waiting for cluster... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}Timeout waiting for cluster${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}Step 3: Installing dependencies...${NC}"
"${COMPOSE[@]}" run --rm php-client composer install --no-interaction

echo ""
echo -e "${YELLOW}Step 4: Running RawKV E2E tests (V1ttl cluster)...${NC}"
# --fail-on-empty-test-suite: a selection that matches no test must be a
# failure, not a green run that tested nothing (issue #361).
RAWKV_EXIT_CODE=0
"${COMPOSE[@]}" run --rm -e PD_ENDPOINTS=pd:2379 php-client vendor/bin/phpunit --testsuite E2E-RawKV --testdox --fail-on-empty-test-suite --no-coverage || RAWKV_EXIT_CODE=$?

if [ $RAWKV_EXIT_CODE -ne 0 ]; then
    # cleanup happens via the EXIT trap
    exit $RAWKV_EXIT_CODE
fi

echo ""
echo -e "${YELLOW}Step 5: Switching to V1 cluster (txnkv override) for TxnKV E2E tests...${NC}"
"${COMPOSE[@]}" down -v --remove-orphans 2>/dev/null || true
"${COMPOSE[@]}" -f docker-compose.yml -f docker-compose.txnkv.yml up -d pd tikv1 tikv2 tikv3

MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if "${COMPOSE[@]}" -f docker-compose.yml -f docker-compose.txnkv.yml ps | grep -q "healthy"; then
        echo -e "${GREEN}V1 cluster is ready!${NC}"
        break
    fi
    echo "Waiting for V1 cluster... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}Timeout waiting for V1 cluster${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}Step 6: Running TxnKV E2E tests (V1 cluster)...${NC}"
TEST_EXIT_CODE=0
"${COMPOSE[@]}" -f docker-compose.yml -f docker-compose.txnkv.yml run --rm --no-deps \
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
