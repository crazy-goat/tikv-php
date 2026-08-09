#!/bin/bash
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

# Function to cleanup
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker-compose down -v --remove-orphans 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo -e "${YELLOW}Step 1: Starting TiKV cluster...${NC}"
docker-compose up -d pd tikv1 tikv2 tikv3

echo ""
echo -e "${YELLOW}Step 2: Waiting for cluster to be ready...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose ps | grep -q "healthy"; then
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
docker-compose run --rm php-client composer install --no-interaction

echo ""
echo -e "${YELLOW}Step 4: Running RawKV E2E tests (V1ttl cluster)...${NC}"
docker-compose run --rm -e PD_ENDPOINTS=pd:2379 php-client vendor/bin/phpunit --testsuite E2E-RawKV --testdox --no-coverage

RAWKV_EXIT_CODE=$?

if [ $RAWKV_EXIT_CODE -ne 0 ]; then
    # cleanup happens via the EXIT trap
    exit $RAWKV_EXIT_CODE
fi

echo ""
echo -e "${YELLOW}Step 5: Switching to V1 cluster (txnkv override) for TxnKV E2E tests...${NC}"
docker-compose down -v --remove-orphans 2>/dev/null || true
docker-compose -f docker-compose.yml -f docker-compose.txnkv.yml up -d pd tikv1 tikv2 tikv3

MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose -f docker-compose.yml -f docker-compose.txnkv.yml ps | grep -q "healthy"; then
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
docker-compose -f docker-compose.yml -f docker-compose.txnkv.yml run --rm --no-deps \
    -e PD_ENDPOINTS=pd:2379 php-client vendor/bin/phpunit --testsuite E2E-TxnKV --testdox --no-coverage

TEST_EXIT_CODE=$?

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
