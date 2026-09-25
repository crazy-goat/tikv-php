#!/bin/bash
#
# API V2 E2E lane.
#
# Runs the E2E-ApiV2 suite against a cluster whose TiKV nodes run with
# storage.api-version = 2, which the default docker-compose.yml cluster does
# not. Kept separate from scripts/test-e2e.sh so the V1 suites keep running
# against a V1 cluster.
set -euo pipefail

COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.apiv2.yml)

# A second, non-default keyspace: the suite proves that identical user keys
# stay isolated once the keyspace ID becomes part of the encoded key.
PRIMARY_KEYSPACE="${TIKV_KEYSPACE:-php-e2e-primary}"
ALT_KEYSPACE="${TIKV_ALT_KEYSPACE:-php-e2e-alt}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

compose() {
    docker compose "${COMPOSE_FILES[@]}" --profile test "$@"
}

cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    compose down -v --remove-orphans 2>/dev/null || true
}

create_keyspace() {
    # PD exposes keyspace creation over HTTP only; LoadKeyspace (the gRPC
    # call the client uses) only reads an existing keyspace. PD answers 500
    # when it cannot split regions for the new keyspace, which in practice
    # means not all stores are up yet — so let the body reach the log.
    compose exec -T pd wget -qO- \
        --header='Content-Type: application/json' \
        --post-data="{\"name\":\"$1\"}" \
        http://127.0.0.1:2379/pd/api/v2/keyspaces
}

up_store_count() {
    # PD pretty-prints its JSON, so the count is taken on whitespace-free
    # input; each store entry carries exactly one state_name.
    compose exec -T pd wget -qO- http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null |
        tr -d ' \n' | grep -o '"state_name":"Up"' | wc -l
}

trap cleanup EXIT

echo -e "${YELLOW}Step 1: Starting API V2 TiKV cluster...${NC}"
compose up -d pd tikv1 tikv2 tikv3

echo ""
echo -e "${YELLOW}Step 2: Waiting for cluster to be ready...${NC}"
RETRIES=90
COUNT=0
while [ "$COUNT" -lt "$RETRIES" ]; do
    # Both conditions matter: PD's healthcheck only covers PD itself, and a
    # test that starts before the stores register would fail region lookups.
    if compose ps --format '{{.Service}} {{.Health}}' 2>/dev/null | grep -q '^pd healthy$' &&
        [ "$(up_store_count)" -ge 3 ]; then
        break
    fi
    sleep 2
    COUNT=$((COUNT + 1))
done
if [ "$COUNT" -eq "$RETRIES" ]; then
    echo "Timeout waiting for cluster"
    compose ps
    compose logs --tail=50
    exit 1
fi
echo -e "${GREEN}Cluster is ready!${NC}"

echo ""
echo -e "${YELLOW}Step 3: Installing dependencies...${NC}"
compose run --rm --no-deps php-client composer install --no-interaction --prefer-dist --no-progress

echo ""
echo -e "${YELLOW}Step 4: Creating keyspaces '${PRIMARY_KEYSPACE}' and '${ALT_KEYSPACE}'...${NC}"
create_keyspace "$PRIMARY_KEYSPACE"
create_keyspace "$ALT_KEYSPACE"

echo ""
echo -e "${YELLOW}Step 5: Running API V2 E2E tests...${NC}"
TEST_EXIT_CODE=0
compose run --rm --no-deps \
    -e PD_ENDPOINTS=pd:2379 \
    -e TIKV_API_VERSION=2 \
    -e TIKV_KEYSPACE="$PRIMARY_KEYSPACE" \
    -e TIKV_ALT_KEYSPACE="$ALT_KEYSPACE" \
    php-test vendor/bin/phpunit --testsuite E2E-ApiV2 --testdox --no-coverage ||
    TEST_EXIT_CODE=$?

echo ""
if [ "$TEST_EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}==========================================${NC}"
    echo -e "${GREEN}All API V2 E2E tests passed!${NC}"
    echo -e "${GREEN}==========================================${NC}"
else
    echo "Some API V2 E2E tests failed!"
fi

exit "$TEST_EXIT_CODE"
