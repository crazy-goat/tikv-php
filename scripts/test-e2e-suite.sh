#!/bin/bash
#
# Runs one end-to-end suite on the host, against a cluster that is already
# up. The per-mode composer scripts call it:
#
#   composer test:e2e:raw   -> scripts/test-e2e-suite.sh E2E-RawKV
#   composer test:e2e:txn   -> scripts/test-e2e-suite.sh E2E-TxnKV
#   composer test:e2e:apiv2 -> scripts/test-e2e-suite.sh E2E-ApiV2
#
# The suites reach PD through the PD_ENDPOINTS environment variable. Its
# default, "pd:2379", is a Docker Compose service name: it resolves only
# inside the compose network, so a run on the host needs the published PD
# port (issue #361).
#
# The script fails loudly instead of reporting a green run that tested
# nothing: a selection that matches no test is a non-zero exit code
# (--fail-on-empty-test-suite), and the API V2 lane cannot skip its whole
# suite, because the environment it needs is set here.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1

YELLOW='\033[1;33m'
NC='\033[0m'

suite="${1:-}"

usage() {
    echo "Usage: $0 <E2E-RawKV|E2E-TxnKV|E2E-ApiV2>" >&2
    echo "The suite needs a running TiKV cluster; see docs/contributing.md." >&2
}

case "$suite" in
    E2E-RawKV)
        phpunit_args=(--testsuite E2E-RawKV)
        ;;
    E2E-TxnKV)
        phpunit_args=(--testsuite E2E-TxnKV)
        ;;
    E2E-ApiV2)
        # The API V2 suite skips every one of its tests unless
        # TIKV_API_VERSION is 2, and a skipped test counts as a test, so
        # --fail-on-empty-test-suite does not fire: the lane would be green
        # having tested nothing. The environment is set here, and
        # --fail-on-skipped turns a leftover skip into a failure.
        export TIKV_API_VERSION="${TIKV_API_VERSION:-2}"
        phpunit_args=(--testsuite E2E-ApiV2 --fail-on-skipped)

        if [ -z "${TIKV_ALT_KEYSPACE:-}" ]; then
            echo "TIKV_ALT_KEYSPACE is not set, but the API V2 suite needs a second" >&2
            echo "keyspace to prove that the same user key stays isolated per" >&2
            echo "keyspace. Create it in the cluster and run the suite again:" >&2
            echo "" >&2
            echo "  docker compose exec -T pd wget -qO- \\" >&2
            echo "    --header='Content-Type: application/json' \\" >&2
            echo "    --post-data='{\"name\":\"php-e2e-alt\"}' \\" >&2
            echo "    http://127.0.0.1:2379/pd/api/v2/keyspaces" >&2
            echo "" >&2
            echo "  TIKV_ALT_KEYSPACE=php-e2e-alt composer test:e2e:apiv2" >&2
            exit 2
        fi
        ;;
    *)
        usage
        exit 2
        ;;
esac

if [ -z "${PD_ENDPOINTS:-}" ]; then
    echo -e "${YELLOW}PD_ENDPOINTS is not set: the suite will use \"pd:2379\".${NC}"
    echo "\"pd:2379\" is a Docker Compose service name and resolves only inside the" >&2
    echo "compose network. On the host, point PD_ENDPOINTS at the published PD" >&2
    echo "port, for example \"PD_ENDPOINTS=localhost:${PD_PORT:-2379}\"." >&2
    echo "" >&2
fi

phpunit_command=(vendor/bin/phpunit "${phpunit_args[@]}" --fail-on-empty-test-suite --no-coverage)

echo "Running: ${phpunit_command[*]}"
echo "PD_ENDPOINTS: ${PD_ENDPOINTS:-pd:2379}"

exec "${phpunit_command[@]}"
