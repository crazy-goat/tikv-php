#!/bin/bash
#
# Cluster readiness for the end-to-end runners (issue #514).
#
# A healthy PD container is not enough: the test container reaches the TiKV
# stores by their Compose service names (tikv1:20160, ...), and a run that
# starts while a store is still booting fails with "Name does not resolve
# tikv1:20160" (docs/helpers/faq.md). Readiness therefore means every service
# container is running *and* PD has every TiKV store registered as Up.
#
# The sourcing script provides a `compose` function (see
# scripts/lib/compose.sh) and may override TIKV_SERVICE_COUNT or
# TIKV_STORE_COUNT for a different topology:
#
#   compose() { "${COMPOSE[@]}" "$@"; }
#   . "$(dirname "$0")/lib/wait-for-cluster.sh"
#   wait_for_cluster "cluster"

TIKV_SERVICE_COUNT="${TIKV_SERVICE_COUNT:-3}"
TIKV_STORE_COUNT="${TIKV_STORE_COUNT:-3}"

CLUSTER_SERVICES=(pd)
for ((store = 1; store <= TIKV_SERVICE_COUNT; store++)); do
    CLUSTER_SERVICES+=("tikv${store}")
done

# The number of TiKV stores PD reports as Up. An unreachable PD answers zero.
up_store_count() {
    compose exec -T pd wget -qO- http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null |
        tr -d ' \n' | grep -o '"state_name":"Up"' | wc -l | tr -d ' '
}

# True when every service container is running and PD sees every store Up.
# A `compose ps` failure (no cluster yet) means "not ready", not an error.
cluster_ready() {
    local states service
    states=$(compose ps --format '{{.Service}} {{.State}}' 2>/dev/null) || return 1

    for service in "${CLUSTER_SERVICES[@]}"; do
        if ! grep -qx "$service running" <<<"$states"; then
            return 1
        fi
    done

    [ "$(up_store_count)" -ge "$TIKV_STORE_COUNT" ]
}

# Waits until cluster_ready passes, then prints the ready message. Returns 1
# on timeout; the caller's EXIT trap is responsible for the diagnostics.
wait_for_cluster() {
    local label="${1:-cluster}"
    local retries="${2:-90}"
    local count=0

    while [ "$count" -lt "$retries" ]; do
        if cluster_ready; then
            echo -e "${GREEN:-}${label} is ready!${NC:-}"
            return 0
        fi

        echo "Waiting for ${label}... ($count/$retries)"
        sleep 2
        count=$((count + 1))
    done

    echo -e "${RED:-}Timeout waiting for ${label}${NC:-}"
    return 1
}
