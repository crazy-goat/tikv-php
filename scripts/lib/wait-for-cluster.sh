#!/bin/bash
#
# Cluster readiness for the end-to-end runners (issue #514, issue #713).
#
# A healthy PD container is not enough: the test container reaches the TiKV
# stores by their Compose service names (tikv1:20160, ...), and a run that
# starts while a store is still booting fails with "Name does not resolve
# tikv1:20160" (docs/helpers/faq.md). Readiness therefore means every service
# container is running *and* PD has every TiKV store registered as Up.
#
# Every store being Up is not enough either. A fresh cluster starts with one
# bootstrap region, splits it into the TiKV table regions a few seconds later
# and only then adds the remaining replicas to every region; the peer set is
# complete roughly ten seconds after the stores register (measured against
# TiKV 8.5.5). Each step bumps the region epoch, so a client that cached a
# region between two steps gets `EpochNotMatch` until its retry refreshes the
# entry — and a commit that straddles the bump is not retried, so the
# transaction fails (issue #713). Waiting until every region carries its full
# replica set (`TIKV_REGION_PEER_COUNT`, three on the default topology) means
# the bootstrap is over and the layout stops changing on its own.
#
# The sourcing script provides a `compose` function (see
# scripts/lib/compose.sh) and may override TIKV_SERVICE_COUNT,
# TIKV_STORE_COUNT or TIKV_REGION_PEER_COUNT for a different topology:
#
#   . "$(dirname "$0")/lib/compose.sh"
#   . "$(dirname "$0")/lib/wait-for-cluster.sh"
#   wait_for_cluster "cluster"
#
# The file can also be executed directly as a readiness gate, so a workflow
# that does not source the runner scripts reuses the same checks instead of
# copying them (issue #905):
#
#   COMPOSE_FILE=docker-compose.yml:docker-compose.txnkv.yml \
#       scripts/lib/wait-for-cluster.sh "V1 cluster"

TIKV_SERVICE_COUNT="${TIKV_SERVICE_COUNT:-3}"
TIKV_STORE_COUNT="${TIKV_STORE_COUNT:-3}"
TIKV_REGION_PEER_COUNT="${TIKV_REGION_PEER_COUNT:-$TIKV_STORE_COUNT}"

CLUSTER_SERVICES=(pd)
for ((store = 1; store <= TIKV_SERVICE_COUNT; store++)); do
    CLUSTER_SERVICES+=("tikv${store}")
done

# The number of TiKV stores PD reports as Up. An unreachable PD answers zero.
up_store_count() {
    compose exec -T pd wget -qO- http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null |
        tr -d ' \n' | grep -o '"state_name":"Up"' | wc -l | tr -d ' '
}

# The number of peers of the region that has the fewest, which is the size of
# the smallest replica set PD currently reports. An unreachable PD, or a PD
# that knows no region yet, answers nothing.
min_region_peer_count() {
    local regions peer_count minimum=''

    regions=$(compose exec -T pd wget -qO- http://127.0.0.1:2379/pd/api/v1/regions 2>/dev/null |
        tr -d ' \n' | grep -o '"peers":\[[^]]*\]') || return 1

    [ -n "$regions" ] || return 1

    while IFS= read -r region; do
        peer_count=$(grep -o '"store_id":' <<<"$region" | wc -l | tr -d ' ')

        if [ -z "$minimum" ] || [ "$peer_count" -lt "$minimum" ]; then
            minimum=$peer_count
        fi
    done <<<"$regions"

    printf '%s' "$minimum"
}

# True when every region carries its full replica set, the point after which
# a fresh cluster stops bumping region epochs on its own.
region_layout_settled() {
    local peer_count

    peer_count=$(min_region_peer_count) || return 1

    [ -n "$peer_count" ] || return 1
    [ "$peer_count" -ge "$TIKV_REGION_PEER_COUNT" ]
}

# True when every service container is running, PD sees every store Up and
# the region layout has settled. A `compose ps` failure (no cluster yet)
# means "not ready", not an error.
cluster_ready() {
    local services service
    services=$(compose_running_services 2>/dev/null) || return 1

    for service in "${CLUSTER_SERVICES[@]}"; do
        if ! grep -qx "$service" <<<"$services"; then
            return 1
        fi
    done

    [ "$(up_store_count)" -ge "$TIKV_STORE_COUNT" ] || return 1

    region_layout_settled
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

# Run directly (not sourced) as a readiness gate.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    # shellcheck source=scripts/lib/compose.sh
    . "$(dirname "$0")/compose.sh"
    wait_for_cluster "${1:-cluster}" "${2:-90}"
fi
