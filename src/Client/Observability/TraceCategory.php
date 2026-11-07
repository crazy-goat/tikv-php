<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Observability;

/**
 * Categories carried by {@see TraceEventInterface::traceEvent()}.
 *
 * The set mirrors client-go's structured trace-event layer (issue #434,
 * GAP-20), so a bridge written for one client family maps naturally onto the
 * other.
 */
final class TraceCategory
{
    /** One-phase/two-phase commit: prewrite and commit phases. */
    public const TXN_2PC = 'txn_2pc';

    /** Lock resolution: status checks, resolve RPCs and their outcome. */
    public const TXN_LOCK_RESOLVE = 'txn_lock_resolve';

    /** An outbound KV or PD gRPC call. */
    public const KV_REQUEST = 'kv_request';

    /**
     * Region cache lookup, miss and invalidation events.
     *
     * Reserved: the constant mirrors client-go's `CategoryRegionCache`, but
     * the region cache does not emit trace events yet.
     */
    public const REGION_CACHE = 'region_cache';

    private function __construct()
    {
    }
}
