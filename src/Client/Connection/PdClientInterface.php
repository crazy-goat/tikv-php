<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;

interface PdClientInterface
{
    /**
     * Get the region that contains the given key.
     *
     * Fails closed when PD returns no region for the key: a fabricated
     * regionId/leaderStoreId would be cached and silently misroute
     * requests. Throws a {@see TiKvException} so the failure is visible
     * rather than corrupting the region cache.
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error, or when PD returns no region
     */
    public function getRegion(string $key): RegionInfo;

    /**
     * Get store metadata by ID.
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getStore(int $storeId): ?Store;

    /**
     * Scan all regions covering the key range [startKey, endKey).
     *
     * @return RegionInfo[]
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function scanRegions(string $startKey, string $endKey, int $limit = 0): array;

    /**
     * Get all stores from PD.
     *
     * @return Store[]
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getAllStores(): array;

    /**
     * Get a monotonically increasing timestamp from PD.
     *
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds (null = no timeout)
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getTimestamp(?int $timeoutMs = null): int;

    /**
     * Fetch the cluster's current GC safe point.
     *
     * Issues a `GetGCSafePoint` RPC and returns the safe point timestamp:
     * any transaction whose start timestamp is below this value can no
     * longer read and will be rejected by TiKV with a "GC life time is
     * shorter than transaction duration" error. Used by the safe-point
     * validation cache (issue #422).
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getGCSafePoint(): int;

    /**
     * Register or refresh a service GC safe point with PD.
     *
     * A long-running read (large scan, report job, batch export) can hold
     * GC back for its duration by registering a service safe point with a
     * bounded TTL: GC will not advance past the returned min safe point
     * while the registration is refreshed before expiry. Pass
     * $ttlSeconds <= 0 to REMOVE the registration for this $serviceId (PD's
     * UpdateServiceGCSafePoint deletes the service's safe point whenever
     * the TTL is zero or negative) — for a removal the $safePoint value is
     * ignored by PD, so callers should pass 0. A lapsed TTL also releases
     * the hold automatically; the explicit removal is for orderly shutdown.
     *
     * Returns the resulting min safe point across all registered services
     * (the value GC is actually held at), or null when the cluster's PD
     * does not support service safe points (older PD, or GC v1 without
     * service safe-point support — the header error is inspected for
     * "not supported"/"unknown method" style rejections).
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error (other than unsupported feature)
     */
    public function updateServiceGCSafePoint(string $serviceId, int $safePoint, int $ttlSeconds): ?int;

    /**
     * Get the learned cluster ID, or null if not yet discovered.
     */
    public function getClusterId(): ?int;

    /**
     * Probe PD connectivity and learn the cluster ID.
     *
     * Issues a `GetMembers` RPC and returns the learned cluster ID, or null
     * if the response carried no cluster ID header. Suitable as a health
     * check: no user data is looked up and no "no region" failure can occur.
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD-level error
     */
    public function ping(): ?int;

    /**
     * Set the cluster ID (learned from PD response headers).
     *
     * Called by TimestampOracle and PdClient itself when the cluster ID
     * is discovered during a cluster-id mismatch retry or from a response
     * header. Idempotent.
     */
    public function setClusterId(int $clusterId): void;

    /**
     * Close the PD connection and release resources.
     */
    public function close(): void;
}
