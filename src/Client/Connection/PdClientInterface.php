<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
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
     * @param int|null $timeoutMs Deadline in milliseconds for the `GetRegion`
     *                            RPC; null means the configured
     *                            TimeoutConfig::$pdTimeoutMs (issue #257)
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error, or when PD returns no region
     */
    public function getRegion(string $key, ?int $timeoutMs = null): RegionInfo;

    /**
     * Get the region immediately before the region that contains the given
     * key.
     *
     * PD's `GetPrevRegion` is the backwards counterpart of
     * {@see self::getRegion()}: it resolves the adjacent region whose end key
     * is the start key of the region containing `$key`. A reverse scan uses
     * it to walk a range from its upper bound downwards without enumerating
     * the whole range up front (issue #198). Returns null when there is no
     * adjacent region below — the key is in the first region of the keyspace,
     * or PD reports a gap.
     *
     * @param int|null $timeoutMs Deadline in milliseconds for the
     *                            `GetPrevRegion` RPC; null means the configured
     *                            TimeoutConfig::$pdTimeoutMs (issue #257)
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getPrevRegion(string $key, ?int $timeoutMs = null): ?RegionInfo;

    /**
     * Resolve a keyspace name to its numeric ID through PD.
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error or an empty keyspace response
     */
    public function getKeyspaceId(string $name): int;

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
     * @param int|null $timeoutMs Deadline in milliseconds for the
     *                            `ScanRegions` RPC; null means the configured
     *                            TimeoutConfig::$pdTimeoutMs (issue #257)
     * @return RegionInfo[]
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function scanRegions(string $startKey, string $endKey, int $limit = 0, ?int $timeoutMs = null): array;

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
     * Since issue #292 the implementation serves timestamps from a small
     * per-client pool (one `Tso` RPC grants `tsoPoolSize` consecutive
     * values), so consecutive calls usually do not each perform a round
     * trip. Returned values remain strictly increasing. The pool is
     * discarded on a PD error, a cluster-ID change, after a fork, or when
     * it outlives its physical window; a TSO failure still fails closed.
     *
     * @param int|null $timeoutMs Deadline in milliseconds for the `Tso` RPC; null
     *                            means the configured TimeoutConfig::$tsoTimeoutMs (#260)
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getTimestamp(?int $timeoutMs = null): int;

    /**
     * Get a batch of monotonically increasing timestamps from PD in a
     * single TSO RPC (issue #420).
     *
     * @param int $count number of timestamps to request (>= 1 and
     *                   <= {@see TimestampOracle::MAX_TIMESTAMP_POOL_SIZE})
     * @param int|null $timeoutMs Deadline in milliseconds for the `Tso` RPC; null
     *                            means the configured TimeoutConfig::$tsoTimeoutMs (#260)
     *
     * @return list<int> at most $count monotonically increasing timestamps
     *
     * @throws InvalidArgumentException When $count is out of range
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getTimestampBatch(int $count, ?int $timeoutMs = null): array;

    /**
     * Get a timestamp that is at most the configured
     * lowResTimestampMaxStalenessMs old (issue #420).
     *
     * With no staleness bound configured this performs a fresh TSO RPC per
     * call (it intentionally bypasses the pooled {@see getTimestamp()});
     * with a bound set, repeated calls within the bound reuse the cached
     * timestamp. Intended for staleness-tolerant consumers such as lock
     * resolution — never for start/commit timestamps.
     *
     * @param int|null $timeoutMs Deadline in milliseconds for the `Tso` RPC; null
     *                            means the configured TimeoutConfig::$tsoTimeoutMs (#260)
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getLowResolutionTimestamp(?int $timeoutMs = null): int;

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
     *
     * PD cluster IDs are `uint64`. The ID is returned as an `int` while it
     * fits in `PHP_INT_MAX`; an ID above that bound is returned as its
     * canonical unsigned decimal string (for example
     * `"18446744073709551615"`), because a PHP int cannot represent it
     * (issue #818). Callers that need a string can cast with
     * `(string) $id`.
     */
    public function getClusterId(): int|string|null;

    /**
     * Probe PD connectivity and learn the cluster ID.
     *
     * Issues a `GetMembers` RPC and returns the learned cluster ID, or null
     * if the response carried no cluster ID header or reported `0`. When an
     * expected cluster ID is configured, a response without a cluster ID
     * header throws instead of returning null. Suitable as a health check: no
     * user data is looked up and no "no region" failure can occur.
     *
     * The return value follows {@see self::getClusterId()}: an int while the
     * ID fits, the canonical unsigned decimal string above `PHP_INT_MAX`
     * (issue #818).
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD-level error
     */
    public function ping(): int|string|null;

    /**
     * Set the cluster ID (learned from PD response headers).
     *
     * Called by TimestampOracle and PdClient itself when the cluster ID
     * is discovered during a cluster-id mismatch retry or from a response
     * header. Idempotent for the same value. When the client was built with
     * an expected cluster ID (issue #308, SEC-05), a different value throws a
     * {@see TiKvException} instead of replacing the configured identity; a
     * value that differs from an already-established ID throws too, and a
     * non-positive value (including the proto3 default `0`) is rejected
     * (issue #250, REG-19).
     *
     * Accepts a positive int or the canonical unsigned decimal string, so a
     * uint64 above `PHP_INT_MAX` can be set (issue #818).
     *
     * @throws InvalidArgumentException when $clusterId is not a positive
     *         uint64 value
     * @throws TiKvException when $clusterId differs from the expected or the
     *         already-established cluster ID
     */
    public function setClusterId(int|string $clusterId): void;

    /**
     * Close the PD connection and release resources.
     */
    public function close(): void;
}
