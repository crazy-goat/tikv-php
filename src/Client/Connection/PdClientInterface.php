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
     * Get a monotonically increasing timestamp from PD.
     *
     * @throws GrpcException On transport error
     * @throws TiKvException On PD error
     */
    public function getTimestamp(): int;

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
