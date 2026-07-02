<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;

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
     * Close the PD connection and release resources.
     */
    public function close(): void;
}
