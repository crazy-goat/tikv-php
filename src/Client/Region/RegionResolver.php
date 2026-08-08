<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use Closure;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class RegionResolver
{
    /**
     * @param string[] $allowedStoreHosts Hostnames, DNS suffixes (leading dot
     *     optional) or CIDR ranges the store host must match. Empty when no
     *     host restriction is configured (backward compatible).
     * @param (Closure(string): bool)|null $storeHostPolicy Custom policy that
     *     receives the full address; when set it overrides $allowedStoreHosts.
     */
    public function __construct(
        private PdClientInterface $pdClient,
        private RegionCacheInterface $regionCache,
        private MetricsInterface $metrics = new NoOpMetrics(),
        private array $allowedStoreHosts = [],
        private ?Closure $storeHostPolicy = null,
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function getRegionInfo(string $key): RegionInfo
    {
        $region = $this->regionCache->getByKey($key);
        if ($region instanceof RegionInfo) {
            $this->metrics->regionCacheHit('region_resolution');

            return $region;
        }

        $this->metrics->regionCacheMiss('region_resolution');
        $region = $this->pdClient->getRegion($key);
        $this->regionCache->put($region);

        return $region;
    }

    /**
     * Resolve regions for a batch of keys using a single scanRegions() call
     * instead of one getRegion() per key. Populates the cache as a side effect.
     *
     * @param string[] $keys
     * @return array<string, RegionInfo> key => region mapping
     */
    public function batchResolveRegions(array $keys): array
    {
        if ($keys === []) {
            return [];
        }

        $sorted = $keys;
        sort($sorted, SORT_STRING);

        $minKey = $sorted[0];
        $maxKey = end($sorted);

        $regions = $this->pdClient->scanRegions($minKey, $maxKey);

        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }

        return $this->assignKeysToRegions($keys, $regions);
    }

    /**
     * Assign keys to regions using binary search on sorted region boundaries.
     *
     * @param string[] $keys
     * @param RegionInfo[] $regions regions sorted by startKey
     * @return array<string, RegionInfo>
     */
    private function assignKeysToRegions(array $keys, array $regions): array
    {
        if ($regions === []) {
            return [];
        }

        $result = [];
        foreach ($keys as $key) {
            $region = $this->findRegionForKey($key, $regions);
            if ($region instanceof RegionInfo) {
                $result[$key] = $region;
            }
        }

        return $result;
    }

    /**
     * Find the region containing the given key using binary search.
     *
     * @param RegionInfo[] $regions regions sorted by startKey
     */
    private function findRegionForKey(string $key, array $regions): ?RegionInfo
    {
        $left = 0;
        $right = count($regions) - 1;
        $result = null;

        while ($left <= $right) {
            $mid = (int) (($left + $right) / 2);
            $region = $regions[$mid];

            if ($region->startKey <= $key) {
                $result = $region;
                $left = $mid + 1;
            } else {
                $right = $mid - 1;
            }
        }

        if ($result !== null && $result->endKey !== '' && $key >= $result->endKey) {
            return null;
        }

        return $result;
    }

    public function resolveStoreAddress(int $storeId): string
    {
        $store = $this->pdClient->getStore($storeId);
        if (!$store instanceof Store) {
            throw new StoreNotFoundException($storeId);
        }

        $address = $store->getAddress();
        if ($address === '') {
            throw new StoreNotFoundException($storeId);
        }

        // Unconditional format check: gRPC target strings are not restricted
        // to host:port — grpc-core also accepts unix:/path, unix-abstract:,
        // dns:/// and ipv4:/ipv6: schemes. A rogue/on-path PD must never be
        // able to redirect the client to an arbitrary target, so anything
        // that is not a bare host:port is rejected regardless of policy.
        if (preg_match('/^[A-Za-z0-9._-]+:\d{1,5}$/', $address) !== 1) {
            $this->logger->error('PD returned a store address that is not a bare host:port', [
                'storeId' => $storeId,
                'address' => $address,
            ]);
            throw new InvalidStoreAddressException(sprintf(
                'PD returned malformed store address "%s" for store %d (expected host:port)',
                $address,
                $storeId,
            ));
        }

        if (!$this->isStoreAddressAllowed($address)) {
            $this->logger->error('PD returned a store address outside the allowed set', [
                'storeId' => $storeId,
                'address' => $address,
                'allowedStoreHosts' => $this->allowedStoreHosts,
            ]);
            throw new InvalidStoreAddressException(sprintf(
                'PD returned store address "%s" for store %d outside the allowed set',
                $address,
                $storeId,
            ));
        }

        return $address;
    }

    /**
     * Decide whether a validated host:port address may be connected to.
     */
    private function isStoreAddressAllowed(string $address): bool
    {
        if ($this->storeHostPolicy instanceof \Closure) {
            return (bool) ($this->storeHostPolicy)($address);
        }

        if ($this->allowedStoreHosts === []) {
            return true;
        }

        $host = strstr($address, ':', true);
        if ($host === false) {
            return false;
        }

        foreach ($this->allowedStoreHosts as $entry) {
            if ($this->matchesHostEntry($host, $entry)) {
                return true;
            }
        }

        return false;
    }

    private function matchesHostEntry(string $host, string $entry): bool
    {
        if (str_contains($entry, '/')) {
            return $this->matchesCidr($host, $entry);
        }

        // DNS suffix: entry may be written as 'example.com' or '.example.com';
        // both match the domain itself and any subdomain.
        $suffix = ltrim($entry, '.');

        return $host === $suffix || str_ends_with($host, '.' . $suffix);
    }

    private function matchesCidr(string $host, string $cidr): bool
    {
        $parts = explode('/', $cidr, 2);
        if (count($parts) !== 2 || $parts[1] === '' || !ctype_digit($parts[1])) {
            return false;
        }

        $prefixLength = (int) $parts[1];
        $packedHost = @inet_pton($host);
        $packedNetwork = @inet_pton($parts[0]);
        if ($packedHost === false || $packedNetwork === false || strlen($packedHost) !== strlen($packedNetwork)) {
            return false;
        }

        $totalBits = strlen($packedHost) * 8;
        if ($prefixLength > $totalBits) {
            return false;
        }

        $fullBytes = intdiv($prefixLength, 8);
        if (substr($packedHost, 0, $fullBytes) !== substr($packedNetwork, 0, $fullBytes)) {
            return false;
        }

        $remainderBits = $prefixLength % 8;
        if ($remainderBits > 0) {
            $mask = chr((0xFF << (8 - $remainderBits)) & 0xFF);
            $hostByte = substr($packedHost, $fullBytes, 1);
            $networkByte = substr($packedNetwork, $fullBytes, 1);
            if (($hostByte & $mask) !== ($networkByte & $mask)) {
                return false;
            }
        }

        return true;
    }
}
