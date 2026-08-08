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
     * @param string[] $allowedStoreHosts Exact hostnames, DNS suffixes
     *     (leading dot: matches the domain itself and any subdomain) or
     *     CIDR ranges the store host must match.
     * @param (Closure(string): bool)|null $storeHostPolicy Custom policy that
     *     receives the full address; when set it overrides $allowedStoreHosts
     *     and the default PD-derived policy.
     * @param list<string> $pdEndpoints Configured PD endpoints the default
     *     host policy is derived from. Only used when neither
     *     $allowedStoreHosts nor $storeHostPolicy is set. Empty only for
     *     direct construction (permissive: no default policy applies).
     */
    public function __construct(
        private PdClientInterface $pdClient,
        private RegionCacheInterface $regionCache,
        private MetricsInterface $metrics = new NoOpMetrics(),
        private array $allowedStoreHosts = [],
        private ?Closure $storeHostPolicy = null,
        private LoggerInterface $logger = new NullLogger(),
        private array $pdEndpoints = [],
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

        $this->validateStoreAddress($address, $storeId);

        return $address;
    }

    /**
     * Validate a PD-supplied store address before it is used as a gRPC
     * channel target (used by resolveStoreAddress() and by callers that
     * handle store addresses directly, e.g. SstIngestor). Enforces the
     * unconditional host:port format check (bracketed IPv6 allowed) and the
     * configured host policy.
     *
     * @throws InvalidStoreAddressException when the address is malformed or
     *     outside the allowed set
     */
    public function validateStoreAddress(string $address, int $storeId): void
    {
        $parsed = $this->parseHostPort($address);
        if ($parsed === null) {
            $this->logger->error('PD returned a store address that is not a bare host:port', [
                'storeId' => $storeId,
                'address' => $address,
            ]);
            throw new InvalidStoreAddressException(sprintf(
                'PD returned malformed store address "%s" for store %d (expected host:port, port 1-65535)',
                $address,
                $storeId,
            ));
        }

        if (!$this->isStoreAddressAllowed($address, $parsed['host'])) {
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
    }

    /**
     * Parse a bare host:port or bracketed IPv6 [addr]:port string. The
     * format check is unconditional and independent of the host policy.
     *
     * @return array{host: string, port: int}|null null when the address is
     *     not a valid bare host:port (schemes such as unix:, dns:/// and
     *     trailing-newline / out-of-range-port variants are rejected)
     */
    private function parseHostPort(string $address): ?array
    {
        if ($address === '') {
            return null;
        }

        if ($address[0] === '[') {
            $close = strpos($address, ']');
            if ($close === false || $close < 2 || !isset($address[$close + 1]) || $address[$close + 1] !== ':') {
                return null;
            }

            $host = substr($address, 1, $close - 1);
            $portPart = substr($address, $close + 2);
            $packed = @inet_pton($host);
            if ($packed === false || strlen($packed) !== 16) {
                return null;
            }
        } else {
            $colon = strrpos($address, ':');
            if ($colon === false || $colon === 0 || $colon === strlen($address) - 1) {
                return null;
            }

            $host = substr($address, 0, $colon);
            $portPart = substr($address, $colon + 1);
            if (preg_match('/\A[A-Za-z0-9._-]+\z/', $host) !== 1) {
                return null;
            }
        }

        if (preg_match('/\A[0-9]+\z/', $portPart) !== 1) {
            return null;
        }

        $port = (int) $portPart;
        if ($port < 1 || $port > 65535) {
            return null;
        }

        return ['host' => $host, 'port' => $port];
    }

    /**
     * Decide whether a validated host:port address may be connected to.
     */
    private function isStoreAddressAllowed(string $address, string $host): bool
    {
        if ($this->storeHostPolicy instanceof Closure) {
            return (bool) ($this->storeHostPolicy)($address);
        }

        if ($this->allowedStoreHosts !== []) {
            foreach ($this->allowedStoreHosts as $entry) {
                if ($this->matchesHostEntry($host, $entry)) {
                    return true;
                }
            }

            return false;
        }

        if ($this->pdEndpoints !== []) {
            return $this->matchesDefaultPolicy($host);
        }

        return true;
    }

    /**
     * Match a store host against a single allowlist entry. An entry with a
     * leading dot is a DNS suffix (matches the domain itself and any
     * subdomain); any other entry matches the hostname exactly.
     */
    private function matchesHostEntry(string $host, string $entry): bool
    {
        if (str_contains($entry, '/')) {
            return $this->matchesCidr($host, $entry);
        }

        if ($entry !== '' && $entry[0] === '.') {
            return $host === substr($entry, 1) || str_ends_with($host, $entry);
        }

        return $host === $entry;
    }

    /**
     * The default host policy, derived from the configured PD endpoints
     * (applied only when neither allowedStoreHosts nor storeHostPolicy is
     * configured): the store host must equal a configured PD host exactly,
     * be a single-label name (same network namespace, e.g. compose/K8s short
     * names), share the last two DNS labels with a configured PD host, or be
     * an IP in the same /16 subnet as a configured PD IP endpoint.
     */
    private function matchesDefaultPolicy(string $host): bool
    {
        if (!str_contains($host, '.')) {
            return true;
        }

        foreach ($this->pdEndpoints as $endpoint) {
            $parsed = $this->parseHostPort($endpoint);
            if ($parsed === null) {
                continue;
            }

            $pdHost = $parsed['host'];

            if (strcasecmp($host, $pdHost) === 0) {
                return true;
            }

            $pdLabels = explode('.', $pdHost);
            if (count($pdLabels) >= 3) {
                $sharedSuffix = '.' . implode('.', array_slice($pdLabels, -2));
                if (str_ends_with($host, $sharedSuffix)) {
                    return true;
                }
            }

            if ($this->isSameIpv16Subnet($host, $pdHost)) {
                return true;
            }
        }

        return false;
    }

    private function isSameIpv16Subnet(string $a, string $b): bool
    {
        $packedA = @inet_pton($a);
        $packedB = @inet_pton($b);
        if ($packedA === false || $packedB === false || strlen($packedA) !== strlen($packedB)) {
            return false;
        }

        return substr($packedA, 0, 2) === substr($packedB, 0, 2);
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
