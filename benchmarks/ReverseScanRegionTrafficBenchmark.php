<?php

declare(strict_types=1);

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use Psr\Log\NullLogger;

require dirname(__DIR__) . '/vendor/autoload.php';

/**
 * Benchmark for issue #198 (RAW-13): the PD traffic of a narrow reverse scan
 * over a wide keyspace.
 *
 * Run from the repository root with:
 *   php benchmarks/ReverseScanRegionTrafficBenchmark.php
 *
 * The layout is {@see REGION_COUNT} single-row regions. The `before` column
 * reproduces the pre-#198 shape: `resolveScanRegions()` drained every batch of
 * `ScanRegions` over the whole remaining range before the reverse walk could
 * start. The `after` column is the shipped top-down walk: the region cache
 * answers the common case and a miss asks PD for one region at a time, so a
 * ten-row scan resolves only the regions it can consume.
 *
 * The benchmark measures client-issued PD calls and region descriptors, not
 * wall time: the numbers are the issue's acceptance criterion and do not
 * depend on the machine. Run it after a change to the reverse path to see the
 * ratio move.
 */

const REGION_COUNT = 5000;
const ROW_LIMIT = 10;

/** A fixed-width boundary so byte order and numeric order agree. */
$boundary = static fn (int $index): string => $index >= REGION_COUNT ? '' : sprintf('k%05d', $index);

/**
 * A PD double that answers region lookups from the layout and counts the
 * descriptors each path asks for.
 */
final class BenchPd implements PdClientInterface
{
    public int $getRegionCalls = 0;

    public int $getPrevRegionCalls = 0;

    public int $scanRegionsCalls = 0;

    public int $descriptors = 0;

    /**
     * @param list<RegionInfo> $regions
     */
    public function __construct(private readonly array $regions)
    {
    }

    public function reset(): void
    {
        $this->getRegionCalls = 0;
        $this->getPrevRegionCalls = 0;
        $this->scanRegionsCalls = 0;
        $this->descriptors = 0;
    }

    public function getRegion(string $key): RegionInfo
    {
        $this->getRegionCalls++;

        foreach ($this->regions as $region) {
            if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                return $region;
            }
        }
        foreach ($this->regions as $region) {
            if ($region->startKey === $key) {
                return $region;
            }
        }

        throw new RuntimeException('no region for key ' . bin2hex($key));
    }

    public function getPrevRegion(string $key): ?RegionInfo
    {
        $this->getPrevRegionCalls++;

        foreach ($this->regions as $region) {
            if ($region->endKey !== '' && KeyOrder::eq($region->endKey, $key)) {
                return $region;
            }
        }

        return null;
    }

    public function scanRegions(string $startKey, string $endKey, int $limit = 0): array
    {
        $this->scanRegionsCalls++;

        $matches = [];
        foreach ($this->regions as $region) {
            if ($region->endKey !== '' && KeyOrder::lte($region->endKey, $startKey)) {
                continue;
            }
            if ($endKey !== '' && KeyOrder::gte($region->startKey, $endKey)) {
                continue;
            }
            $matches[] = $region;
            if ($limit > 0 && count($matches) >= $limit) {
                break;
            }
        }

        $this->descriptors += count($matches);

        return $matches;
    }

    public function getKeyspaceId(string $name): int
    {
        throw new LogicException('not used by the benchmark');
    }

    public function getStore(int $storeId): ?Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress('store-1:20160');

        return $store;
    }

    public function getAllStores(): array
    {
        return [];
    }

    public function getTimestamp(?int $timeoutMs = null): int
    {
        throw new LogicException('not used by the benchmark');
    }

    public function getTimestampBatch(int $count, ?int $timeoutMs = null): array
    {
        throw new LogicException('not used by the benchmark');
    }

    public function getLowResolutionTimestamp(?int $timeoutMs = null): int
    {
        throw new LogicException('not used by the benchmark');
    }

    public function getGCSafePoint(): int
    {
        throw new LogicException('not used by the benchmark');
    }

    public function updateServiceGCSafePoint(string $serviceId, int $safePoint, int $ttlSeconds): ?int
    {
        throw new LogicException('not used by the benchmark');
    }

    public function getClusterId(): ?int
    {
        return null;
    }

    public function ping(): ?int
    {
        return null;
    }

    public function setClusterId(int|string $clusterId): void
    {
    }

    public function close(): void
    {
    }
}

/**
 * A store double: a reverse RawScan answers with the single key of the region
 * that owns the request's lower bound.
 */
final class BenchStore implements GrpcClientInterface
{
    public int $scanCalls = 0;

    /**
     * @param list<RegionInfo> $regions
     */
    public function __construct(private readonly array $regions)
    {
    }

    public function call(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        $this->scanCalls++;

        $response = new RawScanResponse();
        if (!$request instanceof RawScanRequest || !$request->getReverse()) {
            return $response;
        }

        $lower = $request->getEndKey();
        $upper = $request->getStartKey();
        foreach ($this->regions as $index => $region) {
            if (!KeyOrder::inRange($lower, $region->startKey, $region->endKey)) {
                continue;
            }
            $key = sprintf('k%05d-row', $index);
            if (KeyOrder::gte($key, $lower) && ($upper === '' || KeyOrder::lt($key, $upper))) {
                $pair = new KvPair();
                $pair->setKey($key);
                $pair->setValue('v');
                $response->setKvs([$pair]);
            }

            return $response;
        }

        return $response;
    }

    public function callAsync(
        string $address,
        string $service,
        string $method,
        Message $request,
        string $responseClass,
        ?int $timeoutMs = null,
    ): GrpcFuture {
        throw new LogicException('a bounded reverse scan stays sequential');
    }

    public function callStreaming(
        string $address,
        string $service,
        string $method,
        iterable $requests,
        string $responseClass,
        ?int $timeoutMs = null,
    ): Message {
        throw new LogicException('not used by the benchmark');
    }

    public function close(): void
    {
    }

    public function closeChannel(string $address): void
    {
    }

    public function reapIdleChannels(): void
    {
    }

    public function getChannel(string $address): \Grpc\Channel
    {
        throw new LogicException('not used by the benchmark');
    }
}

$regions = [];
for ($index = 0; $index < REGION_COUNT; $index++) {
    $regions[] = new RegionInfo(
        regionId: $index + 1,
        leaderPeerId: 1,
        leaderStoreId: 1,
        epochConfVer: 1,
        epochVersion: 1,
        startKey: $boundary($index),
        endKey: $boundary($index + 1),
    );
}

$upper = sprintf('k%05d', REGION_COUNT);
$lower = 'k00000';

$pd = new BenchPd($regions);
$store = new BenchStore($regions);
$cache = new RegionCache(logger: new NullLogger());
$scanner = new RawKvScanner(
    $pd,
    $store,
    new RegionResolver($pd, $cache),
    new TimeoutConfig(),
    maxBackoffMs: 20000,
    serverBusyBudgetMs: 60000,
    regionCache: $cache,
    logger: new NullLogger(),
);

// The shipped top-down walk.
$pd->reset();
$rows = $scanner->reverseScan($upper, $lower, ROW_LIMIT, false);
$afterCalls = $pd->getRegionCalls + $pd->getPrevRegionCalls + $pd->scanRegionsCalls;
$afterDescriptors = $pd->getRegionCalls + $pd->getPrevRegionCalls + $pd->descriptors;

// The pre-#198 shape: drain every ScanRegions batch of the remaining range
// before walking it from the top.
$pd->reset();
$cursor = $lower;
while (true) {
    $batch = $pd->scanRegions($cursor, $upper, 32);
    if ($batch === []) {
        break;
    }
    $last = $batch[count($batch) - 1]->endKey;
    if ($last === '' || KeyOrder::gte($last, $upper) || count($batch) < 32) {
        break;
    }
    $cursor = $last;
}
$beforeCalls = $pd->scanRegionsCalls;
$beforeDescriptors = $pd->descriptors;

printf("reverseScan(limit: %d) over %d single-row regions\n", ROW_LIMIT, REGION_COUNT);
printf("  rows returned:            %d\n", count($rows));
printf("  before: PD calls %d, region descriptors %d\n", $beforeCalls, $beforeDescriptors);
printf("  after:  PD calls %d, region descriptors %d\n", $afterCalls, $afterDescriptors);
printf(
    "  ratio:  %.1fx fewer descriptors, %.1fx fewer PD calls\n",
    $beforeDescriptors / max(1, $afterDescriptors),
    $beforeCalls / max(1, $afterCalls),
);
