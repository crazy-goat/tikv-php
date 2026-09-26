<?php

/**
 * Benchmark for issue #288 (PERF-01): the CPU cost of one
 * `RegionResolver::batchResolveRegions()` call for a 100-region batch against
 * a warm 10 000-entry region cache, and of the `put()`-skip that PERF-02's
 * share of that cost was made of.
 *
 * Run from the repository root with:
 *   php benchmarks/BatchResolveRegionsBenchmark.php
 *
 * Three measurements plus a batch-size sweep, all machine-dependent on
 * purpose (never assert on them in the test suite):
 *
 *  1. `pre-#288` — the algorithm the issue describes, reproduced inline: one
 *     `ScanRegions` over `[minKey, successor(maxKey))` with `limit = 0` and an
 *     unconditional `put()` of everything that came back. The scan is a stub
 *     returning a pre-built list, so what is timed is the client's CPU work
 *     and not the round trip — the `PD scanRegions() calls` line is the other
 *     half of the pre-#288 cost, and the one that a LAN round trip or a
 *     10 000-region `ScanRegions` answer dwarfs.
 *  2. `read-through` — the shipped code path: one cache lookup per key, zero
 *     PD calls, zero `put()`s. A batch-size sweep follows it, because this
 *     half of the change is charged **per key** and is linear: a 10 000-key
 *     batch costs ~12 us x 10 000 = ~120 ms of CPU, which "zero PD calls" does
 *     not convey. The old path paid its `put()`s per *region*, so it grew with
 *     the region count instead.
 *  3. `put() skip` — the meaningful micro-measurement: re-inserting 100
 *     regions the cache already holds unchanged, in three shapes: the
 *     unconditional `put()` this criterion exists to avoid, the
 *     `getByKey()`-based identity check #288 first shipped (a measured
 *     regression — the probe cost *more* than the write it skipped, because a
 *     key lookup walks the treap and redacts a key for its debug line), and
 *     the `getById()`-based check it ships now, which is one array lookup.
 *     This is the part of the old cost that survives a partial-cache miss.
 *
 * Note on the issue's numbers: it quotes 33.85 ms of CPU for a 100-region
 * batch, measured against the pre-#289 `RegionCache` (a packed array with
 * index shifts). #289 replaced that with the ID map + start-key treap, whose
 * re-insert fast path is roughly ten times cheaper, so the 33.85 ms no longer
 * reproduces — see the `RegionCacheBenchmark.php` "warm %d puts ... (%.2f
 * us/put)" line for the current cost of 10 000 of them. What the read-through
 * removes is therefore mostly *not* CPU; it is the unconditional PD round trip
 * and the unbounded answer.
 */

declare(strict_types=1);

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Psr\Log\NullLogger;

require dirname(__DIR__) . '/vendor/autoload.php';

const CACHE_ENTRIES = 10000;
const BATCH_REGIONS = 100;
const REPEATS = 20;

/**
 * A fixed-width, byte-ordered region boundary. 12 digits cover 10^12
 * regions, so numeric and byte order agree by construction and no key needs
 * escaping.
 */
$regionKey = static fn (int $index): string => 'k' . str_pad((string) $index, 12, '0', STR_PAD_LEFT);

/**
 * A PD client whose `scanRegions()` answers from a pre-built list, so the
 * benchmark measures the client's CPU work rather than a round trip.
 */
$pdClient = static function (array $regions): PdClientInterface {
    return new class ($regions) implements PdClientInterface {
        public int $scanCalls = 0;

        public int $lastLimit = -1;

        /**
         * @param list<RegionInfo> $regions
         */
        public function __construct(private readonly array $regions)
        {
        }

        public function getRegion(string $key): RegionInfo
        {
            throw new LogicException('not used by the benchmark');
        }

        public function getKeyspaceId(string $name): int
        {
            throw new LogicException('not used by the benchmark');
        }

        public function getStore(int $storeId): ?Store
        {
            throw new LogicException('not used by the benchmark');
        }

        public function scanRegions(string $startKey, string $endKey, int $limit = 0): array
        {
            $this->scanCalls++;
            $this->lastLimit = $limit;

            return $this->regions;
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

        public function setClusterId(int $clusterId): void
        {
        }

        public function close(): void
        {
        }
    };
};

$elapsedMs = static function (callable $operation): float {
    $start = hrtime(true);
    $operation();

    return (hrtime(true) - $start) / 1_000_000;
};

$median = static function (array $samples): float {
    sort($samples);
    $middle = intdiv(count($samples), 2);

    return count($samples) % 2 === 1
        ? $samples[$middle]
        : ($samples[$middle - 1] + $samples[$middle]) / 2;
};

// A 10 000-region layout, region $i covering [key($i), key($i+1)) with the
// last one unbounded — the shape a real cluster's PD answers with.
$layout = [];
for ($i = 0; $i < CACHE_ENTRIES; $i++) {
    $layout[] = new RegionInfo(
        regionId: $i + 1,
        leaderPeerId: $i + 1,
        leaderStoreId: 1,
        epochConfVer: 1,
        epochVersion: 1,
        startKey: $regionKey($i),
        endKey: $i + 1 === CACHE_ENTRIES ? '' : $regionKey($i + 1),
    );
}

// One key inside each of the first BATCH_REGIONS regions — the issue's
// scenario: a 100-region batchGet against a warm 10 000-entry cache.
$keys = [];
for ($i = 0; $i < BATCH_REGIONS; $i++) {
    $keys[] = $regionKey($i) . '-row';
}

$cache = new RegionCache(jitterSeconds: 0, logger: new NullLogger());
foreach ($layout as $region) {
    $cache->put($region);
}

$warmPd = $pdClient(array_slice($layout, 0, BATCH_REGIONS));
$warmResolver = new RegionResolver($warmPd, $cache, logger: new NullLogger());
$prePd = $pdClient(array_slice($layout, 0, BATCH_REGIONS));
$preResolver = new RegionResolver($prePd, $cache, logger: new NullLogger());

// The pre-#288 algorithm, verbatim in shape: one `limit = 0` scan over
// [minKey, successor(maxKey)), an unconditional put() of every region it
// returned, and a per-key binary search.
$pre288 = static function () use ($prePd, $preResolver, $cache, $keys): void {
    $regions = $prePd->scanRegions($keys[0], KeyOrder::successor($keys[count($keys) - 1]));
    foreach ($regions as $region) {
        $cache->put($region);
    }
    $preResolver->getRegionInfo($keys[0]);
};

$readThrough = static function () use ($warmResolver, $keys): void {
    $resolved = $warmResolver->batchResolveRegions($keys);
    if (count($resolved) !== count($keys)) {
        throw new LogicException('incomplete resolution');
    }
};

$before = [];
$after = [];
for ($i = 0; $i < REPEATS; $i++) {
    $before[] = $elapsedMs($pre288);
    $after[] = $elapsedMs($readThrough);
}

// The put() skip, isolated: re-inserting regions the cache already holds.
// Three shapes, because the criterion is only a win if the probe is cheaper
// than the write — and the probe's cost is the whole question.
$unchanged = array_slice($layout, 0, BATCH_REGIONS);
$unconditionalPuts = static function () use ($cache, $unchanged): void {
    foreach ($unchanged as $region) {
        $cache->put($region);
    }
};
$sameIdentity = static fn (RegionInfo $cached, RegionInfo $region): bool => $cached->epochVersion === $region->epochVersion
    && $cached->epochConfVer === $region->epochConfVer
    && $cached->startKey === $region->startKey
    && $cached->endKey === $region->endKey
    && $cached->leaderStoreId === $region->leaderStoreId
    && $cached->leaderPeerId === $region->leaderPeerId;
$keyProbed = static function () use ($cache, $unchanged, $sameIdentity): void {
    foreach ($unchanged as $region) {
        $cached = $cache->getByKey($region->startKey);
        if ($cached instanceof RegionInfo && $sameIdentity($cached, $region)) {
            continue;
        }
        $cache->put($region);
    }
};
$idProbed = static function () use ($cache, $unchanged, $sameIdentity): void {
    foreach ($unchanged as $region) {
        $cached = $cache->getById($region->regionId);
        if ($cached instanceof RegionInfo && $sameIdentity($cached, $region)) {
            continue;
        }
        $cache->put($region);
    }
};

$puts = [];
$keyChecks = [];
$idChecks = [];
for ($i = 0; $i < REPEATS; $i++) {
    $puts[] = $elapsedMs($unconditionalPuts);
    $keyChecks[] = $elapsedMs($keyProbed);
    $idChecks[] = $elapsedMs($idProbed);
}

printf(
    "batchResolveRegions(), %d keys over %d regions, %d-entry warm cache (median of %d):\n",
    count($keys),
    BATCH_REGIONS,
    CACHE_ENTRIES,
    REPEATS,
);
printf("  pre-#288  (1 unbounded scan + %d unconditional put()): %8.3f ms\n", BATCH_REGIONS, $median($before));
printf("  read-through (%d cache lookups, 0 PD calls, 0 put()): %8.3f ms\n", count($keys), $median($after));
printf(
    "  put() skip, %d already-cached regions (per region in brackets):\n",
    BATCH_REGIONS,
);
printf("      unconditional put()                     %8.3f ms  [%6.2f us]\n", $median($puts), $median($puts) * 1000 / BATCH_REGIONS);
printf("      identity check via getByKey() (pre-fix) %8.3f ms  [%6.2f us]\n", $median($keyChecks), $median($keyChecks) * 1000 / BATCH_REGIONS);
printf("      identity check via getById()  (shipped) %8.3f ms  [%6.2f us]\n", $median($idChecks), $median($idChecks) * 1000 / BATCH_REGIONS);
printf(
    "  PD scanRegions() calls over the run: pre-#288 %d, read-through %d\n",
    $prePd->scanCalls,
    $warmPd->scanCalls,
);
printf("  cache entries: %d\n", $cache->count());

// The read-through is charged per KEY, so the batch-size sweep is what makes
// its cost legible: a caller that reads a million keys in batches of 10 000
// pays 120 ms of CPU per batch, and "zero PD calls" alone would not say so.
printf("\nread-through cost by batch size (1 key per region, %d-entry warm cache, median of %d):\n", CACHE_ENTRIES, REPEATS);
foreach ([100, 1000, 5000, CACHE_ENTRIES] as $batchSize) {
    $batchKeys = [];
    for ($i = 0; $i < $batchSize; $i++) {
        $batchKeys[] = $regionKey($i) . '-row';
    }

    $samples = [];
    for ($i = 0; $i < REPEATS; $i++) {
        $samples[] = $elapsedMs(static function () use ($warmResolver, $batchKeys): void {
            if (count($warmResolver->batchResolveRegions($batchKeys)) !== count($batchKeys)) {
                throw new LogicException('incomplete resolution');
            }
        });
    }

    $medianMs = $median($samples);
    printf(
        "  %6d keys: %8.3f ms  [%6.2f us/key]\n",
        $batchSize,
        $medianMs,
        $medianMs * 1000 / $batchSize,
    );
}
