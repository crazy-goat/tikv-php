<?php

declare(strict_types=1);

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use Psr\Log\AbstractLogger;
use Psr\Log\NullLogger;

require dirname(__DIR__) . '/vendor/autoload.php';

/**
 * Small standalone benchmark for the RegionCache write and read paths.
 *
 * Run from the repository root with:
 *   php benchmarks/RegionCacheBenchmark.php
 *
 * The read path is the hot one: 100,000 `getByKey()` calls against a warm
 * 10,000-entry cache. Before issue #300 every one of those calls built a
 * `KeyRedactor::redact()` string for a debug record the `NullLogger`
 * discarded.
 *
 * The benchmark measures **both** shapes on the same machine, so the
 * ratio it prints is valid everywhere — the absolute milliseconds of the
 * issue's acceptance criterion (100,000 lookups below 78 ms) were measured
 * on the issue author's hardware and are not comparable across machines:
 *
 * - the *guarded* cache is the current one: the logger cannot emit a debug
 *   record, so no log context is built for it;
 * - the *reference* cache gets a logger that accepts every record and
 *   discards it — the shape the cache had before issue #300, with the
 *   context built on every lookup.
 */

/**
 * A logger that accepts every record and discards it.
 *
 * It is not a `NullLogger`, so the region cache builds the log context for
 * it: the shape the cache had before issue #300.
 */
final class DiscardingLogger extends AbstractLogger
{
    /**
     * @param mixed $level
     * @param mixed[] $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
    }
}

/**
 * The median of the given measurements, in milliseconds.
 *
 * @param list<float> $measurements
 */
function median(array $measurements): float
{
    sort($measurements);
    $count = count($measurements);
    $middle = intdiv($count, 2);

    if ($count % 2 === 1) {
        return $measurements[$middle];
    }

    return ($measurements[$middle - 1] + $measurements[$middle]) / 2;
}

$regions = [];
for ($i = 0; $i < 10000; $i++) {
    $start = sprintf('k%08d', $i * 10);
    $end = sprintf('k%08d', ($i + 1) * 10);
    $regions[] = new RegionInfo(
        regionId: $i + 1,
        leaderPeerId: 1,
        leaderStoreId: 1,
        epochConfVer: 1,
        epochVersion: 1,
        startKey: $start,
        endKey: $end,
    );
}

// One key per region, each of them inside its region.
$keys = [];
foreach ($regions as $region) {
    $keys[] = $region->startKey . 'x';
}

$lookups = 100000;
$keyCount = count($keys);
$runs = 7;

$guarded = new RegionCache(jitterSeconds: 0, logger: new NullLogger());
$reference = new RegionCache(jitterSeconds: 0, logger: new DiscardingLogger());

$start = hrtime(true);
foreach ($regions as $region) {
    $guarded->put($region);
}
$coldMs = (hrtime(true) - $start) / 1_000_000;

$start = hrtime(true);
foreach ($regions as $region) {
    $reference->put($region);
}
$referenceColdMs = (hrtime(true) - $start) / 1_000_000;

$start = hrtime(true);
foreach ($regions as $region) {
    $guarded->put($region);
}
$warmMs = (hrtime(true) - $start) / 1_000_000;

$start = hrtime(true);
foreach ($regions as $region) {
    $reference->put($region);
}
$referenceWarmMs = (hrtime(true) - $start) / 1_000_000;

printf(
    "RegionCache: cold %d puts %.2f ms; warm %d puts %.2f ms (%.2f us/put); count=%d\n",
    count($regions),
    $coldMs,
    count($regions),
    $warmMs,
    $warmMs * 1000 / count($regions),
    $guarded->count(),
);
printf(
    "RegionCache: reference (log context built) cold %d puts %.2f ms; warm %d puts %.2f ms (%.2f us/put)\n",
    count($regions),
    $referenceColdMs,
    count($regions),
    $referenceWarmMs,
    $referenceWarmMs * 1000 / count($regions),
);

$guardedLookups = [];
$referenceLookups = [];
for ($run = 0; $run < $runs; $run++) {
    $start = hrtime(true);
    for ($i = 0; $i < $lookups; $i++) {
        $guarded->getByKey($keys[$i % $keyCount]);
    }
    $guardedLookups[] = (hrtime(true) - $start) / 1_000_000;

    $start = hrtime(true);
    for ($i = 0; $i < $lookups; $i++) {
        $reference->getByKey($keys[$i % $keyCount]);
    }
    $referenceLookups[] = (hrtime(true) - $start) / 1_000_000;
}

$guardedMs = median($guardedLookups);
$referenceMs = median($referenceLookups);

printf(
    "RegionCache: %d getByKey() calls over %d entries (median of %d runs)\n",
    $lookups,
    $keyCount,
    $runs,
);
printf(
    "  guarded (no log context built): %.2f ms (%.3f us/lookup)\n",
    $guardedMs,
    $guardedMs * 1000 / $lookups,
);
printf(
    "  reference (log context built, as before #300): %.2f ms (%.3f us/lookup)\n",
    $referenceMs,
    $referenceMs * 1000 / $lookups,
);
printf(
    "  the guard saves %.1f %% of the lookup time (the absolute milliseconds are machine-specific)\n",
    (1 - $guardedMs / $referenceMs) * 100,
);
