<?php

declare(strict_types=1);

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use Psr\Log\NullLogger;

require dirname(__DIR__) . '/vendor/autoload.php';

/**
 * Small standalone benchmark for the RegionCache write path.
 *
 * Run from the repository root with:
 *   php benchmarks/RegionCacheBenchmark.php
 */

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

$cache = new RegionCache(jitterSeconds: 0, logger: new NullLogger());
$start = hrtime(true);
foreach ($regions as $region) {
    $cache->put($region);
}
$coldMs = (hrtime(true) - $start) / 1_000_000;

$start = hrtime(true);
foreach ($regions as $region) {
    $cache->put($region);
}
$warmMs = (hrtime(true) - $start) / 1_000_000;

printf(
    "RegionCache: cold %d puts %.2f ms; warm %d puts %.2f ms (%.2f us/put); count=%d\n",
    count($regions),
    $coldMs,
    count($regions),
    $warmMs,
    $warmMs * 1000 / count($regions),
    $cache->count(),
);
