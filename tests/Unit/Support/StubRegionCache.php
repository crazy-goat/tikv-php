<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Util\KeyOrder;

/**
 * A stateful, in-memory {@see RegionCacheInterface} for unit tests that must
 * react to the client's *own* cache mutations (a `put()` that makes the next
 * lookup a hit, an `invalidate()` that makes it a miss, a `switchLeader()`
 * that moves it).
 *
 * ## Why a double and not `willReturnOnConsecutiveCalls()`
 *
 * A PHPUnit mock answers `getByKey()` by *call order*, which silently turns
 * every test that uses it into a bet on how many cache lookups the code under
 * test happens to perform. Issue #288 made that bet losable: `batchResolveRegions()`
 * now reads the cache for every key before it contacts PD, so one extra
 * lookup shifts the whole sequence and the test fails with
 * `NoMoreReturnValuesConfiguredException` — or, worse, still passes while
 * modelling a cache state the client never had. #293 hit the same trap for the
 * same reason.
 *
 * This double removes the bet: it holds the regions the client actually put
 * there and derives every answer from the current state, so the test states
 * "the cache starts out holding the stale region" instead of "the first two
 * lookups answer stale and the third one misses".
 *
 * Wire it behind a mock when the test also wants call-count expectations:
 *
 * ```php
 * $cache = new StubRegionCache([$staleRegion]);
 * $this->regionCache->method('getByKey')->willReturnCallback($cache->getByKey(...));
 * $this->regionCache->method('getById')->willReturnCallback($cache->getById(...));
 * $this->regionCache->method('put')->willReturnCallback($cache->put(...));
 * $this->regionCache->method('invalidate')->willReturnCallback($cache->invalidate(...));
 * ```
 *
 * `getById()` is wired for the same reason: the resolver's `put()`-skip asks
 * "does the cache already hold this region ID?" before it stores a region a PD
 * scan returned, and a mock that auto-returns `null` for it would make every
 * scanned region look uncached.
 *
 * Scope: no TTL, no LRU, no overlap removal, no metrics — the production
 * semantics of those are `RegionCache`'s own subject (see `RegionCacheTest`).
 * What is modelled is the *state machine* a test reasons about: which
 * regions are cached, which one owns a key, and what the client's
 * invalidation and leader-switch calls do to that.
 */
final class StubRegionCache implements RegionCacheInterface
{
    /** @var array<int, RegionInfo> regionId => region */
    private array $regions = [];

    private int $putCalls = 0;

    private int $invalidateCalls = 0;

    /**
     * @param RegionInfo[] $regions regions to seed the cache with
     */
    public function __construct(array $regions = [])
    {
        foreach ($regions as $region) {
            // Deliberately not put(): seeding is test setup, not a client call,
            // so putCalls() must stay at zero until the code under test runs.
            $this->regions[$region->regionId] = $region;
        }
    }

    /** @var list<int> store ids passed to switchLeader(), in call order */
    private array $switchLeaderCalls = [];

    /** @var list<string> keys passed to getByKey(), in call order */
    private array $lookups = [];

    public function getByKey(string $key): ?RegionInfo
    {
        $this->lookups[] = $key;

        $best = null;
        foreach ($this->regions as $region) {
            if (!KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                continue;
            }
            // A complete layout has at most one owner; the bytewise-latest
            // start key wins so a partially seeded cache still answers with
            // the most specific region it holds (the same choice the
            // production cache's predecessor walk makes).
            if ($best === null || KeyOrder::gte($region->startKey, $best->startKey)) {
                $best = $region;
            }
        }

        return $best;
    }

    /**
     * The cached region with this ID, or null when the cache does not hold it
     * — the same all-or-nothing answer {@see self::getByKey()} gives, so a
     * caller can state "the cache already holds this region" without also
     * stating which key owns it.
     */
    public function getById(int $regionId): ?RegionInfo
    {
        return $this->regions[$regionId] ?? null;
    }

    /**
     * The cached chain covering [$startKey, $endKey), or an empty array when
     * the chain is incomplete — the same all-or-nothing contract as
     * {@see \CrazyGoat\TiKV\Client\Cache\RegionCache::getRegionsInRange()}, so a
     * caller cannot mistake a partial answer for a complete one.
     *
     * @return list<RegionInfo>
     */
    public function getRegionsInRange(string $startKey, string $endKey): array
    {
        $regions = [];
        $cursor = $startKey;

        while (true) {
            $region = $this->getByKey($cursor);
            if (!$region instanceof RegionInfo) {
                return [];
            }
            $regions[] = $region;

            if ($region->endKey === '' || ($endKey !== '' && KeyOrder::gte($region->endKey, $endKey))) {
                return $regions;
            }
            if (KeyOrder::lte($region->endKey, $cursor)) {
                return [];
            }
            $cursor = $region->endKey;
        }
    }

    public function put(RegionInfo $region): void
    {
        $this->putCalls++;
        $this->regions[$region->regionId] = $region;
    }

    public function invalidate(int $regionId, string $reason = 'region_error'): void
    {
        $this->invalidateCalls++;
        unset($this->regions[$regionId]);
    }

    /**
     * Move a cached region's leader to the peer on $leaderStoreId.
     *
     * The peer id comes from the region's own peer list, as the production
     * cache does. A region seeded without peers (the common shape in these
     * tests) keeps its current leader *peer* id and only moves the store id —
     * the tests assert the store, which is what reaches the wire.
     */
    public function switchLeader(int $regionId, int $leaderStoreId): bool
    {
        $this->switchLeaderCalls[] = $leaderStoreId;

        $region = $this->regions[$regionId] ?? null;
        if (!$region instanceof RegionInfo) {
            return false;
        }

        $leaderPeerId = $region->leaderPeerId;
        foreach ($region->peers as $peer) {
            if ($peer->storeId === $leaderStoreId) {
                $leaderPeerId = $peer->peerId;
                break;
            }
        }

        $this->regions[$regionId] = new RegionInfo(
            regionId: $region->regionId,
            leaderPeerId: $leaderPeerId,
            leaderStoreId: $leaderStoreId,
            epochConfVer: $region->epochConfVer,
            epochVersion: $region->epochVersion,
            startKey: $region->startKey,
            endKey: $region->endKey,
            peers: $region->peers,
        );

        return true;
    }

    public function clear(): void
    {
        $this->regions = [];
    }

    public function count(): int
    {
        return count($this->regions);
    }

    public function putCalls(): int
    {
        return $this->putCalls;
    }

    public function invalidateCalls(): int
    {
        return $this->invalidateCalls;
    }

    /**
     * @return list<int>
     */
    public function switchLeaderCalls(): array
    {
        return $this->switchLeaderCalls;
    }

    /**
     * @return list<string>
     */
    public function lookups(): array
    {
        return $this->lookups;
    }
}
