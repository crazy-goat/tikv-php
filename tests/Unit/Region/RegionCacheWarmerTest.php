<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionCacheWarmer;
use CrazyGoat\TiKV\Tests\Unit\Support\StubRegionCache;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #425: refresh and preload the region cache.
 *
 * The warmer is the PHP-process-model answer to the missing background
 * refresh: it owns no timer and starts no loop, so a worker drives it from its
 * own tick. These tests pin the properties the tick relies on — the interval
 * bounds the PD traffic, a PD failure leaves the cache as it was, and a split
 * or merge replaces the stale entry.
 */
class RegionCacheWarmerTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    private RegionCache $cache;

    private int $nowMs = 1_000_000;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->cache = new RegionCache();
    }

    private function warmer(int $refreshIntervalMs = 0): RegionCacheWarmer
    {
        return new RegionCacheWarmer(
            $this->pdClient,
            $this->cache,
            new NullLogger(),
            $refreshIntervalMs,
            fn (): int => $this->nowMs,
        );
    }

    private function region(
        int $id,
        string $startKey,
        string $endKey = '',
        int $epochVersion = 1,
        int $leaderStoreId = 1,
    ): RegionInfo {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: $epochVersion,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    public function testPreloadCachesTheRegionsOfARange(): void
    {
        $first = $this->region(1, '100', '200');
        $second = $this->region(2, '200', '300');

        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('100', '300', 128)
            ->willReturn([$first, $second]);

        $stored = $this->warmer()->preload([['100', '300']]);

        $this->assertSame(2, $stored);
        $this->assertSame($first, $this->cache->getByKey('150'));
        $this->assertSame($second, $this->cache->getByKey('250'));
    }

    public function testPreloadTreatsAnEmptyEndKeyAsInfinity(): void
    {
        $region = $this->region(1, '', '');

        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->with('', '', 128)
            ->willReturn([$region]);

        $this->assertSame(1, $this->warmer()->preload([['', '']]));
        $this->assertSame($region, $this->cache->getByKey('anything'));
    }

    public function testPreloadPagesUntilTheRangeIsCovered(): void
    {
        $firstPage = [];
        for ($id = 1; $id <= 128; $id++) {
            $firstPage[] = $this->region($id, sprintf('%04d', $id), sprintf('%04d', $id + 1));
        }
        $last = $this->region(129, '0129', '0200');

        $this->pdClient->expects($this->exactly(2))
            ->method('scanRegions')
            ->willReturnOnConsecutiveCalls($firstPage, [$last]);

        $stored = $this->warmer()->preload([['0001', '0200']]);

        $this->assertSame(129, $stored);
        $this->assertSame(129, $this->cache->count());
    }

    public function testPreloadStopsAtAShortPage(): void
    {
        $this->pdClient->expects($this->once())
            ->method('scanRegions')
            ->willReturn([$this->region(1, '100', '150')]);

        $stored = $this->warmer()->preload([['100', '300']]);

        $this->assertSame(1, $stored);
    }

    public function testPreloadRejectsAReversedRangeBeforeAnyRpc(): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');

        $this->expectException(InvalidArgumentException::class);

        $this->warmer()->preload([['300', '100']]);
    }

    public function testPreloadRejectsAMalformedRange(): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');

        $this->expectException(InvalidArgumentException::class);

        $this->warmer()->preload([['start' => '100']]);
    }

    public function testPreloadKeepsTheCacheAndDoesNotThrowOnPdFailure(): void
    {
        $cached = $this->region(1, '100', '200');
        $this->cache->put($cached);

        $this->pdClient->method('scanRegions')
            ->willThrowException(new TiKvException('PD unavailable'));

        $this->assertSame(0, $this->warmer()->preload([['100', '300']]));
        $this->assertSame($cached, $this->cache->getByKey('150'));
    }

    public function testPreloadContinuesWithTheNextRangeAfterAFailure(): void
    {
        $this->cache->put($this->region(9, '700', '800'));
        $this->pdClient->method('scanRegions')
            ->willReturnCallback(function (string $startKey): array {
                if ($startKey === '100') {
                    throw new TiKvException('PD unavailable');
                }

                return [$this->region(1, '500', '600')];
            });

        $this->assertSame(1, $this->warmer()->preload([['100', '200'], ['500', '600']]));
        $this->assertSame(2, $this->cache->count());
        $this->assertNotNull($this->cache->getByKey('550'));
        // The range that failed is untouched.
        $this->assertNotNull($this->cache->getById(9));
    }

    public function testRefreshUpdatesAStaleEpoch(): void
    {
        $this->cache->put($this->region(1, '100', '200', epochVersion: 1));
        $fresh = $this->region(1, '100', '200', epochVersion: 2);

        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->with('100')
            ->willReturn($fresh);

        $this->assertSame(1, $this->warmer()->refresh(true));
        $this->assertSame(2, $this->cache->getById(1)?->epochVersion);
    }

    public function testRefreshDropsARegionPdNoLongerReturns(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        // A merge moved the range into region 2: PD no longer knows region 1.
        $replacement = $this->region(2, '100', '300');

        $this->pdClient->method('getRegion')->willReturn($replacement);

        $this->warmer()->refresh(true);

        $this->assertNull($this->cache->getById(1));
        $this->assertSame($replacement, $this->cache->getById(2));
    }

    public function testRefreshDropsAnEntryPdNoLongerMapsToItsStartKey(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        // A stale PD tree answers with a region that does not cover the key.
        $stale = $this->region(2, '900', '');

        $this->pdClient->method('getRegion')->willReturn($stale);

        $this->assertSame(0, $this->warmer()->refresh(true));
        $this->assertNull($this->cache->getById(1));
        $this->assertNull($this->cache->getById(2));
    }

    public function testRefreshKeepsTheCachedEntryAndDoesNotThrowOnPdFailure(): void
    {
        $cached = $this->region(1, '100', '200', epochVersion: 3);
        $this->cache->put($cached);

        $this->pdClient->method('getRegion')
            ->willThrowException(new TiKvException('PD unavailable'));

        $this->assertSame(0, $this->warmer()->refresh(true));
        $this->assertSame($cached, $this->cache->getById(1));
    }

    public function testRefreshHonoursTheInterval(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        $fresh = $this->region(1, '100', '200', epochVersion: 2);

        $this->pdClient->expects($this->exactly(2))
            ->method('getRegion')
            ->willReturn($fresh);

        $warmer = $this->warmer(refreshIntervalMs: 60_000);

        $this->assertSame(1, $warmer->refresh());
        // 30 s later: inside the interval, no PD traffic and no refresh.
        $this->nowMs += 30_000;
        $this->assertSame(0, $warmer->refresh());
        // Past the interval, the next tick runs again.
        $this->nowMs += 31_000;
        $this->assertSame(1, $warmer->refresh());
    }

    public function testRefreshIsDisabledByDefaultAndForceStillWorks(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        $fresh = $this->region(1, '100', '200', epochVersion: 2);

        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->willReturn($fresh);

        $warmer = $this->warmer();

        $this->assertSame(0, $warmer->refresh());
        $this->assertSame(1, $warmer->refresh(true));
        $this->assertSame(2, $this->cache->getById(1)?->epochVersion);
    }

    public function testRefreshSkipsALeaderlessCachedEntry(): void
    {
        // A RegionCache never stores a leaderless region; a third-party cache
        // may, and the warmer must not route a refresh to store 0.
        $cache = new StubRegionCache([$this->region(1, '100', '200', leaderStoreId: 0)]);
        $warmer = new RegionCacheWarmer($this->pdClient, $cache, new NullLogger());

        $this->pdClient->expects($this->never())->method('getRegion');

        $this->assertSame(0, $warmer->refresh(true));
    }

    public function testRefreshIsIdempotent(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        $fresh = $this->region(1, '100', '200', epochVersion: 2);

        $this->pdClient->method('getRegion')->willReturn($fresh);

        $warmer = $this->warmer();

        $this->assertSame(1, $warmer->refresh(true));
        $this->assertSame(1, $warmer->refresh(true));
        $this->assertSame(1, $this->cache->count());
        $this->assertSame(2, $this->cache->getById(1)?->epochVersion);
    }

    public function testRefreshIsBoundedPerTickAndContinuesOnTheNextTick(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        $this->cache->put($this->region(2, '200', '300'));
        $this->cache->put($this->region(3, '300', '400'));

        $this->pdClient->expects($this->exactly(3))
            ->method('getRegion')
            ->willReturnCallback(fn (string $key): RegionInfo => match ($key) {
                '100' => $this->region(1, '100', '200', epochVersion: 2),
                '200' => $this->region(2, '200', '300', epochVersion: 2),
                '300' => $this->region(3, '300', '400', epochVersion: 2),
                default => throw new \LogicException('unexpected key ' . $key),
            });

        $warmer = new RegionCacheWarmer(
            $this->pdClient,
            $this->cache,
            new NullLogger(),
            clockMs: fn (): int => $this->nowMs,
            maxRegionsPerRefresh: 2,
        );

        // Two entries per tick; the third is picked up by the next tick.
        $this->assertSame(2, $warmer->refresh(true));
        $this->assertSame(1, $warmer->refresh(true));
        $this->assertSame(2, $this->cache->getById(1)?->epochVersion);
        $this->assertSame(2, $this->cache->getById(2)?->epochVersion);
        $this->assertSame(2, $this->cache->getById(3)?->epochVersion);
    }

    public function testRefreshAbortsTheTickAfterTheFirstPdFailure(): void
    {
        $first = $this->region(1, '100', '200', epochVersion: 3);
        $this->cache->put($first);
        $this->cache->put($this->region(2, '200', '300', epochVersion: 3));
        $this->cache->put($this->region(3, '300', '400', epochVersion: 3));

        $this->pdClient->expects($this->once())
            ->method('getRegion')
            ->willThrowException(new TiKvException('PD unavailable'));

        $logger = new TestLogger();
        $warmer = new RegionCacheWarmer($this->pdClient, $this->cache, $logger);

        $this->assertSame(0, $warmer->refresh(true));
        $this->assertCount(3, $this->cache->getCachedRegions());
        $this->assertSame(3, $this->cache->getById(1)?->epochVersion);
        $this->assertCount(1, $logger->records('warning'));
    }

    public function testRefreshCountsAMergeOnce(): void
    {
        $this->cache->put($this->region(1, '100', '200'));
        $this->cache->put($this->region(2, '200', '300'));

        $merged = $this->region(9, '100', '300');
        $this->pdClient->method('getRegion')->willReturn($merged);

        $this->assertSame(1, $this->warmer()->refresh(true));
        $this->assertSame(1, $this->cache->count());
        $this->assertSame($merged, $this->cache->getById(9));
    }

    public function testRefreshDoesNotCountALeaderlessAnswer(): void
    {
        $this->cache->put($this->region(1, '100', '200'));

        $leaderless = $this->region(1, '100', '200', epochVersion: 2, leaderStoreId: 0);
        $this->pdClient->method('getRegion')->willReturn($leaderless);

        $this->assertSame(0, $this->warmer()->refresh(true));
        $this->assertSame(0, $this->cache->count());
    }

    public function testPreloadCountsOnlyCacheableRegions(): void
    {
        $valid = $this->region(1, '100', '200');
        $leaderless = $this->region(2, '200', '300', leaderStoreId: 0);
        $this->pdClient->method('scanRegions')->willReturn([$valid, $leaderless]);

        $this->assertSame(1, $this->warmer()->preload([['100', '300']]));
        $this->assertSame(1, $this->cache->count());
        $this->assertNull($this->cache->getById(2));
    }

    public function testPreloadCountsAnOverlappingRangeOnce(): void
    {
        $region = $this->region(1, '100', '300');
        $this->pdClient->method('scanRegions')->willReturn([$region]);

        $this->assertSame(1, $this->warmer()->preload([['100', '200'], ['150', '250']]));
        $this->assertSame(1, $this->cache->count());
    }

    public function testPreloadReconcilesAMergeArrivingOnALaterRange(): void
    {
        // The first range stores [100,200); the second answer is the merged
        // [100,300), which supersedes it. Counting as answers arrive would
        // report 2 while the cache holds 1.
        $this->pdClient->method('scanRegions')->willReturnCallback(
            fn (string $startKey): array => $startKey === '100'
                ? [$this->region(1, '100', '200')]
                : [$this->region(9, '100', '300')],
        );

        $this->assertSame(1, $this->warmer()->preload([['100', '200'], ['150', '250']]));
        $this->assertSame(1, $this->cache->count());
        $this->assertNull($this->cache->getById(1));
        $this->assertNotNull($this->cache->getById(9));
    }

    public function testPreloadReconcilesASplitArrivingOnALaterRange(): void
    {
        // The first range stores [100,300); the second answer splits it into
        // [100,200) and [200,300), so the original entry is superseded.
        $this->pdClient->method('scanRegions')->willReturnCallback(
            fn (string $startKey): array => $startKey === '100'
                ? [$this->region(1, '100', '300')]
                : [$this->region(2, '100', '200'), $this->region(3, '200', '300')],
        );

        $this->assertSame(2, $this->warmer()->preload([['100', '300'], ['150', '250']]));
        $this->assertSame(2, $this->cache->count());
        $this->assertNull($this->cache->getById(1));
        $this->assertNotNull($this->cache->getById(2));
        $this->assertNotNull($this->cache->getById(3));
    }

    public function testRefreshReconcilesAMergeArrivingOnALaterCall(): void
    {
        // The first call confirms [100,200); the second returns the merged
        // [100,300), which supersedes both cached entries. Counting as calls
        // arrive would report 2 while the cache holds 1.
        $this->cache->put($this->region(1, '100', '200'));
        $this->cache->put($this->region(2, '200', '300'));

        $this->pdClient->method('getRegion')->willReturnCallback(
            fn (string $key): RegionInfo => $key === '100'
                ? $this->region(1, '100', '200')
                : $this->region(9, '100', '300'),
        );

        $this->assertSame(1, $this->warmer()->refresh(true));
        $this->assertSame(1, $this->cache->count());
        $this->assertNull($this->cache->getById(1));
        $this->assertNull($this->cache->getById(2));
        $this->assertNotNull($this->cache->getById(9));
    }

    public function testRefreshReconciliationRunsAfterAnAbort(): void
    {
        // An abort after a merge was stored must still report the cache as it
        // stands, not the IDs accumulated before the failure.
        $this->cache->put($this->region(1, '100', '200'));
        $this->cache->put($this->region(2, '200', '300'));

        $calls = 0;
        $this->pdClient->method('getRegion')->willReturnCallback(
            function (string $key) use (&$calls): RegionInfo {
                $calls++;
                if ($calls === 2) {
                    throw new TiKvException('PD unavailable');
                }

                return $this->region(9, '100', '300');
            },
        );

        $this->assertSame(1, $this->warmer()->refresh(true));
        $this->assertSame(1, $this->cache->count());
        $this->assertNotNull($this->cache->getById(9));
    }
}
