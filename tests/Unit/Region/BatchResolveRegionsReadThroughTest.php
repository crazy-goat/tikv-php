<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Tests\Unit\Support\StubRegionCache;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #288 (PERF-01): `RegionResolver::batchResolveRegions()` read the
 * region cache only to write to it. Every batch and every transaction commit
 * paid one unconditional PD `ScanRegions` round trip, and because the call
 * passed `limit = 0` it asked for *every* region between the smallest and
 * the largest key in the request.
 *
 * The tests below pin the properties the fix is made of:
 *
 * 1. a warm cache answers the batch with **zero** PD calls;
 * 2. keys spanning a cache gap are scanned over **the gap only**, not over
 *    `[min(keys), max(keys))`;
 * 3. the scan carries an explicit non-zero `limit` and the answer is paged
 *    until the requested range is covered, with each termination condition
 *    (empty page, covered, non-advancing, short page, page ceiling) pinned by
 *    a test that fails when exactly that check is removed;
 * 4. a region the cache already holds unchanged is not re-inserted, and one
 *    whose epoch or leader moved is;
 * 5. the fail-closed contract of #187/#244 is untouched: a key no region
 *    owns still throws, naming the key, and the miss path keeps the
 *    `successor(maxKey)` window that #244/#188 pinned.
 *
 * ## The cache double
 *
 * `getByKey()` answers by call order on a PHPUnit mock, which makes any test
 * pinning a per-key lookup count a bet on an implementation detail — #288
 * itself broke four such tests the moment the batch path started reading the
 * cache. {@see StubRegionCache} answers from the regions the client actually
 * stored, so these tests can state the cache state and assert on the PD
 * conversation, which is the subject.
 */
class BatchResolveRegionsReadThroughTest extends TestCase
{
    /**
     * The page size `batchResolveRegions()` asks PD for, restated as a
     * literal on purpose: the limit is a reviewed decision (see
     * `docs/helpers/decisions.md`), so changing it must fail here rather than
     * silently follow the constant.
     */
    private const EXPECTED_LIMIT = 128;

    private PdClientInterface&MockObject $pdClient;

    private RegionCacheInterface&MockObject $regionCache;

    private StubRegionCache $cache;

    private RegionResolver $resolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->cache = new StubRegionCache();
        // A NullLogger keeps the per-lookup debug() of a real RegionCache out
        // of the numbers the miss-count tests assert on.
        $this->resolver = new RegionResolver($this->pdClient, $this->regionCache, logger: new NullLogger());
    }

    /**
     * Route the mock cache to the stateful double. `invalidate()` is wired
     * unconditionally: nothing in this file's flows drops a region, so a
     * stateful answer is always the safe one.
     */
    private function wireCache(): void
    {
        $this->regionCache->method('getByKey')->willReturnCallback($this->cache->getByKey(...));
        $this->regionCache->method('getById')->willReturnCallback($this->cache->getById(...));
        $this->regionCache->method('put')->willReturnCallback($this->cache->put(...));
        $this->regionCache->method('invalidate')->willReturnCallback($this->cache->invalidate(...));
    }

    private function region(
        int $id,
        string $startKey,
        string $endKey,
        int $epochVersion = 1,
        int $leaderStoreId = 1,
    ): RegionInfo {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: $epochVersion,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    // ========================================================================
    // 1. A warm cache costs zero PD round trips
    // ========================================================================

    public function testWarmCacheResolvesTheWholeBatchWithoutAnyScan(): void
    {
        $first = $this->region(1, '', 'm');
        $second = $this->region(2, 'm', '');
        $this->cache->put($first);
        $this->cache->put($second);
        $this->wireCache();

        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->regionCache->expects($this->never())->method('put');

        $resolved = $this->resolver->batchResolveRegions(['a', 'm', 'z']);

        $this->assertSame(['a' => $first, 'm' => $second, 'z' => $second], $resolved);
    }

    /**
     * The issue's own RPC-count vector: resolve the same key set twice
     * against one cache. Before the fix each call issued a `ScanRegions`
     * (2 PD calls); now the second is free, and with the first having warmed
     * the cache from a cold one, so is the repeat.
     */
    public function testSecondResolutionOfTheSameKeysIssuesNoScan(): void
    {
        $region = $this->region(1, '', '');
        $this->wireCache();
        $this->pdClient->method('scanRegions')->willReturn([$region]);

        $first = $this->resolver->batchResolveRegions(['k1', 'k2']);
        $this->assertCount(2, $first);

        $this->pdClient->expects($this->never())->method('scanRegions');
        $second = $this->resolver->batchResolveRegions(['k1', 'k2']);
        $this->assertSame($first, $second);
    }

    public function testEmptyKeyListSkipsEvenTheCacheLookup(): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->regionCache->expects($this->never())->method('getByKey');

        $this->assertSame([], $this->resolver->batchResolveRegions([]));
    }

    // ========================================================================
    // 2. A cache gap is scanned over the gap only
    // ========================================================================

    /**
     * The acceptance criterion: keys on both sides of a cache gap must not
     * cost a scan of `[min(keys), successor(max(keys)))`. The exact
     * `startKey`/`endKey`/`limit` triple is asserted, so widening the window
     * back over the warm keys fails even when the result map stays correct.
     *
     * The cached layout is `['a','m')` and `['x','z')`, so the gap is
     * `[m, x)`: 'n' and 'o' miss, 'a' and 'y' hit, and no cached key sorts
     * between the two misses — one run, one scan over `[n, "o\x00")`, not
     * over `[a, "y\x00")`.
     */
    public function testGapIsScannedOverTheGapOnlyNotTheWholeKeySpan(): void
    {
        $before = $this->region(1, 'a', 'm');
        $after = $this->region(2, 'x', 'z');
        $this->cache->put($before);
        $this->cache->put($after);
        $this->wireCache();

        $inGap = $this->region(3, 'm', 'x');
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('n', "o\x00", self::EXPECTED_LIMIT)
            ->willReturn([$inGap]);

        $resolved = $this->resolver->batchResolveRegions(['a', 'n', 'o', 'y']);

        $this->assertSame($before, $resolved['a']);
        $this->assertSame($inGap, $resolved['n']);
        $this->assertSame($inGap, $resolved['o']);
        $this->assertSame($after, $resolved['y']);
    }

    /**
     * Two separate gaps, so two runs and two bounded scans — never one scan
     * over everything between the outermost keys.
     *
     * The cached layout is `['a','m')` and `['p','t')`, leaving the gaps
     * `(m, p)` and `[t, +inf)`. 'n' and 'z' are the two misses, and the
     * cached region between them proves they cannot share a run.
     */
    public function testTwoCacheGapsCostOneScanEach(): void
    {
        $this->cache->put($this->region(1, 'a', 'm'));
        $this->cache->put($this->region(2, 'p', 't'));
        $this->wireCache();

        $lowGap = $this->region(3, 'm', 'p');
        $highGap = $this->region(4, 't', '');
        $scans = [];
        $this->pdClient->expects($this->exactly(2))->method('scanRegions')
            ->willReturnCallback(
                static function (string $startKey, string $endKey, int $limit) use (&$scans, $lowGap, $highGap): array {
                    $scans[] = [$startKey, $endKey, $limit];

                    return $startKey === 'n' ? [$lowGap] : [$highGap];
                },
            );

        $resolved = $this->resolver->batchResolveRegions(['z', 'n', 'b', 'r']);

        $this->assertSame(
            [['n', "n\x00", self::EXPECTED_LIMIT], ['z', "z\x00", self::EXPECTED_LIMIT]],
            $scans,
        );
        $this->assertSame($lowGap, $resolved['n']);
        $this->assertSame($highGap, $resolved['z']);
    }

    /**
     * A fully cold cache still costs exactly one scan, over the original
     * window — the read-through must not turn a cold batch into one scan per
     * key. The `limit` argument is asserted so the batching cannot be lost
     * along with the read-through.
     */
    public function testColdCacheStillIssuesASingleScanForTheWholeKeySet(): void
    {
        $this->wireCache();
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('a', "z\x00", self::EXPECTED_LIMIT)
            ->willReturn([$this->region(1, 'a', 'm'), $this->region(2, 'm', '')]);

        $resolved = $this->resolver->batchResolveRegions(['z', 'a', 'm']);

        $this->assertCount(3, $resolved);
    }

    /**
     * Issue #244/#188's window, asserted on the miss path with a
     * *partially* warm cache: the maximum key is itself a region start key,
     * so the upper bound must be `successor($maxKey)`. Passing `$maxKey`
     * verbatim would exclude the region that begins exactly there and the key
     * would find no region — the regression the E2E vector
     * `testBatchRoundTripResolvesEveryKeyWhenTheLargestOneIsARegionStartKey`
     * pins against a live cluster.
     *
     * "Partially warm" matters: a fully warm cache would never compute a
     * window at all, so this is the only shape in which a *read-through*
     * regression of the window is observable from a warm client.
     */
    public function testMissPathKeepsTheSuccessorWindowEvenWithAPartiallyWarmCache(): void
    {
        // Only the region *below* the boundary is cached, so both batch keys
        // miss and the window is computed by the new path.
        $this->cache = new StubRegionCache([$this->region(1, '', 'user:1')]);
        $this->wireCache();

        $boundaryRegion = $this->region(2, 'user:3', '');
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('user:1', "user:3\x00", self::EXPECTED_LIMIT)
            ->willReturn([$this->region(1, '', 'user:3'), $boundaryRegion]);

        $resolved = $this->resolver->batchResolveRegions(['user:1', 'user:3']);

        $this->assertSame(2, $resolved['user:3']->regionId, 'the region starting at the maximum key must own it');
    }

    // ========================================================================
    // 3. The explicit limit and the continuation loop
    // ========================================================================

    /**
     * PD truncated its answer at the page limit, so the resolver must
     * continue from the last returned `endKey` until the range is covered.
     * The exact `(startKey, endKey, limit)` triple of every call is asserted,
     * so both a missing `limit` and a cursor that does not advance fail.
     *
     * A 300-region layout over three pages of 128/128/44, with the batch's
     * maximum key in the last region: only the third page completes the
     * range, so a single-call implementation cannot pass.
     */
    public function testTruncatedAnswerIsFollowedUpUntilTheRangeIsCovered(): void
    {
        $this->wireCache();

        $chain = $this->chainOfRegions(300);
        $calls = [];
        $this->pdClient->method('scanRegions')
            ->willReturnCallback(
                function (string $startKey, string $endKey, int $limit) use (&$calls, $chain): array {
                    $calls[] = [$startKey, $endKey, $limit];

                    return $this->pageStartingAt($startKey, $endKey, $limit, $chain);
                },
            );

        $minKey = $chain[0]->startKey;
        // Strictly inside the last (unbounded) region, so the range is only
        // covered once the final page has been read.
        $maxKey = $chain[299]->startKey . '5';
        $resolved = $this->resolver->batchResolveRegions([$minKey, $maxKey]);

        $this->assertCount(3, $calls, 'three pages of 128/128/44 means three calls');
        $this->assertSame([$minKey, KeyOrder::successor($maxKey), self::EXPECTED_LIMIT], $calls[0]);
        $this->assertSame([$chain[128]->startKey, KeyOrder::successor($maxKey), self::EXPECTED_LIMIT], $calls[1]);
        $this->assertSame([$chain[256]->startKey, KeyOrder::successor($maxKey), self::EXPECTED_LIMIT], $calls[2]);
        $this->assertSame(300, $resolved[$maxKey]->regionId);
    }

    /**
     * A full page whose last `endKey` does not advance past the cursor cannot
     * make progress, so the loop must stop instead of spinning. The batch then
     * fails closed, because the key really is unresolved — the throw is the
     * assertion, and `once()` is the termination proof.
     */
    public function testNonAdvancingAnswerCannotLoopForever(): void
    {
        $this->wireCache();

        // A page of exactly EXPECTED_LIMIT regions, none of which advances
        // past the cursor: every end key equals the page's start key.
        $stuck = [];
        for ($i = 0; $i < self::EXPECTED_LIMIT; $i++) {
            $stuck[] = $this->region($i + 1, 'a', 'a');
        }
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('a', "a\x00", self::EXPECTED_LIMIT)
            ->willReturn($stuck);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('"' . bin2hex('a') . '" (1 bytes); refusing to silently drop');
        $this->resolver->batchResolveRegions(['a']);
    }

    /**
     * The same hazard with a page that advances by exactly one byte every
     * time: termination must come from the hard page ceiling, not from the
     * answer eventually growing. The call count is the assertion, so a
     * removed or raised ceiling is visible.
     *
     * Two keys, because the first one *is* resolved by the first page's
     * region and the throw has to come from the key the walk never reaches.
     */
    public function testAnswerThatBarelyAdvancesStopsAtThePageCeiling(): void
    {
        $this->wireCache();

        $pages = 0;
        $this->pdClient->method('scanRegions')
            ->willReturnCallback(
                function (string $startKey, string $endKey, int $limit) use (&$pages): array {
                    $pages++;
                    // One byte further per page, always a full page and always
                    // the same region ID, so the double holds one entry.
                    $region = $this->region(1, $startKey, $startKey . "\x01");
                    $filler = [];
                    for ($i = 0; $i < $limit; $i++) {
                        $filler[] = $region;
                    }

                    return $filler;
                },
            );

        try {
            $this->resolver->batchResolveRegions(['a', "\xff\xff\xff"]);
            self::fail('a never-reaching scan must not report success');
        } catch (TiKvException $e) {
            self::assertStringContainsString('refusing to silently drop', $e->getMessage());
        }

        $this->assertSame(1024, $pages, 'the page ceiling is what stops this loop');
    }

    /**
     * The short-page exit, isolated from the coverage exit: PD answers with
     * fewer regions than the limit asked for, and the last one stops *short*
     * of the requested end. The loop must still stop on the page count — a
     * key PD never answered for is a key the call fails closed on, and
     * "ask again until it runs out" would turn a truncation into a round trip
     * storm.
     */
    public function testShortPageThatStopsShortOfTheRangeStillEndsTheLoop(): void
    {
        $this->wireCache();
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('a', "z\x00", self::EXPECTED_LIMIT)
            ->willReturn([$this->region(1, 'a', 'm')]);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('"' . bin2hex('z') . '" (1 bytes); refusing to silently drop');
        $this->resolver->batchResolveRegions(['a', 'z']);
    }

    /**
     * The coverage exit, isolated from the short-page one: a FULL page whose
     * last region already reaches the requested end key. The page count says
     * "there may be more", the ranges say "there is nothing left to fetch",
     * and the range check is the one that must win.
     */
    public function testFullPageThatReachesTheEndKeyStopsAfterOneCall(): void
    {
        $this->wireCache();

        $page = [];
        for ($i = 0; $i < self::EXPECTED_LIMIT; $i++) {
            // Interior slices of one run; only the LAST one ends past the
            // requested end key, which is what makes the range covered.
            $start = 'a' . str_repeat("\x00", $i);
            $end = $i + 1 === self::EXPECTED_LIMIT ? 'z~' : $start . "\x01";
            $page[] = $this->region($i + 1, $start, $end);
        }
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('a', "b\x00", self::EXPECTED_LIMIT)
            ->willReturn($page);

        $resolved = $this->resolver->batchResolveRegions(['a', 'b']);

        $this->assertSame(self::EXPECTED_LIMIT, $resolved['b']->regionId);
    }

    /**
     * The last region of the keyspace is unbounded (`''` = +infinity), which
     * is the shape a real single-region and last-region answer has. It ends
     * the loop like a covered range does.
     *
     * Overlaps {@see testFullPageThatReachesTheEndKeyStopsAfterOneCall()} on
     * purpose: with a short page the page-count exit would also terminate, so
     * only the full-page variant fails when the coverage check is removed.
     */
    public function testCoveredRangeStopsEvenWhenThePageIsShort(): void
    {
        $this->wireCache();
        $unbounded = $this->region(1, 'a', '');
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('a', "z\x00", self::EXPECTED_LIMIT)
            ->willReturn([$unbounded]);

        $resolved = $this->resolver->batchResolveRegions(['a', 'z']);

        $this->assertSame($unbounded, $resolved['z']);
    }

    public function testEmptyPageFailsClosedWithASingleCall(): void
    {
        $this->wireCache();
        $this->pdClient->expects($this->once())->method('scanRegions')->willReturn([]);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('refusing to silently drop');
        $this->resolver->batchResolveRegions(['a']);
    }

    // ========================================================================
    // 4. Identical regions are not re-inserted; moved epochs/leaders are
    // ========================================================================

    /**
     * A window can legitimately span a region the cache already holds: the
     * run's window is the envelope of its keys, and a cached region may sit
     * strictly inside it without owning any of them (here `['n','p')` holds no
     * batch key — the misses are 'm', below the lowest cached start, and 'q',
     * past the cached end).
     *
     * That middle region comes back from PD identical, and re-inserting it
     * used to cost a full `put()` — TTL refresh, LRU touch, expiry-heap push.
     *
     * The last assertion is why the identity is asked by region ID: a
     * `getByKey($region->startKey)` probe adds a lookup per scanned region,
     * and on the post-#289 cache that probe costs *more* than the `put()` it
     * avoids (~10 µs vs ~6.8 µs against 10 000 entries), which made this
     * criterion a measured regression. `getById()` is the O(1) lookup that
     * makes it a win.
     */
    public function testRegionAlreadyCachedWithTheSameEpochIsNotReinserted(): void
    {
        $middle = $this->region(2, 'n', 'p');
        $this->cache = new StubRegionCache([$middle]);
        $this->wireCache();
        $this->regionCache->expects($this->exactly(2))->method('put');

        $lower = $this->region(1, '', 'n');
        $upper = $this->region(3, 'p', '');
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('m', "q\x00", self::EXPECTED_LIMIT)
            ->willReturn([$lower, $middle, $upper]);

        $resolved = $this->resolver->batchResolveRegions(['m', 'q']);

        $this->assertSame($lower, $resolved['m']);
        $this->assertSame($upper, $resolved['q']);
        $this->assertSame(2, $this->cache->putCalls(), 'only the two new regions are written');
        $this->assertSame(
            ['m', 'q'],
            $this->cache->lookups(),
            'the identity is probed by region ID, so no scanned start key is looked up',
        );
    }

    public function testRegionWhoseEpochMovedIsReinserted(): void
    {
        $this->cache = new StubRegionCache([$this->region(2, 'n', 'p', 1)]);
        $this->wireCache();
        $this->regionCache->expects($this->exactly(3))->method('put');

        $split = $this->region(2, 'n', 'p', 2);
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('m', "q\x00", self::EXPECTED_LIMIT)
            ->willReturn([$this->region(1, '', 'n'), $split, $this->region(3, 'p', '')]);

        $this->resolver->batchResolveRegions(['m', 'q']);

        $this->assertSame(2, $this->cache->getByKey('o')?->epochVersion);
    }

    /**
     * A leader transfer does not bump the epoch, so an epoch-only identity
     * test would keep serving the deposed leader out of the cache. The
     * identity therefore includes the leader, and a re-inserted region with a
     * different one is written.
     */
    public function testRegionWhoseLeaderMovedIsReinserted(): void
    {
        $this->cache = new StubRegionCache([$this->region(2, 'n', 'p', 1, 1)]);
        $this->wireCache();
        $this->regionCache->expects($this->exactly(3))->method('put');

        $moved = $this->region(2, 'n', 'p', 1, 7);
        $this->pdClient->expects($this->once())->method('scanRegions')
            ->with('m', "q\x00", self::EXPECTED_LIMIT)
            ->willReturn([$this->region(1, '', 'n'), $moved, $this->region(3, 'p', '')]);

        $this->resolver->batchResolveRegions(['m', 'q']);

        $this->assertSame(7, $this->cache->getByKey('o')?->leaderStoreId);
    }

    // ========================================================================
    // 5. Fail-closed (issue #187) still holds on the miss path
    // ========================================================================

    /**
     * A scanned region that does not own the key leaves it unresolved, and the
     * call fails loudly naming the key — the read-through must not paper over
     * it, and a warm sibling must not rescue it either.
     */
    public function testUnresolvableKeyStillThrowsOnTheMissPath(): void
    {
        $this->cache->put($this->region(1, 'a', 'm'));
        $this->wireCache();
        $this->pdClient->method('scanRegions')->willReturn([$this->region(2, 'q', '')]);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('"' . bin2hex('n') . '" (1 bytes); refusing to silently drop');
        $this->resolver->batchResolveRegions(['a', 'n']);
    }

    /**
     * The message names the FIRST unresolvable key in the caller's order, not
     * in the byte-sorted order the runs were computed from.
     */
    public function testTheFirstUnresolvableKeyInCallerOrderIsNamed(): void
    {
        $this->wireCache();
        $this->pdClient->method('scanRegions')->willReturn([$this->region(1, 'a', 'b')]);

        try {
            $this->resolver->batchResolveRegions(['z', 'a', 'y']);
            self::fail('both z and y are unresolvable and the call must fail');
        } catch (TiKvException $e) {
            self::assertStringContainsString('"' . bin2hex('z') . '" (1 bytes)', $e->getMessage());
        }
    }

    /**
     * A key the cache resolves and a key it cannot are reported in one map:
     * the cache hit is not lost because a sibling key missed.
     */
    public function testCacheHitAndMissAreReportedInOneResult(): void
    {
        $warm = $this->region(1, 'a', 'm');
        $this->cache->put($warm);
        $this->wireCache();
        $cold = $this->region(2, 'm', 'x');
        $this->pdClient->method('scanRegions')->willReturn([$cold]);

        $resolved = $this->resolver->batchResolveRegions(['b', 'o']);

        $this->assertSame($warm, $resolved['b']);
        $this->assertSame($cold, $resolved['o']);
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /**
     * A gapless chain of $count regions, region $i covering
     * `["a%03d" of $i, "a%03d" of $i+1)`, with the LAST one unbounded
     * (`''` = +infinity). Fixed-width boundaries are byte-ordered and
     * contiguous, and every region has interior key space, so a page boundary
     * is unambiguous and a key can be chosen strictly inside a region.
     *
     * @return list<RegionInfo>
     */
    private function chainOfRegions(int $count): array
    {
        $regions = [];
        for ($i = 0; $i < $count; $i++) {
            $regions[] = $this->region(
                $i + 1,
                sprintf('a%03d', $i),
                $i + 1 === $count ? '' : sprintf('a%03d', $i + 1),
            );
        }

        return $regions;
    }

    /**
     * The page PD would answer with for [$startKey, $endKey): the regions
     * starting at or after $startKey, at most $limit of them, and never one
     * starting at or past $endKey (PD's `ScanRegions` is half-open).
     *
     * @param list<RegionInfo> $chain
     * @return list<RegionInfo>
     */
    private function pageStartingAt(string $startKey, string $endKey, int $limit, array $chain): array
    {
        $answer = [];
        foreach ($chain as $region) {
            if (KeyOrder::lt($region->startKey, $startKey)) {
                continue;
            }
            if (KeyOrder::gte($region->startKey, $endKey)) {
                break;
            }
            $answer[] = $region;
            if (count($answer) >= $limit) {
                break;
            }
        }

        return $answer;
    }
}
