<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\MemComparableCodec;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #246 (REG-15): TiKV's EpochNotMatch carries the regions that now
 * cover the stale range. The retry must refresh the region cache from that
 * payload instead of invalidating the entry and paying a PD GetRegion.
 */
class RetryExecutorEpochNotMatchTest extends TestCase
{
    private const KEY = 'k';
    private const REGION_ID = 1;

    private function region(
        int $regionId,
        string $startKey,
        string $endKey,
        int $epochVersion,
    ): RegionInfo {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: $epochVersion,
            startKey: $startKey,
            endKey: $endKey,
            peers: [
                new PeerInfo(1, 1),
                new PeerInfo(2, 2),
                new PeerInfo(3, 3),
            ],
        );
    }

    /**
     * @param list<int> $storeIds
     */
    private function protoRegion(
        int $regionId,
        string $startKey,
        string $endKey,
        int $epochVersion,
        array $storeIds = [1, 2, 3],
    ): Region {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion($epochVersion);

        $peers = [];
        foreach ($storeIds as $index => $storeId) {
            $peer = new Peer();
            $peer->setId($index + 1);
            $peer->setStoreId($storeId);
            $peers[] = $peer;
        }

        $region = new Region();
        $region->setId($regionId);
        $region->setStartKey($startKey);
        $region->setEndKey($endKey);
        $region->setRegionEpoch($epoch);
        $region->setPeers($peers);

        return $region;
    }

    /**
     * @param list<Region> $regions
     */
    private function epochNotMatchPayload(array $regions): EpochNotMatch
    {
        $payload = new EpochNotMatch();
        $payload->setCurrentRegions($regions);

        return $payload;
    }

    /**
     * @param list<Region> $regions
     */
    private function epochNotMatchError(array $regions): Error
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch($this->epochNotMatchPayload($regions));

        return $error;
    }

    private function createExecutor(
        RegionCache $cache,
        RegionResolver $resolver,
        InMemoryMetrics $metrics,
        int $maxAttempts = 3,
    ): RetryExecutor {
        return new RetryExecutor(
            maxBackoffMs: 10000,
            serverBusyBudgetMs: 10000,
            regionCache: $cache,
            grpc: $this->createMock(GrpcClientInterface::class),
            regionResolver: $resolver,
            logger: new NullLogger(),
            maxAttempts: $maxAttempts,
            deadlineMs: 0,
            metrics: $metrics,
        );
    }

    private function zeroSleepClassifier(): \Closure
    {
        return static fn (TiKvException $e): BackoffType => BackoffType::None;
    }

    /**
     * @return array{RegionCache, InMemoryMetrics, PdClientInterface&MockObject, RegionResolver}
     */
    private function cacheWithStaleRegion(RegionInfo $stale): array
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($stale);

        $pdClient = $this->createMock(PdClientInterface::class);
        $resolver = new RegionResolver($pdClient, $cache, logger: new NullLogger());

        return [$cache, $metrics, $pdClient, $resolver];
    }

    public function testCurrentRegionsRefreshTheCacheWithoutPdLookup(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        [$cache, $metrics, $pdClient, $resolver] = $this->cacheWithStaleRegion($stale);
        $pdClient->expects($this->never())->method('getRegion');

        $error = RegionException::fromRegionError($this->epochNotMatchError([
            $this->protoRegion(2, 'a', 'm', 2),
            $this->protoRegion(3, 'm', 'z', 2),
        ]));

        $calls = 0;
        $result = $this->createExecutor($cache, $resolver, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $resolver, $error): RegionInfo {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return $resolver->getRegionInfo(self::KEY);
            },
            $this->zeroSleepClassifier(),
        );

        self::assertSame(2, $calls, 'the retry must succeed on the second attempt');
        self::assertSame(2, $result->regionId, 'the key must route to the post-split left region');
        self::assertNull($cache->getById(self::REGION_ID), 'the stale region must be superseded');
        self::assertSame(
            0,
            $metrics->getInvalidations('retry_region_error:epoch_not_match'),
            'a cache refresh from current_regions is not an error-driven invalidation',
        );

        $right = $cache->getByKey('m');
        self::assertInstanceOf(RegionInfo::class, $right);
        self::assertSame(3, $right->regionId, 'every returned region must be cached');
    }

    /**
     * TiKV splits keep the original region ID for the left half and only
     * assign a new ID to the right half. This is the real shape of a split,
     * and the only test that exercises the same-ID, strictly-newer-epoch
     * acceptance branch of `isNewerEpoch()`.
     */
    public function testSameIdRegionWithNewerEpochIsAcceptedAndRefreshed(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        [$cache, $metrics, $pdClient, $resolver] = $this->cacheWithStaleRegion($stale);
        $pdClient->expects($this->never())->method('getRegion');

        $error = RegionException::fromRegionError($this->epochNotMatchError([
            $this->protoRegion(self::REGION_ID, 'a', 'm', 2),
            $this->protoRegion(2, 'm', 'z', 2),
        ]));

        $calls = 0;
        $result = $this->createExecutor($cache, $resolver, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $resolver, $error): RegionInfo {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return $resolver->getRegionInfo(self::KEY);
            },
            $this->zeroSleepClassifier(),
        );

        self::assertSame(2, $calls);
        self::assertSame(self::REGION_ID, $result->regionId, 'the left half keeps the original region ID');
        self::assertSame(2, $result->epochVersion, 'the refreshed left half carries the newer epoch');
        self::assertSame(0, $metrics->getInvalidations('retry_region_error:epoch_not_match'));

        $right = $cache->getByKey('m');
        self::assertInstanceOf(RegionInfo::class, $right);
        self::assertSame(2, $right->regionId);
        self::assertSame(2, $right->epochVersion);
    }

    public function testEmptyCurrentRegionsStillInvalidateAndFallBackToPd(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        [$cache, $metrics, $pdClient, $resolver] = $this->cacheWithStaleRegion($stale);
        $fresh = $this->region(9, 'a', 'z', 2);
        $pdClient->expects($this->once())
            ->method('getRegion')
            ->with(self::KEY)
            ->willReturn($fresh);

        $error = RegionException::fromRegionError($this->epochNotMatchError([]));

        $calls = 0;
        $result = $this->createExecutor($cache, $resolver, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $resolver, $error): RegionInfo {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return $resolver->getRegionInfo(self::KEY);
            },
            $this->zeroSleepClassifier(),
        );

        self::assertSame(2, $calls);
        self::assertSame(9, $result->regionId);
        self::assertSame(
            1,
            $metrics->getInvalidations('retry_region_error:epoch_not_match'),
            'the empty-payload fallback must keep the #245 invalidation metric',
        );
    }

    public function testCurrentRegionsThatDoNotCoverTheKeyFallBackToPd(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        [$cache, $metrics, $pdClient, $resolver] = $this->cacheWithStaleRegion($stale);
        $fresh = $this->region(9, 'a', 'z', 2);
        $pdClient->expects($this->once())
            ->method('getRegion')
            ->with(self::KEY)
            ->willReturn($fresh);

        $error = RegionException::fromRegionError($this->epochNotMatchError([
            $this->protoRegion(2, 'b', 'c', 2),
        ]));

        $calls = 0;
        $result = $this->createExecutor($cache, $resolver, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $resolver, $error): RegionInfo {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return $resolver->getRegionInfo(self::KEY);
            },
            $this->zeroSleepClassifier(),
        );

        self::assertSame(2, $calls);
        self::assertSame(9, $result->regionId, 'the PD answer must be used when the payload does not cover the key');
        self::assertNull($cache->getById(2), 'an unusable payload must not be cached');
        self::assertSame(1, $metrics->getInvalidations('retry_region_error:epoch_not_match'));
    }

    public function testOlderEpochForTheSameRegionIdFallsBackToPd(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 5);
        [$cache, $metrics, $pdClient, $resolver] = $this->cacheWithStaleRegion($stale);
        $fresh = $this->region(9, 'a', 'z', 6);
        $pdClient->expects($this->once())
            ->method('getRegion')
            ->with(self::KEY)
            ->willReturn($fresh);

        // A payload whose same-ID region is older than the cached one would
        // be ignored by RegionCache::put() (issue #249), so accepting it
        // would retry the same stale routing information forever.
        $error = RegionException::fromRegionError($this->epochNotMatchError([
            $this->protoRegion(self::REGION_ID, 'a', 'z', 4),
        ]));

        $calls = 0;
        $result = $this->createExecutor($cache, $resolver, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $resolver, $error): RegionInfo {
                $calls++;
                if ($calls === 1) {
                    throw $error;
                }

                return $resolver->getRegionInfo(self::KEY);
            },
            $this->zeroSleepClassifier(),
        );

        self::assertSame(2, $calls);
        self::assertSame(9, $result->regionId);
        self::assertSame(1, $metrics->getInvalidations('retry_region_error:epoch_not_match'));
    }

    public function testCheckCapturesThePayloadAndStaleRegionBeforeInvalidating(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        [$cache, $metrics, $pdClient, $resolver] = $this->cacheWithStaleRegion($stale);
        $pdClient->expects($this->never())->method('getRegion');

        $response = new RawGetResponse();
        $response->setRegionError($this->epochNotMatchError([
            $this->protoRegion(2, 'a', 'm', 2),
            $this->protoRegion(3, 'm', 'z', 2),
        ]));

        $caught = null;
        try {
            RegionErrorHandler::check($response, $cache, self::REGION_ID);
        } catch (RegionException $e) {
            $caught = $e;
        }

        self::assertInstanceOf(RegionException::class, $caught);
        $payload = $caught->epochNotMatch;
        self::assertInstanceOf(EpochNotMatch::class, $payload);
        self::assertCount(2, $payload->getCurrentRegions());
        self::assertSame($stale, $caught->cachedRegion);
        self::assertNull($cache->getById(self::REGION_ID), 'check() still drops the stale entry');
        self::assertSame(1, $metrics->getInvalidations('region_error:epoch_not_match'));
        self::assertSame(
            0,
            $metrics->getInvalidations('retry_region_error:epoch_not_match'),
            'the executor must not count the check() drop a second time',
        );

        $calls = 0;
        $result = $this->createExecutor($cache, $resolver, $metrics)->execute(
            self::KEY,
            function () use (&$calls, $resolver, $caught): RegionInfo {
                $calls++;
                if ($calls === 1) {
                    throw $caught;
                }

                return $resolver->getRegionInfo(self::KEY);
            },
            $this->zeroSleepClassifier(),
        );

        self::assertSame(2, $calls);
        self::assertSame(2, $result->regionId);
    }

    public function testRegionsFromEpochNotMatchDecodesTxnRegionBoundaries(): void
    {
        $codec = new CodecV1(Mode::Txn);
        $mce = new MemComparableCodec();
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        $payload = $this->epochNotMatchPayload([
            $this->protoRegion(2, $mce->encode('a'), $mce->encode('m'), 2),
        ]);

        $resolver = new RegionResolver(
            $this->createMock(PdClientInterface::class),
            new RegionCache(),
            logger: new NullLogger(),
            codec: $codec,
        );

        $regions = $resolver->regionsFromEpochNotMatch($payload, $stale);

        self::assertCount(1, $regions);
        self::assertSame('a', $regions[0]->startKey);
        self::assertSame('m', $regions[0]->endKey);
        self::assertSame(1, $regions[0]->leaderStoreId);
        self::assertSame(1, $regions[0]->leaderPeerId);
    }

    public function testRegionsFromEpochNotMatchRejectsAPayloadWithoutTheOldLeaderPeer(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        $payload = $this->epochNotMatchPayload([
            // The old leader store (1) is gone, so the new region's leader is
            // unknown and caching it would only fail with StoreNotFound.
            $this->protoRegion(2, 'a', 'm', 2, [2, 3]),
        ]);

        $resolver = new RegionResolver(
            $this->createMock(PdClientInterface::class),
            new RegionCache(),
            logger: new NullLogger(),
        );

        self::assertSame([], $resolver->regionsFromEpochNotMatch($payload, $stale));
    }

    public function testRegionsFromEpochNotMatchRejectsUndecodableBoundaries(): void
    {
        $stale = $this->region(self::REGION_ID, 'a', 'z', 1);
        $payload = $this->epochNotMatchPayload([
            $this->protoRegion(2, 'not-an-mce-group', 'also-not-mce', 2),
        ]);

        $resolver = new RegionResolver(
            $this->createMock(PdClientInterface::class),
            new RegionCache(),
            logger: new NullLogger(),
            codec: new CodecV1(Mode::Txn),
        );

        self::assertSame([], $resolver->regionsFromEpochNotMatch($payload, $stale));
    }
}
