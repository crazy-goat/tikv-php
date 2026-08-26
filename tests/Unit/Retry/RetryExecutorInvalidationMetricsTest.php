<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Issue #474: RegionCache::invalidate() is the single emission point for the
 * regionInvalidated() metric. These tests drive RetryExecutor against a REAL
 * RegionCache wired to an InMemoryMetrics so the choke-point emission is
 * proven end to end — a mocked RegionCacheInterface would bypass
 * RegionCache::invalidate() entirely and prove nothing about counting.
 */
class RetryExecutorInvalidationMetricsTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;

    private PdClientInterface&MockObject $pdClient;

    private LoggerInterface&MockObject $logger;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);
    }

    private function createExecutor(RegionCache $cache): RetryExecutor
    {
        return new RetryExecutor(
            maxBackoffMs: 63,
            serverBusyBudgetMs: 10000,
            regionCache: $cache,
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, $cache),
            logger: $this->logger,
            metrics: $cache->metrics() ?? new InMemoryMetrics(),
        );
    }

    /**
     * @param list<PeerInfo> $peers
     */
    private function makeRegion(int $id, int $leaderStoreId = 1, array $peers = []): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            peers: $peers,
        );
    }

    public function testNotLeaderWithoutHintEmitsExactlyOnceWithNotLeaderReason(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(42));
        $executor = $this->createExecutor($cache);

        // NotLeader WITHOUT a hint: handleNotLeader() must drop the region
        // (reason 'not_leader'). The operation then succeeds, so the drop
        // happens exactly once for this execute() call.
        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);
        $error = new RegionException(
            operation: 'KvGet',
            message: 'not leader',
            notLeader: $notLeader,
        );

        $calls = 0;
        $result = $executor->execute('some_key', function () use (&$calls, $error): string {
            $calls++;
            if ($calls === 1) {
                throw $error;
            }

            return 'ok';
        });

        $this->assertSame('ok', $result);
        $this->assertGreaterThanOrEqual(2, $calls, 'NotLeader must be classified retryable');
        $this->assertSame(1, $metrics->getInvalidations('not_leader'));
        $this->assertSame(0, $metrics->getInvalidations('retry_region_error'), 'Must not double-count as region_error');
    }

    public function testNotLeaderWithKnownHintPeerSwitchesLeaderWithoutEmitting(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $hintPeer = new PeerInfo(peerId: 20, storeId: 3);
        $cache->put($this->makeRegion(42, peers: [$hintPeer]));
        $executor = $this->createExecutor($cache);

        // Hint peer IS among the cached peers: only switchLeader() runs —
        // no invalidation, so regionInvalidated() must NOT be emitted.
        $protoLeader = new Peer();
        $protoLeader->setId(20);
        $protoLeader->setStoreId(3);
        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);
        $notLeader->setLeader($protoLeader);
        $error = new RegionException(
            operation: 'KvGet',
            message: 'not leader',
            notLeader: $notLeader,
        );

        $calls = 0;
        $result = $executor->execute('some_key', function () use (&$calls, $error): string {
            $calls++;
            if ($calls === 1) {
                throw $error;
            }

            return 'ok';
        });

        $this->assertSame('ok', $result);
        $this->assertSame(0, $metrics->getInvalidations('not_leader'), 'Leader switch must not emit an invalidation');
        $this->assertSame(0, $metrics->getInvalidations('retry_region_error'));
        $switched = $cache->getByKey('some_key');
        $this->assertInstanceOf(RegionInfo::class, $switched, 'Region must stay cached after a leader switch');
        $this->assertSame(3, $switched->leaderStoreId, 'Cached leader must point at the hinted peer');
    }

    public function testRetryableRegionErrorEmitsExactlyOnceAsRetryRegionError(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(7));
        $executor = $this->createExecutor($cache);

        // EpochNotMatch classifies as BackoffType::None: RetryExecutor
        // invalidates the cached region before scheduling the next attempt.
        // The cache emits it as 'retry_region_error' — and ONLY the cache
        // emits, so the counter must land on exactly 1 (no double-count).
        $calls = 0;
        $result = $executor->execute('some_key', function () use (&$calls): string {
            $calls++;
            if ($calls === 1) {
                throw new TiKvException('EpochNotMatch something');
            }

            return 'ok';
        });

        $this->assertSame('ok', $result);
        $this->assertSame(1, $metrics->getInvalidations('retry_region_error'));
        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
    }

    public function testUncachedKeyOnRetryableErrorEmitsNothing(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $executor = $this->createExecutor($cache);

        // Nothing cached under the key: the invalidation branch never finds
        // a region, so nothing may be counted.
        try {
            $executor->execute('empty_key', function (): string {
                throw new TiKvException('EpochNotMatch something');
            });
        } catch (TiKvException) {
            // expected — budget exhausts while the error keeps repeating
        }

        $this->assertSame(0, $metrics->getInvalidations('retry_region_error'));
        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
    }

    // ========================================================================
    // Composed paths (issue #474 review fixes)
    //
    // The operation closures below mirror real call sites: they run
    // RegionErrorHandler::check() on a response-shaped object and let the
    // RegionException propagate into the retry loop.
    // ========================================================================

    private function makeNotLeaderResponse(int $regionId, ?Peer $hint): object
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId($regionId);
        if ($hint instanceof Peer) {
            $notLeader->setLeader($hint);
        }
        $error = new \CrazyGoat\Proto\Errorpb\Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        return new class ($error) {
            public function __construct(private readonly \CrazyGoat\Proto\Errorpb\Error $error)
            {
            }

            public function getRegionError(): \CrazyGoat\Proto\Errorpb\Error
            {
                return $this->error;
            }
        };
    }

    /**
     * Regression for the reviewer-confirmed double emission: a NotLeader
     * region error flowing through RegionErrorHandler::check() inside the
     * retried closure used to invalidate once from check() AND once more
     * from handleNotLeader() (region already dropped → switchLeader false →
     * invalidate) on EVERY attempt — 54 emissions for this scenario. Now
     * check() skips NotLeader oneofs entirely and handleNotLeader is the
     * sole owner, so the whole storm counts exactly ONE actual drop.
     */
    public function testNotLeaderThroughCheckEmitsOnceTotalAcrossAllAttempts(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(42));
        $executor = $this->createExecutor($cache);

        $response = $this->makeNotLeaderResponse(42, null);
        $calls = 0;
        try {
            $executor->execute('some_key', function () use (&$calls, $cache, $response): string {
                $calls++;
                // Mirrors TxnReader/LockResolver/TwoPhaseCommitter call
                // sites: check() throws the RegionException itself.
                RegionErrorHandler::check($response, $cache, 42);

                return 'ok';
            });
            $this->fail('Expected the retry budget to exhaust');
        } catch (RegionException) {
            // expected — original error rethrown when maxBackoffMs crosses
        }

        $this->assertGreaterThan(2, $calls, 'NotLeader must be retried multiple times');
        $this->assertSame(
            1,
            $metrics->getInvalidations('not_leader'),
            'Sole-owner rule: one storm = one actual drop, regardless of attempts',
        );
        $this->assertSame(0, $metrics->getInvalidations('retry_region_error'));
    }

    /**
     * Valid-hint composition: check() must leave the region cached so
     * handleNotLeader can switch to the hinted peer — zero emissions, and
     * the cached leader actually moves to the hint store.
     */
    public function testValidHintThroughCheckSwitchesLeaderWithoutEmittingAcrossAttempts(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(42, peers: [new PeerInfo(peerId: 20, storeId: 3)]));
        $executor = $this->createExecutor($cache);

        $protoLeader = new Peer();
        $protoLeader->setId(20);
        $protoLeader->setStoreId(3);
        $response = $this->makeNotLeaderResponse(42, $protoLeader);

        $calls = 0;
        try {
            $executor->execute('some_key', function () use (&$calls, $cache, $response): string {
                $calls++;
                RegionErrorHandler::check($response, $cache, 42);

                return 'ok';
            });
            $this->fail('Expected the retry budget to exhaust');
        } catch (RegionException) {
            // expected
        }

        $this->assertGreaterThan(2, $calls);
        $this->assertSame(
            0,
            $metrics->getInvalidations('not_leader'),
            'Leader switching must not count as invalidation',
        );
        $this->assertSame(0, $metrics->getInvalidations('retry_region_error'));
        $switched = $cache->getByKey('some_key');
        $this->assertInstanceOf(RegionInfo::class, $switched, 'Region stays cached across the whole storm');
        $this->assertSame(3, $switched->leaderStoreId, 'Cached leader points at the hinted peer');
    }

    /**
     * Retry storm on a non-NotLeader region error: only the FIRST attempt
     * finds the cached region and drops it (one 'retry_region_error');
     * later attempts find nothing to remove and emit nothing.
     */
    public function testRetryStormCountsSingleRetryRegionErrorEmission(): void
    {
        $metrics = new InMemoryMetrics();
        $cache = new RegionCache(metrics: $metrics);
        $cache->put($this->makeRegion(7));
        $executor = $this->createExecutor($cache);

        $calls = 0;
        try {
            $executor->execute('some_key', function () use (&$calls): string {
                $calls++;
                throw new TiKvException('EpochNotMatch something');
            });
        } catch (TiKvException) {
            // expected — attempt cap exhausts while the error repeats
        }

        $this->assertGreaterThan(2, $calls, 'EpochNotMatch retries until the attempt cap');
        $this->assertSame(
            1,
            $metrics->getInvalidations('retry_region_error'),
            'Only the first drop removes a region; the rest of the storm emits nothing',
        );
        $this->assertSame(0, $metrics->getInvalidations('not_leader'));
    }
}
