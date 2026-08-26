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
}
