<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Observability\InMemoryTracer;
use CrazyGoat\TiKV\Client\Observability\TraceCategory;
use CrazyGoat\TiKV\Client\Observability\TraceEventInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Tests\Unit\Support\ThrowingTracer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Two-phase-commit observability (issue #434, GAP-20): phase timings on
 * DetailedMetricsInterface and structured trace events on TraceEventInterface.
 */
class TwoPhaseObservabilityTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private const START_TS = 1000;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
    }

    private function stubSingleRegion(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn (string $key): ?RegionInfo => KeyOrder::inRange($key, '', '') ? $region : null,
        );
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getTimestamp')->willReturn(2000);
    }

    private function mockGrpc(PrewriteResponse $prewriteResponse): void
    {
        $this->grpc->method('call')->willReturnCallback(
            static fn(
                string $address,
                string $service,
                string $method,
                ?object $request = null,
            ): object => match ($method) {
                'KvPrewrite' => $prewriteResponse,
                'KvCommit' => new CommitResponse(),
                'KvBatchRollback' => new BatchRollbackResponse(),
                default => throw new \RuntimeException("Unexpected method: $method"),
            },
        );
    }

    private function createTransaction(
        InMemoryMetrics $metrics,
        TraceEventInterface $tracer,
        bool $enable1Pc = false,
    ): Transaction {
        return new Transaction(
            txnId: 'observability-txn',
            startTs: self::START_TS,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver(
                $this->grpc,
                $this->regionResolver,
                $this->regionCache,
                $this->pdClient,
                self::START_TS,
            ),
            regionResolver: $this->regionResolver,
            metrics: $metrics,
            enable1Pc: $enable1Pc,
            tracer: $tracer,
        );
    }

    public function testTwoPhaseCommitRecordsPrewriteAndCommitTimings(): void
    {
        $this->stubSingleRegion();
        $response = new PrewriteResponse();
        $this->mockGrpc($response);

        $metrics = new InMemoryMetrics();
        $tracer = new InMemoryTracer();
        $txn = $this->createTransaction($metrics, $tracer);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(1, $metrics->getTwoPhaseCount('prewrite'));
        $this->assertSame(1, $metrics->getTwoPhaseCount('commit'));
        $this->assertSame(0, $metrics->getTwoPhaseFailures('prewrite'));
        $this->assertSame(0, $metrics->getTwoPhaseFailures('commit'));

        $this->assertSame(
            [
                'prewrite_started',
                'prewrite_completed',
                'commit_started',
                'commit_completed',
            ],
            $tracer->eventNames(TraceCategory::TXN_2PC),
        );

        $events = $tracer->getEvents();
        $this->assertSame($events[0]['fields']['eventId'], $events[1]['fields']['eventId']);
        $this->assertSame($events[2]['fields']['eventId'], $events[3]['fields']['eventId']);
        $this->assertNotSame($events[0]['fields']['eventId'], $events[2]['fields']['eventId']);
    }

    public function testOnePhaseCommitRecordsOnlyPrewritePhase(): void
    {
        $this->stubSingleRegion();
        $response = new PrewriteResponse();
        $response->setOnePcCommitTs(5000);
        $this->mockGrpc($response);

        $metrics = new InMemoryMetrics();
        $tracer = new InMemoryTracer();
        $txn = $this->createTransaction($metrics, $tracer, enable1Pc: true);
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(1, $metrics->getTwoPhaseCount('prewrite'));
        $this->assertSame(0, $metrics->getTwoPhaseCount('commit'));
        $this->assertSame(
            ['prewrite_started', 'prewrite_completed', 'one_pc_completed'],
            $tracer->eventNames(TraceCategory::TXN_2PC),
        );

        $events = $tracer->getEvents();
        $this->assertSame($events[0]['fields']['eventId'], $events[1]['fields']['eventId']);
        $this->assertSame($events[0]['fields']['eventId'], $events[2]['fields']['eventId']);
    }

    public function testTxnKvClientPropagatesTracerToTransactions(): void
    {
        $this->stubSingleRegion();
        $this->mockGrpc(new PrewriteResponse());

        $metrics = new InMemoryMetrics();
        $tracer = new InMemoryTracer();
        $client = new TxnKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            metrics: $metrics,
            tracer: $tracer,
        );

        $txn = $client->beginOptimistic();
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(
            [
                'prewrite_started',
                'prewrite_completed',
                'commit_started',
                'commit_completed',
            ],
            $tracer->eventNames(TraceCategory::TXN_2PC),
        );
        $this->assertSame(1, $metrics->getTwoPhaseCount('prewrite'));
        $this->assertSame(1, $metrics->getTwoPhaseCount('commit'));
    }

    public function testPrewriteFailureRecordsFailedPhase(): void
    {
        $this->stubSingleRegion();
        $this->grpc->method('call')->willReturnCallback(
            static function (
                string $address,
                string $service,
                string $method,
                ?object $request = null,
            ): object {
                if ($method === 'KvPrewrite') {
                    throw new \CrazyGoat\TiKV\Client\Exception\GrpcException(
                        'prewrite failed',
                        grpcStatusCode: \CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode::Unavailable->value,
                    );
                }

                throw new \RuntimeException("Unexpected method: $method");
            },
        );

        $metrics = new InMemoryMetrics();
        $txn = $this->createTransaction($metrics, new InMemoryTracer());
        $txn->set('k1', 'v1');

        try {
            $txn->commit();
            $this->fail('commit should have thrown');
        } catch (\CrazyGoat\TiKV\Client\Exception\TiKvException) {
            // expected
        }

        $this->assertSame(1, $metrics->getTwoPhaseCount('prewrite'));
        $this->assertSame(1, $metrics->getTwoPhaseFailures('prewrite'));
        $this->assertSame(0, $metrics->getTwoPhaseCount('commit'));
    }

    public function testThrowingTracerDoesNotBreakCommit(): void
    {
        $this->stubSingleRegion();
        $this->mockGrpc(new PrewriteResponse());

        $metrics = new InMemoryMetrics();
        $txn = $this->createTransaction($metrics, new ThrowingTracer());
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertSame(1, $metrics->getTwoPhaseCount('prewrite'));
        $this->assertSame(1, $metrics->getTwoPhaseCount('commit'));
    }
}
