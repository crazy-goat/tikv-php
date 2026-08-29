<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Errorpb\RegionNotFound;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #338 (TEST-18) acceptance criteria 2 and 3:
 *
 * - AC-2: a heartbeat whose response carries a top-level region error must
 *   invalidate the cached region (or switch its leader, when the NotLeader
 *   hint is still valid) and be retried until it succeeds.
 * - AC-3: the advised TTL sent on the wire and the granted TTL returned to
 *   the caller are distinct values — the return value is what the server
 *   granted, never what we asked for.
 *
 * The cache is the REAL RegionCache wired to an InMemoryMetrics (issue #474
 * made invalidate() the single emission point, gated on an actual removal),
 * so the tests verify the production gating — emissions, not mock call
 * counts — and a stale-mock harness cannot mask it. Companion to
 * HeartbeatKeyErrorTest (issue #492), which covers AC-1: the KeyError branch
 * of TwoPhaseCommitter::heartbeat().
 */
final class HeartbeatRegionErrorAndTtlTest extends TestCase
{
    private InMemoryMetrics $metrics;

    private RegionCache $regionCache;

    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    /** @var list<array{address: string, method: string, request: object}> */
    private array $rpcCalls = [];

    /** The region PD hands out on cache misses (overridable per test). */
    private RegionInfo $pdRegion;

    protected function setUp(): void
    {
        $this->metrics = new InMemoryMetrics();
        $this->regionCache = new RegionCache(metrics: $this->metrics);
        $this->rpcCalls = [];

        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);

        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress('127.0.0.1:20160');
        $store7 = new Store();
        $store7->setId(7);
        $store7->setAddress('127.0.0.1:20167');

        $this->pdClient->method('getStore')->willReturnCallback(
            static fn (int $storeId): Store => 7 === $storeId ? $store7 : $store1,
        );
        $this->pdClient->method('getRegion')->willReturnCallback(
            fn(string $key): RegionInfo => $this->pdRegion,
        );
    }

    private function makeRegion(bool $withHintedPeer): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: $withHintedPeer ? [new PeerInfo(peerId: 7, storeId: 7)] : [],
        );
    }

    private function createTransaction(): Transaction
    {
        $lockResolver = new LockResolver(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache, $this->metrics),
            $this->regionCache,
            $this->pdClient,
            1000,
        );

        return new Transaction(
            txnId: 'test-txn-1',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $lockResolver,
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache, $this->metrics),
            maxBackoffMs: 20000,
            retryDeadlineMs: 3000,
        );
    }

    private function mockGrpcResponses(TxnHeartBeatResponse ...$responses): void
    {
        $rpcCalls = &$this->rpcCalls;
        $index = 0;
        $this->grpc->method('call')->willReturnCallback(
            static function (
                string $address,
                string $service,
                string $method,
                object $request,
            ) use (
                &$rpcCalls,
                &$index,
                $responses,
            ): TxnHeartBeatResponse {
                $rpcCalls[] = ['address' => $address, 'method' => $method, 'request' => $request];

                $response = $responses[min($index, count($responses) - 1)];
                ++$index;

                return $response;
            },
        );
    }

    private function notLeaderResponse(int $hintStoreId): TxnHeartBeatResponse
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $hint = new Peer();
        $hint->setStoreId($hintStoreId);
        $notLeader->setLeader($hint);

        $regionError = new Error();
        $regionError->setMessage('not leader');
        $regionError->setNotLeader($notLeader);

        $response = new TxnHeartBeatResponse();
        $response->setRegionError($regionError);

        return $response;
    }

    private function regionNotFoundResponse(): TxnHeartBeatResponse
    {
        $regionError = new Error();
        $regionError->setMessage('region not found');
        $regionError->setRegionNotFound(new RegionNotFound());

        $response = new TxnHeartBeatResponse();
        $response->setRegionError($regionError);

        return $response;
    }

    private function okResponse(int $grantedTtl): TxnHeartBeatResponse
    {
        $response = new TxnHeartBeatResponse();
        $response->setLockTtl($grantedTtl);

        return $response;
    }

    private function heartbeat(): int
    {
        $txn = $this->createTransaction();
        $txn->set('key1', 'value1');

        return $txn->heartbeat(10000);
    }

    /**
     * AC-2, NotLeader with a STILL-VALID hint. RegionErrorHandler::check()
     * throws before the KeyError branch is consulted but leaves the NotLeader
     * drop to RetryExecutor::handleNotLeader() (issue #475 ownership); the
     * executor consults the hint via RegionCache::switchLeader() — store 7 is
     * among the cached peers, so the leader is switched IN PLACE and nothing
     * is invalidated (a valid-hint switch emits no regionInvalidated metric).
     * The retry re-resolves from the cache and lands on the hinted leader's
     * store address.
     */
    public function testHeartbeatNotLeaderSwitchesToHintedLeaderAndRetries(): void
    {
        $this->pdRegion = $this->makeRegion(withHintedPeer: true);
        $this->mockGrpcResponses(
            $this->notLeaderResponse(hintStoreId: 7),
            $this->okResponse(grantedTtl: 9500),
        );

        self::assertSame(9500, $this->heartbeat());

        // Two heartbeat RPCs: the first to the PD-resolved leader's store,
        // the retry to the hinted leader's store the cache was switched to.
        self::assertSame(
            [
                ['address' => '127.0.0.1:20160', 'method' => 'KvTxnHeartBeat'],
                ['address' => '127.0.0.1:20167', 'method' => 'KvTxnHeartBeat'],
            ],
            array_map(
                static fn (array $call): array => [
                    'address' => $call['address'],
                    'method' => $call['method'],
                ],
                $this->rpcCalls,
            ),
        );

        // A valid-hint leader switch is NOT a drop: no invalidation of any
        // kind was emitted (issue #474 contract).
        self::assertSame(0, $this->metrics->getInvalidations('not_leader'));
        self::assertSame(0, $this->metrics->getInvalidations('region_error'));
        self::assertSame(0, $this->metrics->getInvalidations('retry_region_error'));
    }

    /**
     * AC-2, NotLeader with a hint the cache cannot honour. switchLeader()
     * returns false (region has no peers), so the executor drops the region
     * itself with reason 'not_leader' — NOT 'region_error', which would mean
     * RegionErrorHandler::check() had handled the NotLeader oneof and
     * violated the #475 ownership split — and the retry re-resolves from PD.
     */
    public function testHeartbeatNotLeaderWithUnknownHintDropsRegionAndRetries(): void
    {
        $this->pdRegion = $this->makeRegion(withHintedPeer: false);
        $this->mockGrpcResponses(
            $this->notLeaderResponse(hintStoreId: 7),
            $this->okResponse(grantedTtl: 9500),
        );

        self::assertSame(9500, $this->heartbeat());

        self::assertCount(2, $this->rpcCalls);
        self::assertSame('KvTxnHeartBeat', $this->rpcCalls[0]['method']);
        self::assertSame('KvTxnHeartBeat', $this->rpcCalls[1]['method']);
        // The dropped region was re-resolved from PD (store 1 again).
        self::assertSame('127.0.0.1:20160', $this->rpcCalls[1]['address']);

        self::assertSame(1, $this->metrics->getInvalidations('not_leader'));
        self::assertSame(0, $this->metrics->getInvalidations('region_error'));
        self::assertSame(0, $this->metrics->getInvalidations('retry_region_error'));
    }

    /**
     * AC-2, non-NotLeader variant. RegionErrorHandler::check() owns this
     * drop and invalidates with reason 'region_error'; the RegionException
     * then flows to the RetryExecutor, whose classifier maps RegionNotFound
     * to RegionMiss backoff (2 ms base, 500 ms cap — fast), and the
     * executor's pre-retry invalidation is GATED on a cached entry: check()
     * already removed region 1, so getByKey() misses and no
     * 'retry_region_error' emission occurs. The second attempt re-resolves
     * from PD and succeeds.
     */
    public function testHeartbeatRegionErrorInvalidatesRegionAndRetries(): void
    {
        $this->pdRegion = $this->makeRegion(withHintedPeer: false);
        $this->mockGrpcResponses(
            $this->regionNotFoundResponse(),
            $this->okResponse(grantedTtl: 9500),
        );

        self::assertSame(9500, $this->heartbeat());

        self::assertCount(2, $this->rpcCalls);
        self::assertSame('KvTxnHeartBeat', $this->rpcCalls[0]['method']);
        self::assertSame('KvTxnHeartBeat', $this->rpcCalls[1]['method']);

        self::assertSame(1, $this->metrics->getInvalidations('region_error'));
        self::assertSame(0, $this->metrics->getInvalidations('retry_region_error'));
        self::assertSame(0, $this->metrics->getInvalidations('not_leader'));
    }

    /**
     * AC-3. The advised TTL goes out on the wire and the granted TTL — a
     * different value — is what heartbeat() returns. A clean success emits
     * no invalidation.
     */
    public function testHeartbeatSendsAdvisedTtlAndReturnsGrantedTtl(): void
    {
        $this->pdRegion = $this->makeRegion(withHintedPeer: false);
        $this->mockGrpcResponses($this->okResponse(grantedTtl: 12345));

        $granted = $this->heartbeat();

        self::assertCount(1, $this->rpcCalls);
        self::assertSame('KvTxnHeartBeat', $this->rpcCalls[0]['method']);

        /** @var TxnHeartBeatRequest $request */
        $request = $this->rpcCalls[0]['request'];
        self::assertInstanceOf(TxnHeartBeatRequest::class, $request);
        self::assertSame(10000, (int) $request->getAdviseLockTtl());
        self::assertSame(1000, (int) $request->getStartVersion());
        self::assertSame('key1', $request->getPrimaryLock());

        self::assertSame(12345, $granted);
        self::assertNotSame(10000, $granted, 'granted TTL must not echo the advised TTL');

        self::assertSame(0, $this->metrics->getInvalidations('not_leader'));
        self::assertSame(0, $this->metrics->getInvalidations('region_error'));
        self::assertSame(0, $this->metrics->getInvalidations('retry_region_error'));
    }
}
