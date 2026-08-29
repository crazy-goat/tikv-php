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
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
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
 *   invalidate the cached region and be retried until it succeeds.
 * - AC-3: the advised TTL sent on the wire and the granted TTL returned to
 *   the caller are distinct values — the return value is what the server
 *   granted, never what we asked for.
 *
 * Companion to HeartbeatKeyErrorTest (issue #492), which covers AC-1: the
 * KeyError branch of TwoPhaseCommitter::heartbeat().
 */
final class HeartbeatRegionErrorAndTtlTest extends TestCase
{
    /** @var list<array{regionId: int, reason: string}> */
    private array $invalidations = [];

    /** @var list<array{regionId: int, storeId: int}> */
    private array $switchLeaderCalls = [];

    /** @var list<array{address: string, method: string, request: object}> */
    private array $rpcCalls = [];

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionInfo $testRegion;

    protected function setUp(): void
    {
        $this->invalidations = [];
        $this->switchLeaderCalls = [];
        $this->rpcCalls = [];

        $this->testRegion = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );

        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($this->testRegion);
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $invalidations = &$this->invalidations;
        $this->regionCache->method('invalidate')->willReturnCallback(
            static function (int $regionId, string $reason = 'region_error') use (&$invalidations): void {
                $invalidations[] = ['regionId' => $regionId, 'reason' => $reason];
            },
        );

        $switchLeaderCalls = &$this->switchLeaderCalls;
        $this->regionCache->method('switchLeader')->willReturnCallback(
            static function (int $regionId, int $leaderStoreId) use (&$switchLeaderCalls): bool {
                $switchLeaderCalls[] = ['regionId' => $regionId, 'storeId' => $leaderStoreId];

                // The mocked cache has no peers, so the hinted store can
                // never be found — the executor then falls back to a drop.
                return false;
            },
        );
    }

    private function createTransaction(): Transaction
    {
        $lockResolver = new LockResolver(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
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
            regionResolver: new RegionResolver($this->pdClient, $this->regionCache),
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

    private function regionErrorResponse(Error $regionError): TxnHeartBeatResponse
    {
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
     * AC-2, NotLeader variant. RegionErrorHandler::check() throws before
     * the KeyError branch is ever consulted, but leaves the NotLeader drop
     * to RetryExecutor::handleNotLeader() (issue #475 ownership): the
     * executor consults the leader hint via switchLeader() and — the
     * hinted store being unknown to the cache — drops the region itself.
     * The second attempt succeeds; a successful attempt emits nothing.
     */
    public function testHeartbeatNotLeaderSwitchesLeaderAndRetries(): void
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $hint = new Peer();
        $hint->setStoreId(7);
        $notLeader->setLeader($hint);

        $regionError = new Error();
        $regionError->setMessage('not leader');
        $regionError->setNotLeader($notLeader);

        $this->mockGrpcResponses(
            $this->regionErrorResponse($regionError),
            $this->okResponse(9500),
        );

        self::assertSame(9500, $this->heartbeat());

        // Exactly one RPC per attempt, both heartbeats.
        self::assertSame(
            ['KvTxnHeartBeat', 'KvTxnHeartBeat'],
            array_column($this->rpcCalls, 'method'),
        );

        // The executor followed the leader hint from the region error.
        self::assertSame(
            [['regionId' => 1, 'storeId' => 7]],
            $this->switchLeaderCalls,
        );

        // Hint store unknown to the cache → executor-owned drop, and no
        // plain 'region_error' drop from RegionErrorHandler::check().
        self::assertSame(
            [['regionId' => 1, 'reason' => 'not_leader']],
            $this->invalidations,
        );
    }

    /**
     * AC-2, non-NotLeader variant. RegionErrorHandler::check() owns this
     * drop (reason 'region_error'); the RegionException then flows to
     * RetryExecutor, whose classifier maps RegionNotFound to RegionMiss
     * backoff (2 ms base, 500 ms cap — fast), and the executor drops the
     * cached region again with its own reason 'retry_region_error'
     * (issue #475: two distinct owners, two emissions). The second attempt
     * succeeds.
     */
    public function testHeartbeatRegionErrorInvalidatesRegionAndRetries(): void
    {
        $regionError = new Error();
        $regionError->setMessage('region not found');
        $regionError->setRegionNotFound(new RegionNotFound());

        $this->mockGrpcResponses(
            $this->regionErrorResponse($regionError),
            $this->okResponse(9500),
        );

        self::assertSame(9500, $this->heartbeat());

        self::assertSame(
            ['KvTxnHeartBeat', 'KvTxnHeartBeat'],
            array_column($this->rpcCalls, 'method'),
        );

        self::assertSame(
            [
                ['regionId' => 1, 'reason' => 'region_error'],
                ['regionId' => 1, 'reason' => 'retry_region_error'],
            ],
            $this->invalidations,
        );
    }

    /**
     * AC-3. The advised TTL goes out on the wire and the granted TTL — a
     * different value — is what heartbeat() returns.
     */
    public function testHeartbeatSendsAdvisedTtlAndReturnsGrantedTtl(): void
    {
        $this->mockGrpcResponses($this->okResponse(12345));

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
    }
}
