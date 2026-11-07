<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\Proto\Pdpb\ScanRegionsResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryBudget;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TwoPhaseCommitter;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #257 (REG-26): the retry deadline must bound the RPC deadline of
 * every operation the executor wraps, not only the prewrite/rollback calls
 * covered by issue #569.
 *
 * Each test drives a public Transaction or LockResolver entry point with a
 * retry deadline (800 ms) shorter than every configured store/metadata
 * timeout (5 s and up), then asserts the recorded gRPC deadline was clamped
 * to the live remaining budget.
 */
final class OuterRpcDeadlineClampTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private const DEADLINE_MS = 800;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;

    /** @var list<array{method: string, timeoutMs: ?int}> */
    private array $callTimeouts = [];

    /** @var \Closure(string): Message */
    private \Closure $responder;

    /** @var list<?int> Deadlines passed to PD GetRegion/ScanRegions. */
    private array $pdTimeouts = [];

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->callTimeouts = [];
        $this->pdTimeouts = [];

        $store = new Store();
        $store->setId(1);
        $store->setAddress('store-1:20160');

        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturnCallback(
            function (string $key, ?int $timeoutMs = null): RegionInfo {
                $this->pdTimeouts[] = $timeoutMs;

                return $this->region();
            },
        );
        $this->pdClient->method('scanRegions')->willReturnCallback(
            function (string $start, string $end, int $limit = 0, ?int $timeoutMs = null): array {
                $this->pdTimeouts[] = $timeoutMs;

                return [$this->region()];
            },
        );
        $this->pdClient->method('getTimestamp')->willReturn(9001);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturn(9002);

        $this->responder = static fn (string $method): Message => match ($method) {
            'KvPrewrite' => new PrewriteResponse(),
            'KvCommit' => new CommitResponse(),
            'KvGet' => new GetResponse(),
            'KvBatchGet' => new BatchGetResponse(),
            'KvScan' => new ScanResponse(),
            'KvTxnHeartBeat' => new TxnHeartBeatResponse(),
            'KvPessimisticLock' => new PessimisticLockResponse(),
            'KvCheckTxnStatus' => self::rolledBackStatus(),
            'KvResolveLock' => new ResolveLockResponse(),
            'GetRegion' => self::getRegionResponse(),
            'ScanRegions' => self::scanRegionsResponse(),
            default => throw new \RuntimeException("unexpected method {$method}"),
        };

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs = null,
            ): Message {
                $this->callTimeouts[] = ['method' => $method, 'timeoutMs' => $timeoutMs];

                return ($this->responder)($method);
            },
        );
    }

    private static function rolledBackStatus(): CheckTxnStatusResponse
    {
        $response = new CheckTxnStatusResponse();
        $response->setCommitVersion(0);
        $response->setLockTtl(0);

        return $response;
    }

    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private static function regionProto(): Region
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(1);
        $region->setRegionEpoch($epoch);

        return $region;
    }

    private static function leaderProto(): Peer
    {
        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        return $leader;
    }

    private static function getRegionResponse(): GetRegionResponse
    {
        $header = new ResponseHeader();
        $header->setClusterId(1);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion(self::regionProto());
        $response->setLeader(self::leaderProto());

        return $response;
    }

    private static function scanRegionsResponse(): ScanRegionsResponse
    {
        $header = new ResponseHeader();
        $header->setClusterId(1);

        $response = new ScanRegionsResponse();
        $response->setHeader($header);
        $response->setRegionMetas([self::regionProto()]);
        $response->setLeaders([self::leaderProto()]);

        return $response;
    }

    private function createTransaction(bool $pessimistic = false): Transaction
    {
        $region = $this->region();
        $regionCache = new RegionCache();
        $regionCache->put($region);
        $regionResolver = new RegionResolver($this->pdClient, $regionCache);
        $lockResolver = new LockResolver(
            $this->grpc,
            $regionResolver,
            $regionCache,
            $this->pdClient,
            1000,
        );

        return new Transaction(
            txnId: 'outer-rpc-deadline-txn',
            startTs: 1000,
            pessimistic: $pessimistic,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $regionCache,
            lockResolver: $lockResolver,
            regionResolver: $regionResolver,
            retryDeadlineMs: self::DEADLINE_MS,
        );
    }

    private function createCommitter(): TwoPhaseCommitter
    {
        $regionCache = new RegionCache();
        $regionResolver = new RegionResolver($this->pdClient, $regionCache);
        $lockResolver = new LockResolver(
            $this->grpc,
            $regionResolver,
            $regionCache,
            $this->pdClient,
            1000,
        );

        return new TwoPhaseCommitter(
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $regionCache,
            regionResolver: $regionResolver,
            lockResolver: $lockResolver,
            timeoutConfig: new TimeoutConfig(),
            maxBackoffMs: 20000,
        );
    }

    /**
     * @return list<array{method: string, timeoutMs: ?int}>
     */
    private function callsTo(string ...$methods): array
    {
        return array_values(array_filter(
            $this->callTimeouts,
            static fn (array $call): bool => in_array($call['method'], $methods, true),
        ));
    }

    private function assertWithinDeadline(string ...$methods): void
    {
        $calls = $this->callsTo(...$methods);
        self::assertNotEmpty($calls, 'expected at least one call to ' . implode(', ', $methods));

        foreach ($calls as $call) {
            self::assertNotNull($call['timeoutMs']);
            self::assertGreaterThan(0, $call['timeoutMs']);
            self::assertLessThanOrEqual(
                self::DEADLINE_MS,
                $call['timeoutMs'],
                sprintf('%s must be capped by the remaining retry deadline', $call['method']),
            );
        }
    }

    public function testPointReadRpcDeadlineIsCappedByTheRemainingRetryDeadline(): void
    {
        $this->createTransaction()->get('k1');

        $this->assertWithinDeadline('KvGet');
    }

    public function testBatchReadRpcDeadlineIsCappedByTheRemainingRetryDeadline(): void
    {
        $this->createTransaction()->batchGet(['k1', 'k2']);

        $this->assertWithinDeadline('KvBatchGet');
    }

    public function testScanRpcDeadlineIsCappedByTheRemainingRetryDeadline(): void
    {
        $this->createTransaction()->scan('k0', 'k9', 10);

        $this->assertWithinDeadline('KvScan');
    }

    public function testCommitRpcDeadlineIsCappedByTheRemainingRetryDeadline(): void
    {
        $txn = $this->createTransaction();
        $txn->set('k1', 'v1');
        $txn->commit();

        $this->assertWithinDeadline('KvPrewrite', 'KvCommit');
    }

    public function testHeartbeatRpcDeadlineIsCappedByTheRemainingRetryDeadline(): void
    {
        $txn = $this->createTransaction();
        $txn->set('k1', 'v1');
        $txn->heartbeat();

        $this->assertWithinDeadline('KvTxnHeartBeat');
    }

    /**
     * A heartbeat fired inside an enclosing operation must keep that
     * operation's budget instead of resetting it (issue #257 review).
     *
     * The executor carries a budget that already spent 750 of its 800 ms;
     * starting a fresh budget would hand the heartbeat roughly the full
     * 800 ms again, which is exactly what a prewrite-triggered heartbeat did
     * before the fix.
     */
    public function testHeartbeatKeepsTheEnclosingOperationBudget(): void
    {
        $executor = new RetryExecutor(
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 60000,
            regionCache: new RegionCache(),
            grpc: $this->grpc,
            regionResolver: new RegionResolver($this->pdClient, new RegionCache()),
            logger: new NullLogger(),
            deadlineMs: self::DEADLINE_MS,
            budget: new RetryBudget(
                startTimeMs: (int) (microtime(true) * 1000) - 750,
                deadlineMs: self::DEADLINE_MS,
            ),
        );

        $state = new TransactionState();
        $state->setWrite('k1', 'v1');

        $this->createCommitter()->heartbeat(
            'k1',
            $state,
            $executor,
            static fn (): RetryVerdict => RetryVerdict::pass(),
        );

        $calls = $this->callsTo('KvTxnHeartBeat');
        self::assertCount(1, $calls);
        foreach ($calls as $call) {
            self::assertNotNull($call['timeoutMs']);
            self::assertGreaterThan(0, $call['timeoutMs']);
            // A fresh budget would be ~800 ms; the enclosing budget has
            // ~50 ms left, so a generous 400 ms ceiling still separates the
            // two while tolerating slow CI overhead.
            self::assertLessThanOrEqual(
                400,
                $call['timeoutMs'],
                'a fresh budget would give the heartbeat the full deadline again',
            );
        }
    }

    public function testPessimisticLockRpcDeadlineIsCappedByTheRemainingRetryDeadline(): void
    {
        $this->createTransaction(pessimistic: true)->set('k1', 'v1');

        $this->assertWithinDeadline('KvPessimisticLock');
    }

    /**
     * The PD region lookups performed while resolving a lock are part of the
     * resolution, so they share the caller's remaining retry deadline instead
     * of running under the configured `pdTimeoutMs` alone.
     */
    public function testLockResolutionPdLookupsAreCappedByTheRemainingRetryDeadline(): void
    {
        $regionCache = new RegionCache();
        $regionResolver = new RegionResolver($this->pdClient, $regionCache);
        $lockResolver = new LockResolver(
            $this->grpc,
            $regionResolver,
            $regionCache,
            $this->pdClient,
            1000,
        );

        $lock = new LockInfo();
        $lock->setKey('k1');
        $lock->setPrimaryLock('k1');
        $lock->setLockVersion(2000);

        $lockResolver->resolveLock('k1', $lock, self::DEADLINE_MS);

        self::assertNotEmpty($this->pdTimeouts);
        foreach ($this->pdTimeouts as $timeoutMs) {
            self::assertNotNull($timeoutMs);
            self::assertGreaterThan(0, $timeoutMs);
            self::assertLessThanOrEqual(self::DEADLINE_MS, $timeoutMs);
        }
    }

    /**
     * The resolver seam itself: a caller-supplied timeout reaches the PD
     * `GetRegion` and `ScanRegions` calls unchanged.
     */
    public function testRegionResolverPassesTheTimeoutToPdLookups(): void
    {
        $regionResolver = new RegionResolver($this->pdClient, new RegionCache());
        $regionResolver->getRegionInfo('k1', 250);

        $regionResolver = new RegionResolver($this->pdClient, new RegionCache());
        $regionResolver->batchResolveRegions(['k1'], 260);

        self::assertSame([250, 260], $this->pdTimeouts);
    }

    /**
     * The predecessor lookup uses the same caller-supplied timeout as the
     * containing-region lookup.
     */
    public function testRegionResolverPassesTheTimeoutToGetRegionBelow(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getRegion')->willReturnCallback(
            function (string $key, ?int $timeoutMs = null): RegionInfo {
                $this->pdTimeouts[] = $timeoutMs;

                return new RegionInfo(1, 1, 1, 1, 1, startKey: 'k1');
            },
        );
        $pdClient->method('getPrevRegion')->willReturnCallback(
            function (string $key, ?int $timeoutMs = null): ?RegionInfo {
                $this->pdTimeouts[] = $timeoutMs;

                return null;
            },
        );

        (new RegionResolver($pdClient, new RegionCache()))->getRegionBelow('k1', 250);

        self::assertSame([250, 250], $this->pdTimeouts);
    }

    /**
     * No configured timeout means no clamp: the configured TimeoutConfig
     * values stay in force (the caller did not opt into a deadline).
     */
    public function testNoBudgetKeepsTheConfiguredTimeout(): void
    {
        $regionCache = new RegionCache();
        $regionResolver = new RegionResolver($this->pdClient, $regionCache);
        $lockResolver = new LockResolver(
            $this->grpc,
            $regionResolver,
            $regionCache,
            $this->pdClient,
            1000,
        );

        $lock = new LockInfo();
        $lock->setKey('k1');
        $lock->setPrimaryLock('k1');
        $lock->setLockVersion(2000);

        $lockResolver->resolveLock('k1', $lock, 0);

        self::assertNotEmpty($this->pdTimeouts);
        foreach ($this->pdTimeouts as $timeoutMs) {
            self::assertNull(
                $timeoutMs,
                'no budget must leave PdClient to apply its configured pdTimeoutMs',
            );
        }
    }

    /**
     * `PdClient` forwards the explicit deadline to the transport instead of
     * always using the configured `pdTimeoutMs`.
     */
    public function testPdClientForwardsAnExplicitGetRegionTimeout(): void
    {
        (new PdClient($this->grpc, 'pd:2379'))->getRegion('k1', 250);

        $calls = $this->callsTo('GetRegion');
        self::assertCount(1, $calls);
        foreach ($calls as $call) {
            self::assertSame(250, $call['timeoutMs']);
        }
    }

    public function testPdClientForwardsAnExplicitScanRegionsTimeout(): void
    {
        (new PdClient($this->grpc, 'pd:2379'))->scanRegions('k0', 'k9', 10, 260);

        $calls = $this->callsTo('ScanRegions');
        self::assertCount(1, $calls);
        foreach ($calls as $call) {
            self::assertSame(260, $call['timeoutMs']);
        }
    }
}
