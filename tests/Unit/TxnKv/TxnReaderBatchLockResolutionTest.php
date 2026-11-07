<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TxnReader;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * TxnReader hands every lock of one scan()/batchGet() response to
 * LockResolver::resolveLocks() at once (issue #428).
 */
final class TxnReaderBatchLockResolutionTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;

    /** @var list<string> */
    private array $methods = [];

    /** @var list<ResolveLockRequest> */
    private array $resolves = [];

    /** @var list<array{method: string, timeoutMs: ?int}> */
    private array $callTimeouts = [];

    private object $readResponse;

    /** @var list<object> Per-read responses; falls back to $readResponse. */
    private array $readResponses = [];

    /** When set, KvCheckTxnStatus throws it instead of answering. */
    private ?GrpcException $lockFailure = null;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->methods = [];
        $this->resolves = [];
        $this->callTimeouts = [];
        $this->readResponses = [];
        $this->lockFailure = null;

        $region = new RegionInfo(1, 1, 1, 1, 1, '', '');
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturn((1 << 42) | 9);
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
                ?string $responseClass = null,
                ?int $timeoutMs = null,
            ): object {
                $this->methods[] = $method;
                $this->callTimeouts[] = ['method' => $method, 'timeoutMs' => $timeoutMs];
                if ($method === 'KvCheckTxnStatus') {
                    if ($this->lockFailure instanceof GrpcException) {
                        throw $this->lockFailure;
                    }
                    $status = new CheckTxnStatusResponse();
                    $status->setCommitVersion(3000);
                    return $status;
                }
                if ($method === 'KvResolveLock') {
                    $this->assertInstanceOf(ResolveLockRequest::class, $request);
                    $this->resolves[] = $request;
                    return new ResolveLockResponse();
                }
                return array_shift($this->readResponses) ?? $this->readResponse;
            },
        );
    }

    private function reader(): TxnReader
    {
        return new TxnReader(
            1000,
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new LockResolver($this->grpc, $this->regionResolver, $this->regionCache, $this->pdClient, 1000),
            $this->regionCache,
        );
    }

    /**
     * @return callable(\CrazyGoat\TiKV\Client\Exception\TiKvException): RetryVerdict
     */
    private function classifier(): callable
    {
        return RetryVerdict::pass(...);
    }

    private function executor(int $deadlineMs = RetryExecutor::DEFAULT_RETRY_DEADLINE_MS): RetryExecutor
    {
        return new RetryExecutor(
            1000,
            1000,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            new \Psr\Log\NullLogger(),
            deadlineMs: $deadlineMs,
        );
    }

    private function scanAll(): void
    {
        $this->reader()->scan(
            'k0',
            'k9',
            10,
            new TransactionState(),
            $this->executor(),
            static fn (): RetryVerdict => RetryVerdict::pass(),
        );
    }

    private function lockedPair(string $key, int $ts = 2000): KvPair
    {
        $lock = new LockInfo();
        $lock->setKey($key);
        $lock->setPrimaryLock('k1');
        $lock->setLockVersion($ts);
        $error = new KeyError();
        $error->setLocked($lock);
        $pair = new KvPair();
        $pair->setError($error);

        return $pair;
    }

    private function conflictPair(): KvPair
    {
        $error = new KeyError();
        $error->setRetryable('write conflict');
        $pair = new KvPair();
        $pair->setError($error);

        return $pair;
    }

    /**
     * @return list<string>
     */
    private function resolvedKeys(): array
    {
        $this->assertCount(1, $this->resolves, 'one KvResolveLock for the whole transaction');

        return array_values(array_map(
            static fn (string $key): string => $key,
            iterator_to_array($this->resolves[0]->getKeys()),
        ));
    }

    private function assertBatchResolved(): void
    {
        $this->assertSame(['KvCheckTxnStatus', 'KvResolveLock'], array_slice($this->methods, 1));
        $this->assertSame(['k1', 'k2', 'k3'], $this->resolvedKeys());
        $this->assertSame(3000, (int) $this->resolves[0]->getCommitVersion());
    }

    public function testScanWithSeveralLocksResolvesThemInOneBatch(): void
    {
        $response = new ScanResponse();
        $response->setPairs([$this->lockedPair('k1'), $this->lockedPair('k2'), $this->lockedPair('k3')]);
        $this->readResponse = $response;

        try {
            $this->scanAll();
            $this->fail('Expected a retry request');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
        }

        $this->assertBatchResolved();
    }

    public function testBatchGetWithSeveralLocksResolvesThemInOneBatch(): void
    {
        $response = new BatchGetResponse();
        $response->setPairs([$this->lockedPair('k1'), $this->lockedPair('k2'), $this->lockedPair('k3')]);
        $this->readResponse = $response;

        try {
            $this->reader()->batchGet(
                ['k1', 'k2', 'k3'],
                new TransactionState(),
                $this->executor(),
                $this->classifier(),
            );
            $this->fail('Expected a retry request');
        } catch (TxnRetryableException $e) {
            $this->assertSame(BackoffType::TxnLock, $e->backoffType);
        }

        $this->assertBatchResolved();
    }

    public function testScanLockWinsOverANonLockPairError(): void
    {
        $response = new ScanResponse();
        $response->setPairs([$this->conflictPair(), $this->lockedPair('k1'), $this->lockedPair('k2')]);
        $this->readResponse = $response;

        $this->expectException(TxnRetryableException::class);
        try {
            $this->scanAll();
        } finally {
            $this->assertSame(['k1', 'k2'], $this->resolvedKeys());
        }
    }

    public function testBatchGetLockWinsOverANonLockPairError(): void
    {
        $response = new BatchGetResponse();
        $response->setPairs([$this->conflictPair(), $this->lockedPair('k1'), $this->lockedPair('k2')]);
        $this->readResponse = $response;

        $this->expectException(TxnRetryableException::class);
        try {
            $this->reader()->batchGet(
                ['k0', 'k1', 'k2'],
                new TransactionState(),
                $this->executor(),
                $this->classifier(),
            );
        } finally {
            $this->assertSame(['k1', 'k2'], $this->resolvedKeys());
        }
    }

    /**
     * Regression (issue #248, P1): the lock's primary is usually in another
     * region than the key being read. A transport failure of the lock RPC must
     * close the channel that actually failed — the address carried on the
     * GrpcException — not the read's own store.
     */
    public function testLockResolutionTransportFailureClosesTheLockRpcTargetStore(): void
    {
        $locked = new ScanResponse();
        $locked->setPairs([$this->lockedPair('k1')]);
        $this->readResponses = [$locked];
        $this->readResponse = new ScanResponse();

        // The read runs against 127.0.0.1:20160 (store 1); lock resolution
        // talks to the primary's store, which is the address that failed.
        $this->lockFailure = new GrpcException('connection reset', 14, 'primary-store:20160');

        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with('primary-store:20160');

        $this->scanAll();
    }

    /**
     * The read's retry budget caps the lock-resolution RPCs too (issue #569):
     * a lock conflict encountered during a scan must not spend the configured
     * 5 s lock-resolution timeout after the operation's deadline is nearly
     * spent.
     */
    public function testLockResolutionRpcTimeoutIsCappedByTheReadRetryDeadline(): void
    {
        $response = new ScanResponse();
        $response->setPairs([$this->lockedPair('k1')]);
        $this->readResponse = $response;

        try {
            $this->reader()->scan(
                'k0',
                'k9',
                10,
                new TransactionState(),
                $this->executor(500),
                static fn (): RetryVerdict => RetryVerdict::pass(),
            );
        } catch (TxnRetryableException) {
            // Expected: the lock was resolved and the read must be retried.
        }

        $lockCalls = array_values(array_filter(
            $this->callTimeouts,
            static fn (array $call): bool => in_array($call['method'], ['KvCheckTxnStatus', 'KvResolveLock'], true),
        ));
        self::assertNotEmpty($lockCalls);
        foreach ($lockCalls as $call) {
            self::assertNotNull($call['timeoutMs']);
            self::assertGreaterThan(0, $call['timeoutMs']);
            self::assertLessThanOrEqual(500, $call['timeoutMs']);
        }
    }
}
