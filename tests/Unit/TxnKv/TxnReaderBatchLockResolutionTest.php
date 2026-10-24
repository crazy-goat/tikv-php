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
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
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

    private object $readResponse;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->methods = [];
        $this->resolves = [];

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
            function (string $address, string $service, string $method, mixed $request): object {
                $this->methods[] = $method;
                if ($method === 'KvCheckTxnStatus') {
                    $status = new CheckTxnStatusResponse();
                    $status->setCommitVersion(3000);
                    return $status;
                }
                if ($method === 'KvResolveLock') {
                    $this->assertInstanceOf(ResolveLockRequest::class, $request);
                    $this->resolves[] = $request;
                    return new ResolveLockResponse();
                }
                return $this->readResponse;
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
            classifier: static fn(\Throwable $e): ?BackoffType => null,
        );
    }

    private function executor(): RetryExecutor
    {
        return new RetryExecutor(
            1000,
            1000,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            new \Psr\Log\NullLogger(),
        );
    }

    private function scanAll(): void
    {
        $this->reader()->scan('k0', 'k9', 10, new TransactionState(), $this->executor(), static fn (): null => null);
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
            $this->reader()->batchGet(['k1', 'k2', 'k3'], new TransactionState());
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
            $this->reader()->batchGet(['k0', 'k1', 'k2'], new TransactionState());
        } finally {
            $this->assertSame(['k1', 'k2'], $this->resolvedKeys());
        }
    }
}
