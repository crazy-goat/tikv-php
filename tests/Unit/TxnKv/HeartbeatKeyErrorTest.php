<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\AlreadyExist;
use CrazyGoat\Proto\Kvrpcpb\AssertionFailed;
use CrazyGoat\Proto\Kvrpcpb\CommitTsExpired;
use CrazyGoat\Proto\Kvrpcpb\CommitTsTooLarge;
use CrazyGoat\Proto\Kvrpcpb\Deadlock;
use CrazyGoat\Proto\Kvrpcpb\DebugInfo;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\PrimaryMismatch;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnLockNotFound;
use CrazyGoat\Proto\Kvrpcpb\TxnNotFound;
use CrazyGoat\Proto\Kvrpcpb\WriteConflict;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\KeyErrorDescriber;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #492: Transaction::heartbeat() must surface the server's KeyError
 * detail instead of always throwing the constant
 * 'Heartbeat failed: key error'.
 */
final class HeartbeatKeyErrorTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionInfo $testRegion;

    protected function setUp(): void
    {
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

    private function heartbeatKeyError(KeyError $keyError): TiKvException
    {
        $response = new TxnHeartBeatResponse();
        $response->setError($keyError);
        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction();
        $txn->set('key1', 'value1');

        try {
            $txn->heartbeat(10000);
        } catch (TiKvException $e) {
            return $e;
        }

        throw new \LogicException('heartbeat did not throw on key error');
    }

    public function testRetryableVariantThrowsTransactionConflictWithDetail(): void
    {
        $e = $this->heartbeatKeyError((new KeyError())->setRetryable('key was locked in test'));

        self::assertInstanceOf(TransactionConflictException::class, $e);
        self::assertSame('Heartbeat failed: retryable: key was locked in test', $e->getMessage());
    }

    public function testAbortVariantThrowsTransactionConflictWithDetail(): void
    {
        $e = $this->heartbeatKeyError((new KeyError())->setAbort('txn aborted in test'));

        self::assertInstanceOf(TransactionConflictException::class, $e);
        self::assertSame('Heartbeat failed: abort: txn aborted in test', $e->getMessage());
    }

    public function testTxnNotFoundVariantFallsBackToBaseTiKvException(): void
    {
        $e = $this->heartbeatKeyError((new KeyError())->setTxnNotFound(new TxnNotFound()));

        self::assertNotInstanceOf(TransactionConflictException::class, $e);
        self::assertNotInstanceOf(TxnRetryableException::class, $e);
        self::assertSame('Heartbeat failed: TxnNotFound', $e->getMessage());
    }

    public function testTxnLockNotFoundVariantFallsBackToBaseTiKvException(): void
    {
        $e = $this->heartbeatKeyError((new KeyError())->setTxnLockNotFound(new TxnLockNotFound()));

        self::assertNotInstanceOf(TransactionConflictException::class, $e);
        self::assertSame('Heartbeat failed: TxnLockNotFound', $e->getMessage());
    }

    public function testEmptyKeyErrorStillThrows(): void
    {
        $e = $this->heartbeatKeyError(new KeyError());

        self::assertSame('Heartbeat failed: unknown error', $e->getMessage());
    }

    public function testDescribeCoversEveryMessageVariant(): void
    {
        $variants = [
            [(new KeyError())->setLocked(new LockInfo()), 'Locked'],
            [(new KeyError())->setConflict(new WriteConflict()), 'Conflict'],
            [(new KeyError())->setAlreadyExist(new AlreadyExist()), 'AlreadyExist'],
            [(new KeyError())->setDeadlock(new Deadlock()), 'Deadlock'],
            [(new KeyError())->setCommitTsExpired(new CommitTsExpired()), 'CommitTsExpired'],
            [(new KeyError())->setTxnNotFound(new TxnNotFound()), 'TxnNotFound'],
            [(new KeyError())->setCommitTsTooLarge(new CommitTsTooLarge()), 'CommitTsTooLarge'],
            [(new KeyError())->setAssertionFailed(new AssertionFailed()), 'AssertionFailed'],
            [(new KeyError())->setPrimaryMismatch(new PrimaryMismatch()), 'PrimaryMismatch'],
            [(new KeyError())->setTxnLockNotFound(new TxnLockNotFound()), 'TxnLockNotFound'],
            [(new KeyError())->setDebugInfo(new DebugInfo()), 'unknown error'],
            [new KeyError(), 'unknown error'],
            [null, 'unknown error'],
        ];

        foreach ($variants as [$error, $expected]) {
            self::assertSame($expected, KeyErrorDescriber::describe($error));
        }
    }

    public function testDescribeCombinesStringPayloads(): void
    {
        $error = (new KeyError())->setRetryable('retry-me');

        self::assertSame('retryable: retry-me', KeyErrorDescriber::describe($error));
    }
}
