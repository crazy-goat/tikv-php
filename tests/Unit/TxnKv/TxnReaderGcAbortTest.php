<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * Server-side GC rejection surfacing (issue #422, AC-4).
 *
 * When GC has already passed a transaction's start timestamp, TiKV answers
 * reads with KeyError.abort = "GC life time is shorter than transaction
 * duration". Previously TxnReader inspected only locked/retryable and fell
 * through on abort — the read returned "not found" as if the key never
 * existed. These tests lock in the typed, non-retryable failure.
 */
final class TxnReaderGcAbortTest extends TestCase
{
    private PdClientInterface&\PHPUnit\Framework\MockObject\MockObject $pdClient;
    private GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject $grpc;
    private RegionCacheInterface&\PHPUnit\Framework\MockObject\MockObject $regionCache;
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
    }

    private function createTransaction(): Transaction
    {
        $lockResolver = new LockResolver(
            $this->grpc,
            new \CrazyGoat\TiKV\Client\Region\RegionResolver($this->pdClient, $this->regionCache),
            $this->regionCache,
            $this->pdClient,
            1000,
        );

        return new Transaction(
            txnId: 'test-txn-gc',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $lockResolver,
            regionResolver: new \CrazyGoat\TiKV\Client\Region\RegionResolver($this->pdClient, $this->regionCache),
        );
    }

    private function gcAbortResponse(): GetResponse
    {
        $response = new GetResponse();
        $keyError = new KeyError();
        $keyError->setAbort('GC life time is shorter than transaction duration, start ts: 1000, gc safe point: 2000');
        $response->setError($keyError);

        return $response;
    }

    public function testGetThrowsTypedGcExceptionOnAbort(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);
        $this->grpc->method('call')->willReturn($this->gcAbortResponse());

        $txn = $this->createTransaction();

        $this->expectException(TxnAbortedByGcException::class);
        $this->expectExceptionMessage('GC life time is shorter than transaction duration');
        $txn->get('key');
    }

    public function testGetGcAbortIsNotRetried(): void
    {
        // A non-retryable failure must surface the FIRST error, not exhaust
        // the retry budget: count the RPC attempts and assert exactly one.
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $test = $this;
        $attempts = 0;
        $this->grpc->method('call')->willReturnCallback(static function () use ($test, &$attempts): GetResponse {
            $attempts++;

            return $test->gcAbortResponse();
        });

        $txn = $this->createTransaction();

        try {
            $txn->get('key');
            self::fail('Expected TxnAbortedByGcException');
        } catch (TxnAbortedByGcException) {
        }

        $this->assertSame(1, $attempts);
    }

    public function testScanThrowsTypedGcExceptionOnAbort(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $response = new ScanResponse();
        $keyError = new KeyError();
        $keyError->setAbort('GC life time is shorter than transaction duration, start ts: 1000');
        $response->setError($keyError);

        $this->pdClient->method('scanRegions')->willReturn([$this->testRegion]);
        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction();

        $this->expectException(TxnAbortedByGcException::class);
        $this->expectExceptionMessage('GC life time is shorter than transaction duration');
        $txn->scan('a', 'z');
    }

    public function testGetNonGcAbortSurfacesServerText(): void
    {
        // A non-GC abort must not fall through silently as a "not found"
        // read — it surfaces with the server text (as a base TiKvException,
        // which the classifier treats as fatal).
        $this->regionCache->method('getByKey')->willReturn($this->testRegion);

        $response = new GetResponse();
        $keyError = new KeyError();
        $keyError->setAbort('some other abort reason');
        $response->setError($keyError);

        $this->grpc->method('call')->willReturn($response);

        $txn = $this->createTransaction();

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('some other abort reason');
        $txn->get('key');
    }

    public function testHoldGcSafePointDelegatesToPdClient(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(5000);
        $pdClient->expects($this->once())
            ->method('updateServiceGCSafePoint')
            ->with('tikv-php-txnkv-manual', 5000, 600)
            ->willReturn(4242);

        $client = new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));
        $this->assertSame(4242, $client->holdGcSafePoint(5000, 600, 'tikv-php-txnkv-manual'));
    }

    public function testReleaseGcSafePointUsesNegativeTtl(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(5000);
        $pdClient->expects($this->once())
            ->method('updateServiceGCSafePoint')
            ->with('svc-x', 0, -1)
            ->willReturn(null);

        $client = new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));
        $this->assertNull($client->releaseGcSafePoint('svc-x'));
    }

    public function testHoldAndReleaseUseSameDefaultServiceId(): void
    {
        // hold() and release() without an explicit service ID must target
        // the SAME registration — capture the ID from both calls.
        $capturedIds = [];
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(5000);
        $pdClient->method('updateServiceGCSafePoint')
            ->willReturnCallback(static function (string $serviceId) use (&$capturedIds): int {
                $capturedIds[] = $serviceId;

                return 1;
            });

        $client = new TxnKvClient($pdClient, $this->createMock(GrpcClientInterface::class));
        $client->holdGcSafePoint(5000, 600);
        $client->releaseGcSafePoint();

        $this->assertCount(2, $capturedIds);
        $this->assertSame($capturedIds[0], $capturedIds[1]);
        $this->assertStringStartsWith('tikv-php-txnkv-', $capturedIds[0]);
    }
}
