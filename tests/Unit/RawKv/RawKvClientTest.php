<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\CasResult;
use CrazyGoat\TiKV\Client\RawKv\ChecksumResult;
use CrazyGoat\TiKV\Client\RawKv\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\RawKv\ScanIterator;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class RawKvClientTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RawKvClient $client;

    private function defaultRegion(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        return $store;
    }

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache);
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    public function testCreateFactoryMethodExists(): void
    {
        $this->assertTrue(method_exists(RawKvClient::class, 'create')); // @phpstan-ignore function.alreadyNarrowedType
    }

    public function testCreateThrowsOnEmptyPdEndpoints(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('PD endpoints array must not be empty');
        RawKvClient::create([]);
    }

    public function testCloseIsIdempotent(): void
    {
        $this->client->close();
        $this->client->close();
        $this->expectNotToPerformAssertions();
    }

    // ========================================================================
    // ClientClosedException on all operations
    // ========================================================================

    /**
     * @param array<mixed> $args
     */
    #[DataProvider('closedOperationsProvider')]
    public function testThrowsClientClosedExceptionWhenClosed(string $method, array $args): void
    {
        $this->client->close();

        $this->expectException(ClientClosedException::class);
        $this->expectExceptionMessage('Client is closed');

        $this->client->$method(...$args);
    }

    /** @return iterable<string, array{string, array<mixed>}> */
    public static function closedOperationsProvider(): iterable
    {
        yield 'get' => ['get', ['key']];
        yield 'put' => ['put', ['key', 'value']];
        yield 'delete' => ['delete', ['key']];
        yield 'batchGet' => ['batchGet', [['k1', 'k2']]];
        yield 'batchPut' => ['batchPut', [['k1' => 'v1']]];
        yield 'batchDelete' => ['batchDelete', [['k1']]];
        yield 'scan' => ['scan', ['start', 'end']];
        yield 'scanPrefix' => ['scanPrefix', ['prefix']];
        yield 'reverseScan' => ['reverseScan', ['start', 'end']];
        yield 'deleteRange' => ['deleteRange', ['start', 'end']];
        yield 'deletePrefix' => ['deletePrefix', ['prefix']];
        yield 'getKeyTTL' => ['getKeyTTL', ['key']];
        yield 'compareAndSwap' => ['compareAndSwap', ['key', 'old', 'new']];
        yield 'putIfAbsent' => ['putIfAbsent', ['key', 'value']];
        yield 'checksum' => ['checksum', ['start', 'end']];
        yield 'batchScan' => ['batchScan', [[['a', 'b']], 10]];
        yield 'scanIterator' => ['scanIterator', ['start', 'end']];
        yield 'scanPrefixIterator' => ['scanPrefixIterator', ['prefix']];
    }

    // ========================================================================
    // Input validation
    // ========================================================================

    public function testGetThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key must not be empty');
        $this->client->get('');
    }

    public function testGetThrowsOnOversizedKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->get($key);
    }

    public function testPutThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->put('', 'value');
    }

    public function testPutThrowsOnOversizedKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->client->put($key, 'value');
    }

    public function testPutThrowsOnOversizedValue(): void
    {
        $value = str_repeat('a', RawKvClient::MAX_VALUE_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->client->put('key', $value);
    }

    public function testDeleteThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->delete('');
    }

    public function testBatchGetThrowsOnEmptyKeyInList(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->batchGet(['valid', '']);
    }

    public function testBatchPutThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->batchPut(['' => 'value']);
    }

    public function testBatchPutThrowsOnOversizedValue(): void
    {
        $value = str_repeat('a', RawKvClient::MAX_VALUE_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->client->batchPut(['key' => $value]);
    }

    public function testBatchDeleteThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->batchDelete(['valid', '']);
    }

    public function testBatchGetThrowsOnOversizedKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->batchGet([$key]);
    }

    public function testBatchDeleteThrowsOnOversizedKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->batchDelete([$key]);
    }

    public function testScanThrowsOnOversizedStartKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->scan($key, 'end');
    }

    public function testScanThrowsOnOversizedEndKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->scan('start', $key);
    }

    public function testReverseScanThrowsOnOversizedStartKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->reverseScan($key, 'end');
    }

    public function testReverseScanThrowsOnOversizedEndKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->reverseScan('start', $key);
    }

    public function testCompareAndSwapThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->compareAndSwap('', 'old', 'new');
    }

    public function testCompareAndSwapThrowsOnOversizedValue(): void
    {
        $value = str_repeat('a', RawKvClient::MAX_VALUE_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->client->compareAndSwap('key', null, $value);
    }

    // ========================================================================
    // get()
    // ========================================================================

    public function testGetReturnsValue(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('hello');

        $this->grpc->method('call')->willReturn($response);

        $this->assertSame('hello', $this->client->get('key'));
    }

    public function testGetReturnsNullForMissingKey(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setNotFound(true);
        $response->setValue('');

        $this->grpc->method('call')->willReturn($response);

        $this->assertNull($this->client->get('missing'));
    }

    public function testGetThrowsStoreNotFoundWhenStoreIsNull(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn(null);

        $this->expectException(StoreNotFoundException::class);

        $this->client->get('key');
    }

    // ========================================================================
    // put()
    // ========================================================================

    public function testPutCallsGrpc(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                'tikv1:20160',
                'tikvpb.Tikv',
                'RawPut',
                $this->isInstanceOf(Message::class),
                RawPutResponse::class,
            )
            ->willReturn(new RawPutResponse());

        $this->client->put('key', 'value');
    }

    // ========================================================================
    // delete()
    // ========================================================================

    public function testDeleteCallsGrpc(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->willReturn(new RawDeleteResponse());

        $this->client->delete('key');
    }

    // ========================================================================
    // batchGet()
    // ========================================================================

    public function testBatchGetEmptyReturnsEmpty(): void
    {
        $this->assertSame([], $this->client->batchGet([]));
    }

    public function testBatchGetReturnsOrderedResults(): void
    {
        $this->markTestSkipped('Async batchGet requires real gRPC channel - needs integration test');
    }

    public function testBatchGetReturnsNullForMissingKeys(): void
    {
        $this->markTestSkipped('Async batchGet requires real gRPC channel - needs integration test');
    }

    // ========================================================================
    // batchPut()
    // ========================================================================

    public function testBatchPutEmptyIsNoop(): void
    {
        $this->grpc->expects($this->never())->method('call');
        $this->client->batchPut([]);
    }

    // ========================================================================
    // batchDelete()
    // ========================================================================

    public function testBatchDeleteEmptyIsNoop(): void
    {
        $this->grpc->expects($this->never())->method('call');
        $this->client->batchDelete([]);
    }

    // ========================================================================
    // scan()
    // ========================================================================

    public function testScanLimitZeroIsCappedToMax(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs = [];
        for ($i = 0; $i < RawKvClient::MAX_SCAN_LIMIT; $i++) {
            $pair = new KvPair();
            $pair->setKey('k' . $i);
            $pair->setValue('v' . $i);
            $pairs[] = $pair;
        }

        $response = new RawScanResponse();
        $response->setKvs($pairs);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->scan('k', 'l', 0);

        $this->assertCount(RawKvClient::MAX_SCAN_LIMIT, $result);
    }

    public function testScanLimitExceedingMaxThrowsException(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit (10241) exceeds maximum allowed scan limit of 10240');

        $this->client->scan('k', 'l', RawKvClient::MAX_SCAN_LIMIT + 1);
    }

    public function testReverseScanLimitExceedingMaxThrowsException(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit (99999) exceeds maximum allowed scan limit of 10240');

        $this->client->reverseScan('k', 'l', 99999);
    }

    public function testScanReturnsResults(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->scan('k', 'l');

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertSame('v1', $result[0]['value']);
    }

    // ========================================================================
    // deletePrefix()
    // ========================================================================

    public function testDeletePrefixThrowsOnEmptyPrefix(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->deletePrefix('');
    }

    public function testDeletePrefixThrowsOnAllFFPrefix(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('0xFF bytes');
        $this->client->deletePrefix("\xff\xff");
    }

    public function testDeletePrefixThrowsOnSingleFFByte(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->deletePrefix("\xff");
    }

    // ========================================================================
    // scanPrefix()
    // ========================================================================

    public function testScanPrefixDelegatesToScan(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair = new KvPair();
        $pair->setKey('prefix_key1');
        $pair->setValue('v1');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->scanPrefix('prefix_');

        $this->assertCount(1, $result);
        $this->assertSame('prefix_key1', $result[0]['key']);
        $this->assertSame('v1', $result[0]['value']);
    }

    public function testScanPrefixWithKeyOnly(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');

        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->scanPrefix('k', 0, true);

        $this->assertCount(1, $result);
        $this->assertSame('k1', $result[0]['key']);
        $this->assertNull($result[0]['value']);
    }

    // ========================================================================
    // scanIterator() / scanPrefixIterator()
    // ========================================================================

    public function testScanIteratorReturnsScanIterator(): void
    {
        $iterator = $this->client->scanIterator('start', 'end');

        $this->assertInstanceOf(ScanIterator::class, $iterator);
    }

    public function testScanPrefixIteratorReturnsScanIterator(): void
    {
        $iterator = $this->client->scanPrefixIterator('prefix_');

        $this->assertInstanceOf(ScanIterator::class, $iterator);
    }

    // ========================================================================
    // reverseScan()
    // ========================================================================

    public function testReverseScanReturnsResultsInDescendingOrder(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: '',
        );

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pairs = [];
        foreach (['c', 'b', 'a'] as $k) {
            $pair = new KvPair();
            $pair->setKey($k);
            $pair->setValue('v_' . $k);
            $pairs[] = $pair;
        }

        $response = new RawScanResponse();
        $response->setKvs($pairs);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->reverseScan('z', 'a');

        $this->assertCount(3, $result);
        $this->assertSame('c', $result[0]['key']);
        $this->assertSame('b', $result[1]['key']);
        $this->assertSame('a', $result[2]['key']);
    }

    // ========================================================================
    // deleteRange()
    // ========================================================================

    public function testDeleteRangeWithSameStartEndReturnsEarly(): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->grpc->expects($this->never())->method('call');

        $this->client->deleteRange('a', 'a');
    }

    // ========================================================================
    // batchScan()
    // ========================================================================

    public function testBatchScanMultipleRangesReturnsCorrectShape(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: '',
        );

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');

        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');

        $response = new RawScanResponse();
        $response->setKvs([$pair1]);

        $response2 = new RawScanResponse();
        $response2->setKvs([$pair2]);

        $this->grpc->method('call')
            ->willReturnOnConsecutiveCalls($response, $response2);

        $result = $this->client->batchScan([['a', 'm'], ['m', 'z']], 10);

        $this->assertCount(2, $result);
        $this->assertCount(1, $result[0]);
        $this->assertSame('k1', $result[0][0]['key']);
        $this->assertCount(1, $result[1]);
        $this->assertSame('k2', $result[1][0]['key']);
    }

    // ========================================================================

    public function testBatchScanThrowsOnInvalidEachLimit(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->client->batchScan([['a', 'b']], 0);
    }

    public function testBatchScanThrowsOnEachLimitExceedingMax(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('eachLimit (10241) exceeds maximum allowed scan limit of 10240');

        $this->client->batchScan([['a', 'b']], RawKvClient::MAX_SCAN_LIMIT + 1);
    }

    public function testBatchScanThrowsOnInvalidRangeFormat(): void
    {
        $this->pdClient->method('scanRegions')->willReturn([]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->expectException(InvalidArgumentException::class);
        $this->client->batchScan([['only-one']], 10); // @phpstan-ignore argument.type
    }

    public function testBatchScanEmptyReturnsEmpty(): void
    {
        $this->assertSame([], $this->client->batchScan([], 10));
    }

    // ========================================================================
    // compareAndSwap()
    // ========================================================================

    public function testCompareAndSwapSuccess(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(true);
        $response->setPreviousNotExist(false);
        $response->setPreviousValue('old');

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->compareAndSwap('key', 'old', 'new');

        $this->assertInstanceOf(CasResult::class, $result);
        $this->assertTrue($result->swapped);
        $this->assertSame('old', $result->previousValue);
    }

    public function testCompareAndSwapFailure(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(false);
        $response->setPreviousNotExist(false);
        $response->setPreviousValue('actual');

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->compareAndSwap('key', 'wrong', 'new');

        $this->assertFalse($result->swapped);
        $this->assertSame('actual', $result->previousValue);
    }

    // ========================================================================
    // putIfAbsent()
    // ========================================================================

    public function testPutIfAbsentReturnsNullOnSuccess(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(true);
        $response->setPreviousNotExist(true);

        $this->grpc->method('call')->willReturn($response);

        $this->assertNull($this->client->putIfAbsent('key', 'value'));
    }

    public function testPutIfAbsentReturnsExistingValue(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(false);
        $response->setPreviousNotExist(false);
        $response->setPreviousValue('existing');

        $this->grpc->method('call')->willReturn($response);

        $this->assertSame('existing', $this->client->putIfAbsent('key', 'value'));
    }

    // ========================================================================
    // checksum()
    // ========================================================================

    public function testChecksumReturnsResult(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $region = new RegionInfo(1, 1, 1, 1, 1, 'a', 'z');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawChecksumResponse();
        $response->setChecksum(12345);
        $response->setTotalKvs(3);
        $response->setTotalBytes(100);

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->checksum('a', 'z');

        $this->assertInstanceOf(ChecksumResult::class, $result);
        $this->assertSame(12345, $result->checksum);
        $this->assertSame(3, $result->totalKvs);
        $this->assertSame(100, $result->totalBytes);
    }

    // ========================================================================
    // getKeyTTL()
    // ========================================================================

    public function testGetKeyTTLReturnsValue(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetKeyTTLResponse();
        $response->setTtl(42);
        $response->setNotFound(false);

        $this->grpc->method('call')->willReturn($response);

        $this->assertSame(42, $this->client->getKeyTTL('key'));
    }

    public function testGetKeyTTLReturnsNullWhenNotFound(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetKeyTTLResponse();
        $response->setNotFound(true);

        $this->grpc->method('call')->willReturn($response);

        $this->assertNull($this->client->getKeyTTL('missing'));
    }

    public function testGetKeyTTLReturnsNullWhenZero(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetKeyTTLResponse();
        $response->setTtl(0);
        $response->setNotFound(false);

        $this->grpc->method('call')->willReturn($response);

        $this->assertNull($this->client->getKeyTTL('no-ttl'));
    }

    public function testGetKeyTTLThrowsOnEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key must not be empty');
        $this->client->getKeyTTL('');
    }

    public function testGetKeyTTLThrowsOnOversizedKey(): void
    {
        $key = str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key size');
        $this->client->getKeyTTL($key);
    }

    // ========================================================================
    // Retry / backoff
    // ========================================================================

    public function testRaftEntryTooLargeThrowsImmediately(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('RaftEntryTooLarge'));

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('RaftEntryTooLarge');
        $this->client->get('key');
    }

    public function testKeyNotInRegionThrowsImmediately(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('KeyNotInRegion'));

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('KeyNotInRegion');
        $this->client->get('key');
    }

    public function testEpochNotMatchRetriesImmediately(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('found');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $response,
            );

        $result = $this->client->get('key');
        $this->assertSame('found', $result);
    }

    public function testBudgetExceededThrowsLastException(): void
    {
        $client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, 0, new NullLogger(), 0);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->method('call')
            ->willThrowException(new TiKvException('ServerIsBusy'));

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('ServerIsBusy');
        $client->get('key');
    }

    public function testGrpcExceptionTriggersRetry(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('connection reset', 14)),
                $response,
            );

        $result = $this->client->get('key');
        $this->assertSame('recovered', $result);
    }

    public function testRetryLogsWarningOnRetriableError(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, 20000, $logger);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('ok');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $response,
            );

        $logger->expects($this->once())
            ->method('warning')
            ->with('Retrying operation', $this->callback(fn(array $context): bool => $context['key'] === 'key'
                && $context['attempt'] === 0
                && $context['backoffType'] === 'None'
                && $context['sleepMs'] === 0));

        $client->get('key');
    }

    public function testRetryLogsCacheInvalidation(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, 20000, $logger);

        $region = $this->defaultRegion();
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('ok');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $response,
            );

        $logger->expects($this->once())
            ->method('info')
            ->with('Invalidated region on retry', ['key' => 'key', 'regionId' => 1]);

        $client->get('key');
    }

    public function testBudgetExhaustedLogsError(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, 0, $logger, 0);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->method('call')
            ->willThrowException(new TiKvException('ServerIsBusy'));

        $logger->expects($this->once())
            ->method('error')
            ->with('ServerBusy budget exhausted', $this->callback(fn(array $context): bool => $context['key'] === 'key'
                && $context['serverBusyBudgetMs'] === 0));

        $this->expectException(TiKvException::class);
        $client->get('key');
    }

    public function testFatalErrorLogsError(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, 20000, $logger);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->method('call')
            ->willThrowException(new TiKvException('RaftEntryTooLarge'));

        $logger->expects($this->once())
            ->method('error')
            ->with('Fatal error, not retrying', $this->callback(fn(array $context): bool => $context['key'] === 'key'
                && $context['error'] === 'RaftEntryTooLarge'));

        $this->expectException(TiKvException::class);
        $client->get('key');
    }

    public function testGrpcExceptionClosesChannel(): void
    {
        $logger = $this->createMock(LoggerInterface::class);
        $client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, 20000, $logger);

        $region = $this->defaultRegion();
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('connection reset', 14)),
                $response,
            );

        $this->grpc->expects($this->once())
            ->method('closeChannel')
            ->with('tikv1:20160');

        $client->get('key');
    }

    // ========================================================================
    // NotLeader handling
    // ========================================================================

    private function regionWithPeers(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 10,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [
                new PeerInfo(peerId: 10, storeId: 1),
                new PeerInfo(peerId: 20, storeId: 2),
                new PeerInfo(peerId: 30, storeId: 3),
            ],
        );
    }

    public function testNotLeaderWithHintSwitchesLeaderAndRetries(): void
    {
        $region = $this->regionWithPeers();
        $this->regionCache->method('getByKey')->willReturn($region);

        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->regionCache->expects($this->once())
            ->method('switchLeader')
            ->with(1, 3)
            ->willReturn(true);

        $leader = new Peer();
        $leader->setId(30);
        $leader->setStoreId(3);

        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $errorResponse = new RawGetResponse();
        $errorResponse->setRegionError($error);

        $successResponse = new RawGetResponse();
        $successResponse->setValue('found');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls($errorResponse, $successResponse);

        $result = $this->client->get('key');
        $this->assertSame('found', $result);
    }

    public function testNotLeaderWithoutHintInvalidatesRegion(): void
    {
        $region = $this->regionWithPeers();
        $this->regionCache->method('getByKey')->willReturn($region);

        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(1);

        $this->regionCache->expects($this->never())
            ->method('switchLeader');

        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $errorResponse = new RawGetResponse();
        $errorResponse->setRegionError($error);

        $successResponse = new RawGetResponse();
        $successResponse->setValue('found');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls($errorResponse, $successResponse);

        $result = $this->client->get('key');
        $this->assertSame('found', $result);
    }

    public function testNotLeaderWithUnknownPeerInvalidatesRegion(): void
    {
        $region = $this->regionWithPeers();
        $this->regionCache->method('getByKey')->willReturn($region);

        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->regionCache->expects($this->once())
            ->method('switchLeader')
            ->with(1, 99)
            ->willReturn(false);

        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(1);

        $leader = new Peer();
        $leader->setId(99);
        $leader->setStoreId(99);

        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $errorResponse = new RawGetResponse();
        $errorResponse->setRegionError($error);

        $successResponse = new RawGetResponse();
        $successResponse->setValue('found');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls($errorResponse, $successResponse);

        $result = $this->client->get('key');
        $this->assertSame('found', $result);
    }

    public function testRegionErrorSurfacesEpochNotMatch(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new \CrazyGoat\Proto\Errorpb\EpochNotMatch());

        $errorResponse = new RawGetResponse();
        $errorResponse->setRegionError($error);

        $successResponse = new RawGetResponse();
        $successResponse->setValue('ok');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls($errorResponse, $successResponse);

        $result = $this->client->get('key');
        $this->assertSame('ok', $result);
    }

    public function testNotLeaderStringFallbackInClassifyError(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new RegionException('test', 'NotLeader')),
                $response,
            );

        $result = $this->client->get('key');
        $this->assertSame('recovered', $result);
    }

    public function testBackoffTypeNotLeaderSleepValues(): void
    {
        $this->assertSame(2, \CrazyGoat\TiKV\Client\Retry\BackoffType::NotLeader->baseMs());
        $this->assertSame(500, \CrazyGoat\TiKV\Client\Retry\BackoffType::NotLeader->capMs());
        $this->assertFalse(\CrazyGoat\TiKV\Client\Retry\BackoffType::NotLeader->equalJitter());
        $this->assertSame(2, \CrazyGoat\TiKV\Client\Retry\BackoffType::NotLeader->sleepMs(0));
    }

    // ========================================================================
    // Atomic for CAS mode
    // ========================================================================

    public function testAtomicForCASDisabledByDefault(): void
    {
        $this->assertFalse($this->client->isAtomicForCAS());
    }

    public function testSetAtomicForCASReturnsSelf(): void
    {
        $result = $this->client->setAtomicForCAS(true);

        $this->assertSame($this->client, $result);
    }

    public function testSetAtomicForCAS(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->assertTrue($this->client->isAtomicForCAS());
    }

    public function testPutSetsForCasWhenAtomicEnabled(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (Message $request): bool {
                    if ($request instanceof \CrazyGoat\Proto\Kvrpcpb\RawPutRequest) {
                        return $request->getForCas() === true;
                    }
                    return false;
                }),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn(new RawPutResponse());

        $this->client->put('key', 'value');
    }

    public function testPutDoesNotSetForCasWhenAtomicDisabled(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (Message $request): bool {
                    if ($request instanceof \CrazyGoat\Proto\Kvrpcpb\RawPutRequest) {
                        return $request->getForCas() === false;
                    }
                    return false;
                }),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn(new RawPutResponse());

        $this->client->put('key', 'value');
    }

    public function testDeleteSetsForCasWhenAtomicEnabled(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (Message $request): bool {
                    if ($request instanceof \CrazyGoat\Proto\Kvrpcpb\RawDeleteRequest) {
                        return $request->getForCas() === true;
                    }
                    return false;
                }),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn(new RawDeleteResponse());

        $this->client->delete('key');
    }

    public function testDeleteDoesNotSetForCasWhenAtomicDisabled(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                $this->anything(),
                $this->anything(),
                $this->callback(function (Message $request): bool {
                    if ($request instanceof \CrazyGoat\Proto\Kvrpcpb\RawDeleteRequest) {
                        return $request->getForCas() === false;
                    }
                    return false;
                }),
                $this->anything(),
                $this->anything(),
            )
            ->willReturn(new RawDeleteResponse());

        $this->client->delete('key');
    }

    public function testCompareAndSwapRequiresAtomicMode(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('CompareAndSwap requires atomic mode');

        $this->client->compareAndSwap('key', 'old', 'new');
    }

    public function testCompareAndSwapWorksWhenAtomicEnabled(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(true);
        $response->setPreviousValue('old');

        $this->grpc->method('call')->willReturn($response);

        $result = $this->client->compareAndSwap('key', 'old', 'new');

        $this->assertTrue($result->swapped);
    }

    public function testPutIfAbsentRequiresAtomicMode(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('CompareAndSwap requires atomic mode');

        $this->client->putIfAbsent('key', 'value');
    }

    public function testPutIfAbsentWorksWhenAtomicEnabled(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(true);
        $response->setPreviousNotExist(true);

        $this->grpc->method('call')->willReturn($response);

        $this->assertNull($this->client->putIfAbsent('key', 'value'));
    }

    // ========================================================================
    // Column family
    // ========================================================================

    public function testGetColumnFamilyDefaultIsEmpty(): void
    {
        $this->assertSame('', $this->client->getColumnFamily());
    }

    public function testSetColumnFamilyReturnsSelf(): void
    {
        $result = $this->client->setColumnFamily('write');

        $this->assertSame($this->client, $result);
    }

    public function testSetGetColumnFamilyRoundTrip(): void
    {
        $this->client->setColumnFamily('lock');

        $this->assertSame('lock', $this->client->getColumnFamily());
    }

    public function testGetWithColumnFamilySetsCfOnRequest(): void
    {
        $this->client->setColumnFamily('write');

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('hello');

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (
                &$capturedRequest,
                $response
): \CrazyGoat\Proto\Kvrpcpb\RawGetResponse {
                $capturedRequest = $request;
                return $response;
            });

        $this->client->get('key');

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class, $capturedRequest);
        $this->assertSame('write', $capturedRequest->getCf());
    }

    public function testGetWithoutColumnFamilyDoesNotSetCf(): void
    {
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('hello');

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (
                &$capturedRequest,
                $response
): \CrazyGoat\Proto\Kvrpcpb\RawGetResponse {
                $capturedRequest = $request;
                return $response;
            });

        $this->client->get('key');

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class, $capturedRequest);
        $this->assertSame('', $capturedRequest->getCf());
    }

    public function testPutWithColumnFamilySetsCfOnRequest(): void
    {
        $this->client->setColumnFamily('default');

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (&$capturedRequest): \CrazyGoat\Proto\Kvrpcpb\RawPutResponse {
                $capturedRequest = $request;
                return new RawPutResponse();
            });

        $this->client->put('key', 'value');

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawPutRequest::class, $capturedRequest);
        $this->assertSame('default', $capturedRequest->getCf());
    }

    public function testDeleteWithColumnFamilySetsCfOnRequest(): void
    {
        $this->client->setColumnFamily('lock');

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (&$capturedRequest): \CrazyGoat\Proto\Kvrpcpb\RawDeleteResponse {
                $capturedRequest = $request;
                return new RawDeleteResponse();
            });

        $this->client->delete('key');

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawDeleteRequest::class, $capturedRequest);
        $this->assertSame('lock', $capturedRequest->getCf());
    }

    public function testGetKeyTTLWithColumnFamilySetsCfOnRequest(): void
    {
        $this->client->setColumnFamily('write');

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetKeyTTLResponse();
        $response->setTtl(100);

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (
                &$capturedRequest,
                $response
): \CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse {
                $capturedRequest = $request;
                return $response;
            });

        $this->client->getKeyTTL('key');

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLRequest::class, $capturedRequest);
        $this->assertSame('write', $capturedRequest->getCf());
    }

    public function testCompareAndSwapWithColumnFamilySetsCfOnRequest(): void
    {
        $this->client->setAtomicForCAS(true);
        $this->client->setColumnFamily('default');

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawCASResponse();
        $response->setSucceed(true);

        $capturedRequest = null;
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (
                &$capturedRequest,
                $response
): \CrazyGoat\Proto\Kvrpcpb\RawCASResponse {
                $capturedRequest = $request;
                return $response;
            });

        $this->client->compareAndSwap('key', 'old', 'new');

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawCASRequest::class, $capturedRequest);
        $this->assertSame('default', $capturedRequest->getCf());
    }
}
