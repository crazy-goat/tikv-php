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
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\HealthCheckException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\RawKv\CasResult;
use CrazyGoat\TiKV\Client\RawKv\ChecksumResult;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\RawKv\ScanIterator;
use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class RawKvClientTest extends TestCase
{
    use GrpcExtensionGate;

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

    /**
     * Future over a mocked gRPC call resolving with the given response.
     * Requires the grpc extension (a real \Grpc\Call mock drives it).
     *
     * @param list<string>|null $events When given, records a 'wait' entry each
     *                                  time the future is awaited.
     */
    private function okFuture(Message $response, ?array &$events = null): GrpcFuture
    {
        $this->requireGrpcExtension();

        $call = $this->createMock(\Grpc\Call::class);
        $call->method('startBatch')->willReturnCallback(
            function () use ($response, &$events): array {
                if (is_array($events)) {
                    $events[] = 'wait';
                }

                return [
                    'status' => ['code' => 0, 'details' => 'OK'],
                    'message' => $response->serializeToString(),
                ];
            },
        );

        return new GrpcFuture($call, $response::class);
    }

    private function scanResponseWithKeys(string ...$keys): RawScanResponse
    {
        $pairs = [];
        foreach ($keys as $key) {
            $pair = new KvPair();
            $pair->setKey($key);
            $pair->setValue('v-' . $key);
            $pairs[] = $pair;
        }

        $response = new RawScanResponse();
        $response->setKvs($pairs);

        return $response;
    }

    // ========================================================================
    // Cluster ID
    // ========================================================================

    public function testGetClusterIdReturnsNullWhenNotDiscovered(): void
    {
        $this->pdClient->method('getClusterId')->willReturn(null);

        $this->assertNull($this->client->getClusterId());
    }

    public function testGetClusterIdReturnsDiscoveredId(): void
    {
        $this->pdClient->method('getClusterId')->willReturn(12345);

        $this->assertSame(12345, $this->client->getClusterId());
    }

    // ========================================================================
    // Health check
    // ========================================================================

    public function testHealthCheckReturnsLearnedClusterId(): void
    {
        $this->pdClient->expects($this->once())->method('ping')->willReturn(12345);

        $this->assertSame(12345, $this->client->healthCheck());
    }

    public function testHealthCheckReturnsNullWhenPdHasNoClusterIdHeader(): void
    {
        $this->pdClient->expects($this->once())->method('ping')->willReturn(null);

        $this->assertNull($this->client->healthCheck());
    }

    public function testHealthCheckThrowsHealthCheckExceptionOnTransportError(): void
    {
        $this->pdClient->expects($this->once())->method('ping')->willThrowException(
            new GrpcException(details: 'connection refused', grpcStatusCode: 14),
        );

        $this->expectException(HealthCheckException::class);
        $this->expectExceptionMessage('PD health check failed');
        $this->client->healthCheck();
    }

    public function testHealthCheckRethrowsClientClosedException(): void
    {
        $this->client->close();

        $this->expectException(ClientClosedException::class);
        $this->client->healthCheck();
    }

    // ========================================================================
    // Metrics
    // ========================================================================

    public function testGetMetricsReturnsNoOpByDefault(): void
    {
        $this->assertInstanceOf(NoOpMetrics::class, $this->client->getMetrics());
    }

    public function testGetMetricsReturnsInjectedInstance(): void
    {
        $metrics = new InMemoryMetrics();
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            metrics: $metrics,
        );

        $this->assertSame($metrics, $client->getMetrics());
    }

    public function testGetMetricsDoesNotThrowWhenNeverQueried(): void
    {
        // The default-construction path must yield a fully working NoOp
        // metrics backend without any additional setup.
        $metrics = $this->client->getMetrics();

        $metrics->rpcStarted('dummy');
        $metrics->rpcCompleted('dummy', 1.0, true);
        $metrics->retryAttempted('NotLeader');
        $metrics->regionCacheHit('dummy');
        $metrics->regionCacheMiss('dummy');
        $metrics->regionInvalidated('not_leader');

        $this->assertInstanceOf(MetricsInterface::class, $metrics);
    }

    public function testCreateAcceptsMetricsOption(): void
    {
        $metrics = new InMemoryMetrics();
        // The create() factory requires real gRPC; we use mocking via the constructor instead.
        $client = new RawKvClient(
            $this->createMock(PdClientInterface::class),
            $this->createMock(GrpcClientInterface::class),
            $this->createMock(RegionCacheInterface::class),
            metrics: $metrics,
        );

        $this->assertSame($metrics, $client->getMetrics());
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

    public function testCreateRejectsNonInterfaceMetricsOption(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['metrics'] must be an instance of MetricsInterface");
        RawKvClient::create(['pd:2379'], options: ['metrics' => new \stdClass()]);
    }

    public function testCloseIsIdempotent(): void
    {
        $this->client->close();
        $this->client->close();
        $this->expectNotToPerformAssertions();
    }

    public function testCloseContinuesWhenGrpcCloseThrows(): void
    {
        $this->grpc->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('grpc boom'));
        $this->pdClient->expects($this->once())->method('close');

        $this->client->close();
    }

    public function testCloseContinuesWhenPdCloseThrows(): void
    {
        $this->grpc->expects($this->once())->method('close');
        $this->pdClient->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('pd boom'));

        $this->client->close();
    }

    public function testCloseSwallowsBothSubCloseExceptions(): void
    {
        $this->grpc->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('grpc boom'));
        $this->pdClient->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('pd boom'));

        $this->client->close();
    }

    public function testCloseIsIdempotentAfterSubCloseThrows(): void
    {
        $this->grpc->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('grpc boom'));
        $this->pdClient->expects($this->once())
            ->method('close')
            ->willThrowException(new \RuntimeException('pd boom'));

        $this->client->close();
        $this->client->close();
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

    public function testBatchGetDispatchesExactlyOneRpcForSameRegionKeys(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);

        // Exactly one RawBatchGet RPC for the two-key single-region batch:
        // guards against a regression that would silently drop keys before
        // any RPC (issue #244). A *successful* batchGet response (ordered
        // results, null for missing keys) cannot be driven end-to-end in
        // unit tests because RawKvBatch hardcodes `new Call(...)` (see
        // docs/helpers/faq.md) — that mapping is pinned by the E2E suite
        // (RawKvE2ETest::testBatchPutAndBatchGet / testBatchGetReturnsKeysInOrder).
        //
        // maxBackoffMs=1 makes the wait-phase retry (#183) abort on the first
        // backoff (TiKvRpc base 100 ms > 1 ms) before re-dispatching, so
        // getChannel() is called exactly once for the one sub-batch.
        $this->grpc->expects($this->exactly(1))->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // No TiKV server in unit tests: the request reaches the transport
        // layer and fails at connection time (issue #322 pattern).
        $this->expectException(BatchPartialFailureException::class);

        (new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, maxBackoffMs: 1))
            ->batchGet(['k1', 'k2']);
    }

    public function testBatchGetAcceptsNumericStringKeys(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // Pre-fix: int keys from array_keys() hit validateKeyNotEmpty(string)
        // and throw a TypeError. Post-fix the keys are normalized and the
        // batch reaches the transport layer; with no TiKV server the batch
        // fails at connection time (issue #322). maxBackoffMs=1 aborts the
        // #183 wait-phase retry before re-dispatching.
        $this->expectException(BatchPartialFailureException::class);

        (new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, maxBackoffMs: 1))
            ->batchGet(array_keys(['12345' => 'v1', '0' => 'v2']));
    }

    public function testBatchGetThrowsOnNonStringKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch keys must be strings or ints, bool given');
        $this->client->batchGet([true]); // @phpstan-ignore argument.type
    }

    // ========================================================================
    // batchPut()
    // ========================================================================

    public function testBatchPutEmptyIsNoop(): void
    {
        $this->grpc->expects($this->never())->method('call');
        $this->client->batchPut([]);
    }

    public function testBatchPutAcceptsNumericStringKeys(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // Pre-fix: PHP coerces the "12345"/"0" array keys to int, so
        // validateKeyNotEmpty(string) throws a TypeError here. Post-fix the
        // wire pairs are built and the request only fails at the transport
        // layer, because there is no TiKV server in unit tests. maxBackoffMs=1
        // aborts the #183 wait-phase retry before re-dispatching.
        $this->expectException(BatchPartialFailureException::class);

        // PHP models a literal "12345"/"0" array key as int; build the pairs
        // through a string-typed key so the map reaches batchPut() with its
        // declared contract (numeric-string keys must survive to the wire).
        $pairs = $this->stringKeyedPairs('12345', 'v') + $this->stringKeyedPairs('0', 'w');
        (new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, maxBackoffMs: 1))
            ->batchPut($pairs);
    }

    /**
     * @return array<string, string>
     */
    private function stringKeyedPairs(string $key, string $value): array
    {
        return [$key => $value];
    }

    // ========================================================================
    // batchDelete()
    // ========================================================================

    public function testBatchDeleteEmptyIsNoop(): void
    {
        $this->grpc->expects($this->never())->method('call');
        $this->client->batchDelete([]);
    }

    public function testBatchDeleteAcceptsNumericStringKeys(): void
    {
        $this->requireGrpcExtension();

        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        // Pre-fix: int keys from array_keys() hit validateKeyNotEmpty(string)
        // and throw a TypeError. Post-fix the keys are normalized and the
        // batch only fails at the transport layer, because there is no TiKV
        // server in unit tests (issue #322). maxBackoffMs=1 aborts the #183
        // wait-phase retry before re-dispatching.
        $this->expectException(BatchPartialFailureException::class);

        (new RawKvClient($this->pdClient, $this->grpc, $this->regionCache, maxBackoffMs: 1))
            ->batchDelete(array_keys(['12345' => 'v1', '0' => 'v2']));
    }

    public function testBatchDeleteThrowsOnNonStringKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch keys must be strings or ints, bool given');
        $this->client->batchDelete([true]); // @phpstan-ignore argument.type
    }

    // ========================================================================
    // scan()
    // ========================================================================

    public function testScanLimitZeroAppliesMaxScanRowsGuard(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$this->defaultRegion()]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');

        $response = new RawScanResponse();
        $response->setKvs([$pair1, $pair2]);

        $this->grpc->method('call')->willReturn($response);

        // limit 0 no longer silently caps at MAX_SCAN_LIMIT; the whole range
        // is collected and a guard throws when it exceeds options['maxScanRows']
        // (issue #191).
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            maxScanRows: 1,
        );

        try {
            $client->scan('k', 'l', 0);
            $this->fail('Expected ScanLimitExceededException was not thrown');
        } catch (ScanLimitExceededException $e) {
            $this->assertSame(1, $e->getMaxRows());
            $this->assertSame(2, $e->getScannedRows());
        }
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

    // ========================================================================
    // scan() – negative limit validation (issue #332)
    //
    // A negative limit previously passed through to RawScanRequest::setLimit(),
    // a uint32 protobuf field, so setLimit(-1) serialised to 4294967295 and
    // TiKV ran an effectively unbounded scan. Every public scan entry point
    // must reject negatives with 'Scan limit must be 0 or greater'.
    // ========================================================================

    /**
     * @param int $limit a negative limit arriving from user input
     */
    #[DataProvider('negativeScanLimitProvider')]
    public function testScanNegativeLimitThrows(int $limit): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $this->client->scan('a', 'z', $limit);
    }

    public function testReverseScanNegativeLimitThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $this->client->reverseScan('z', 'a', -1);
    }

    public function testScanPrefixNegativeLimitThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $this->client->scanPrefix('prefix', -1);
    }

    public function testBatchScanNegativeEachLimitThrows(): void
    {
        // batchScan() uses a `<= 0` guard with its own message; assert
        // THAT existing contract rather than the scan() message.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('eachLimit must be greater than 0');

        $this->client->batchScan([['a', 'z']], -1);
    }

    public function testScanIteratorNegativeBatchSizeThrows(): void
    {
        // ScanIterator's constructor uses a `<= 0` guard with its own
        // message; assert that existing contract.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchSize must be greater than 0');

        $this->client->scanIterator('a', 'z', -1);
    }

    public function testScanPrefixIteratorNegativeBatchSizeThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchSize must be greater than 0');

        $this->client->scanPrefixIterator('prefix', -1);
    }

    /** @return iterable<string, array{int}> */
    public static function negativeScanLimitProvider(): iterable
    {
        yield 'minus one' => [-1];
        yield 'minus large' => [-10240];
        yield 'int min' => [PHP_INT_MIN];
    }

    public function testScanReturnsResults(): void
    {
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
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
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
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
        $this->regionCache->method('getByKey')->willReturn($this->defaultRegion());
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

        $this->regionCache->method('getByKey')->willReturn($region);
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

        $this->regionCache->method('getByKey')->willReturn($region);
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

        // batchScan fans out per-range sends via callAsync (issue #295);
        // one future per range, resolved in dispatch order.
        $responses = [$response, $response2];
        $this->grpc->method('callAsync')
            ->willReturnCallback(function () use (&$responses): GrpcFuture {
                return $this->okFuture(
                    array_shift($responses) ?? new RawScanResponse(),
                );
            });

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

        $this->expectException(\TypeError::class);
        $this->client->batchScan([['only-one']], 10); // @phpstan-ignore argument.type
    }

    public function testBatchScanEmptyReturnsEmpty(): void
    {
        $this->assertSame([], $this->client->batchScan([], 10));
    }

    // ========================================================================
    // scan() – unbounded multi-region fan-out (issue #293)
    // ========================================================================

    public function testUnboundedMultiRegionScanDispatchesEverySendBeforeAwaiting(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 'z',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $region2]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        // The warm cache covers the whole range: no PD scanRegions() at all.
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');

        $response1 = new RawScanResponse();
        $response1->setKvs([$pair1]);
        $response2 = new RawScanResponse();
        $response2->setKvs([$pair2]);

        /** @var list<string> $events */
        $events = [];
        $responses = [$response1, $response2];
        $this->grpc->expects($this->exactly(2))->method('callAsync')->willReturnCallback(
            function () use (&$responses, &$events): GrpcFuture {
                $events[] = 'dispatch';

                return $this->okFuture(array_shift($responses) ?? new RawScanResponse(), $events);
            },
        );

        $result = $this->client->scan('a', 'z', 0, false);

        $this->assertSame(['k1', 'k2'], array_column($result, 'key'));
        // Both RawScan sends are issued (dispatch) before either response is
        // awaited, and the results are concatenated in region order.
        $this->assertSame(['dispatch', 'dispatch', 'wait', 'wait'], $events);
    }

    public function testUnboundedScanFallsBackToSequentialOnRegionError(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 'z',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $region2]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => $key < 'm' ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('not leader');
        $regionError = new RawScanResponse();
        $regionError->setRegionError($error);

        // The region error sits on the DATA-BEARING region and the other
        // parallel response is empty: without the guard parseScanPairs()
        // would drop k1 and the final assertion would be [] — so passing it
        // proves the sequential fallback actually ran.
        $asyncResponses = [$regionError, new RawScanResponse()];
        $this->grpc->method('callAsync')->willReturnCallback(
            function () use (&$asyncResponses): GrpcFuture {
                return $this->okFuture(array_shift($asyncResponses) ?? new RawScanResponse());
            },
        );

        // The fan-out never uses the synchronous call path; the sequential
        // fallback does, so assert it was exercised (not vacuous).
        $this->grpc->expects($this->atLeastOnce())->method('call')->willReturnCallback(
            function (string $address, string $service, string $method, Message $request): Message {
                $response = new RawScanResponse();
                /** @var RawScanRequest $request */
                if ($request->getStartKey() === 'a') {
                    $pair = new KvPair();
                    $pair->setKey('k1');
                    $pair->setValue('v1');
                    $response->setKvs([$pair]);
                }

                return $response;
            },
        );

        $result = $this->client->scan('a', 'z', 0, false);

        $this->assertSame(['k1'], array_column($result, 'key'));
    }

    public function testUnboundedScanSplitContinuationUsesRemainingBudget(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 'z',
        );
        // The last enumerated region shrank to [m, q) before the send: the
        // fan-out must continue from 'q' to 'z' with the real budget.
        $shrunkenRegion2 = new RegionInfo(
            regionId: 3,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 2,
            epochVersion: 2,
            startKey: 'm',
            endKey: 'q',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $region2]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($region1, $region2, $shrunkenRegion2): RegionInfo {
                if (strcmp($key, 'm') < 0) {
                    return $region1;
                }

                return strcmp($key, 'q') < 0 ? $shrunkenRegion2 : $region2;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $pair1 = new KvPair();
        $pair1->setKey('k1');
        $pair1->setValue('v1');
        $pair2 = new KvPair();
        $pair2->setKey('k2');
        $pair2->setValue('v2');

        $response1 = new RawScanResponse();
        $response1->setKvs([$pair1]);
        $response2 = new RawScanResponse();
        $response2->setKvs([$pair2]);

        $asyncResponses = [$response1, $response2];
        $this->grpc->method('callAsync')->willReturnCallback(
            function () use (&$asyncResponses): GrpcFuture {
                return $this->okFuture(array_shift($asyncResponses) ?? new RawScanResponse());
            },
        );

        /** @var list<RawScanRequest> $syncRequests */
        $syncRequests = [];
        $this->grpc->expects($this->atLeastOnce())->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$syncRequests): Message {
                /** @var RawScanRequest $request */
                $syncRequests[] = $request;

                return new RawScanResponse();
            },
        );

        $result = $this->client->scan('a', 'z', 0, false);

        $this->assertSame(['k1', 'k2'], array_column($result, 'key'));
        $this->assertNotEmpty($syncRequests);
        // The continuation must carry the real remaining budget: 0 would be
        // treated as "unbounded" by executeScanForRegion() and issue a full
        // unbounded wire read of the remainder before trimming.
        $this->assertSame('q', $syncRequests[0]->getStartKey());
        $this->assertGreaterThan(0, $syncRequests[0]->getLimit());
    }

    public function testUnboundedScanFanOutIsWindowedAndStopsWhenBudgetExhausted(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 't',
        );
        $region3 = new RegionInfo(
            regionId: 3,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 't',
            endKey: 'z',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $region2, $region3]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($region1, $region2, $region3): RegionInfo {
                if ($key < 'm') {
                    return $region1;
                }

                return $key < 't' ? $region2 : $region3;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // Concurrency cap of 2 over 3 regions: only the first window is
        // dispatched before the page budget is exhausted by that window's
        // data, so the third region is not part of the first page. Because
        // limit 0 now paginates (issue #191), only the first two calls carry
        // data; the follow-up pages return empty so the scan terminates.
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            maxConcurrency: 2,
        );

        $pairs = [];
        for ($i = 0; $i < RawKvClient::MAX_SCAN_LIMIT; $i++) {
            $pair = new KvPair();
            $pair->setKey(sprintf('k%05d', $i));
            $pair->setValue('v');
            $pairs[] = $pair;
        }
        $full = new RawScanResponse();
        $full->setKvs($pairs);

        /** @var list<RawScanRequest> $requests */
        $requests = [];
        $callCount = 0;
        // Index of the first follow-up-page dispatch (an empty response).
        // Everything captured before it belongs to the first page's window.
        $firstEmptyAt = null;
        $this->grpc->method('callAsync')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (
                &$requests,
                &$callCount,
                &$firstEmptyAt,
                $full,
            ): GrpcFuture {
                /** @var RawScanRequest $request */
                $requests[] = $request;
                $callCount++;

                if ($callCount > 2) {
                    $firstEmptyAt ??= count($requests) - 1;

                    return $this->okFuture(new RawScanResponse());
                }

                return $this->okFuture($full);
            },
        );

        $result = $client->scan('a', 'z', 0, false);

        $this->assertCount(RawKvClient::MAX_SCAN_LIMIT, $result);
        // The first page's window covers only region 1 ('a') and region 2
        // ('m'); region 3 ('t') is never dispatched in the same window.
        $this->assertGreaterThanOrEqual(2, count($requests));
        $this->assertSame('a', $requests[0]->getStartKey());
        $this->assertSame('m', $requests[1]->getStartKey());
        // Every dispatched region carries the (positive) remaining budget,
        // never an unbounded/zero limit.
        $this->assertGreaterThan(0, $requests[0]->getLimit());
        $this->assertGreaterThan(0, $requests[1]->getLimit());
        // Pin the first-page window composition: the follow-up page starts
        // with an empty response, so every request captured before it must be
        // exactly regions 1 and 2 — region 3 ('t') excluded.
        $this->assertNotNull($firstEmptyAt);
        $firstWindow = array_slice($requests, 0, $firstEmptyAt);
        $this->assertCount(2, $firstWindow);
        $this->assertSame(
            ['a', 'm'],
            array_map(
                static fn (RawScanRequest $request): string => $request->getStartKey(),
                $firstWindow,
            ),
        );
        // The window above is derived from the mock's call counter, so it
        // cannot on its own prove region 3 ('t') was excluded: whatever is
        // dispatched third is by construction the boundary array_slice()
        // cuts at. The third request is the *next page's* first dispatch,
        // carrying the page-1 continuation cursor ('k10239' . "\x00"), not
        // region 3 ('t'). If the per-window cap were removed, region 3 would
        // be dispatched in the first window and this start key would be 't'.
        $this->assertSame('k10239' . "\x00", $requests[2]->getStartKey());
    }

    public function testUnboundedScanPaginatesAcrossMultipleRegionsInParallelPages(): void
    {
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 't',
        );
        $region3 = new RegionInfo(
            regionId: 3,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 't',
            endKey: 'z',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $region2, $region3]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($region1, $region2, $region3): RegionInfo {
                if ($key < 'm') {
                    return $region1;
                }

                return $key < 't' ? $region2 : $region3;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // The production page size (MAX_SCAN_LIMIT = 10240) is too large for a
        // small deterministic test, so inject a scanner with a two-row page
        // budget; the client's own concurrency cap is separate and must also
        // be set on the scanner.
        $scanner = new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
            maxConcurrency: 2,
            scanPageSize: 2,
        );
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            scanner: $scanner,
            maxConcurrency: 2,
        );

        // Page 1 spans regions 1 ('a') and 2 ('m'), one row each → a full
        // two-row page; region 3 is never dispatched in the first window.
        // The cursor then advances past region 2's last key, so page 2 again
        // spans two segments (the remainder of region 2, then region 3):
        // region 2 yields nothing and region 3 yields the final row, a short
        // page that stops the scan. Both pages exercise the parallel branch.
        /** @var list<RawScanRequest> $requests */
        $requests = [];
        $this->grpc->method('callAsync')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$requests): GrpcFuture {
                /** @var RawScanRequest $request */
                $requests[] = $request;

                $response = match ($request->getStartKey()) {
                    'a' => $this->scanResponseWithKeys('k0'),
                    'm' => $this->scanResponseWithKeys('m0'),
                    't' => $this->scanResponseWithKeys('t0'),
                    default => new RawScanResponse(),
                };

                return $this->okFuture($response);
            },
        );

        $result = $client->scan('a', 'z', 0, false);

        $this->assertSame(['k0', 'm0', 't0'], array_column($result, 'key'));

        // Page 1 fan-out: regions 1 and 2. Page 2 fan-out: the clipped
        // remainder of region 2 (starting at the advanced cursor) and region
        // 3.
        $this->assertCount(4, $requests);
        $this->assertSame('a', $requests[0]->getStartKey());
        $this->assertSame('m', $requests[1]->getStartKey());
        $this->assertSame('m0' . "\x00", $requests[2]->getStartKey());
        $this->assertSame('t', $requests[3]->getStartKey());
    }

    public function testBatchScanRetriesSubRangeOnRegionError(): void
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

        $this->regionCache->method('getRegionsInRange')->willReturn([$region]);
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $error = new Error();
        $error->setMessage('epoch not match');
        $errorResponse = new RawScanResponse();
        $errorResponse->setRegionError($error);

        // The concurrent batchScan send reports a region error; the sub-range
        // must be re-run through the sequential retrying path rather than
        // returned as a silently partial result.
        $this->grpc->method('callAsync')->willReturnCallback(
            fn(): GrpcFuture => $this->okFuture($errorResponse),
        );

        $pair = new KvPair();
        $pair->setKey('k1');
        $pair->setValue('v1');
        $clean = new RawScanResponse();
        $clean->setKvs([$pair]);

        $this->grpc->expects($this->atLeastOnce())->method('call')->willReturn($clean);

        $result = $this->client->batchScan([['a', 'z']], 10);

        $this->assertSame('k1', $result[0][0]['key']);
    }

    public function testBatchScanRerunsMiddleSubRangeWhenRegionShrankAfterEnumeration(): void
    {
        // The cached chain is stale: the middle region still claims [m, t)
        // while getByKey() already sees it split into [m, q) + [q, t). The
        // concurrent send for the stale [m, t) can therefore only cover
        // [m, q), and the waiter must re-run that whole sub-range through the
        // sequential path instead of dropping [q, t).
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $staleMiddle = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 't',
        );
        $shrunkenMiddle = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 2,
            epochVersion: 2,
            startKey: 'm',
            endKey: 'q',
        );
        $middleTail = new RegionInfo(
            regionId: 3,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 2,
            epochVersion: 2,
            startKey: 'q',
            endKey: 't',
        );
        $region4 = new RegionInfo(
            regionId: 4,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 't',
            endKey: 'z',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $staleMiddle, $region4]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($region1, $shrunkenMiddle, $middleTail, $region4): RegionInfo {
                if (strcmp($key, 'm') < 0) {
                    return $region1;
                }
                if (strcmp($key, 'q') < 0) {
                    return $shrunkenMiddle;
                }

                return strcmp($key, 't') < 0 ? $middleTail : $region4;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // One concurrent send per enumerated segment; each response carries a
        // key inside its (stale) range. The middle send only reaches [m, q).
        $asyncResponses = [
            $this->scanResponseWithKeys('a1'),
            $this->scanResponseWithKeys('m1'),
            $this->scanResponseWithKeys('t1'),
        ];
        $this->grpc->method('callAsync')->willReturnCallback(
            function () use (&$asyncResponses): GrpcFuture {
                return $this->okFuture(array_shift($asyncResponses) ?? new RawScanResponse());
            },
        );

        // The sequential retrying path (sync call) must cover the shrunken
        // middle sub-range in full: [m, q) then [q, t).
        /** @var list<RawScanRequest> $syncRequests */
        $syncRequests = [];
        $this->grpc->expects($this->atLeastOnce())->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$syncRequests): Message {
                /** @var RawScanRequest $request */
                $syncRequests[] = $request;

                if ($request->getStartKey() === 'm') {
                    return $this->scanResponseWithKeys('m1');
                }
                if ($request->getStartKey() === 'q') {
                    return $this->scanResponseWithKeys('q1', 's1');
                }

                return new RawScanResponse();
            },
        );

        $result = $this->client->batchScan([['a', 'z']], 10);

        // q1/s1 prove [q, t) was not dropped; a single m1 proves the
        // concurrent middle batch was discarded, not concatenated.
        $this->assertSame(
            ['a1', 'm1', 'q1', 's1', 't1'],
            array_column($result[0], 'key'),
        );
        $this->assertSame(['m', 'q'], array_map(
            static fn(RawScanRequest $request): string => $request->getStartKey(),
            $syncRequests,
        ));
    }

    public function testUnboundedScanFallsBackToSequentialWhenMiddleRegionShrank(): void
    {
        // Same stale-chain/shrunken-middle setup as the batchScan case, but
        // driven through the unbounded fan-out (scanSegmentsInParallel): the
        // non-final shrunken segment must trigger the sequential fallback so
        // the whole [a, z) page is covered without a hole.
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'a',
            endKey: 'm',
        );
        $staleMiddle = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 't',
        );
        $shrunkenMiddle = new RegionInfo(
            regionId: 2,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 2,
            epochVersion: 2,
            startKey: 'm',
            endKey: 'q',
        );
        $middleTail = new RegionInfo(
            regionId: 3,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 2,
            epochVersion: 2,
            startKey: 'q',
            endKey: 't',
        );
        $region4 = new RegionInfo(
            regionId: 4,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 't',
            endKey: 'z',
        );

        $this->regionCache->method('getRegionsInRange')->willReturn([$region1, $staleMiddle, $region4]);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($region1, $shrunkenMiddle, $middleTail, $region4): RegionInfo {
                if (strcmp($key, 'm') < 0) {
                    return $region1;
                }
                if (strcmp($key, 'q') < 0) {
                    return $shrunkenMiddle;
                }

                return strcmp($key, 't') < 0 ? $middleTail : $region4;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // Concurrent sends are discarded once the fallback fires.
        $this->grpc->method('callAsync')->willReturnCallback(
            fn(): GrpcFuture => $this->okFuture(new RawScanResponse()),
        );

        /** @var list<string> $syncStarts */
        $syncStarts = [];
        $this->grpc->expects($this->atLeastOnce())->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$syncStarts): Message {
                /** @var RawScanRequest $request */
                $syncStarts[] = $request->getStartKey();

                return match ($request->getStartKey()) {
                    'a' => $this->scanResponseWithKeys('a1'),
                    'm' => $this->scanResponseWithKeys('m1'),
                    'q' => $this->scanResponseWithKeys('q1', 's1'),
                    't' => $this->scanResponseWithKeys('t1'),
                    default => new RawScanResponse(),
                };
            },
        );

        $result = $this->client->scan('a', 'z', 0, false);

        $this->assertSame(
            ['a1', 'm1', 'q1', 's1', 't1'],
            array_column($result, 'key'),
        );
        // The fallback re-scans the page sequentially from the first segment.
        $this->assertSame(['a', 'm', 'q', 't'], $syncStarts);
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

    public function testCompareAndSwapDoesNotRetryOnGrpcException(): void
    {
        $this->client->setAtomicForCAS(true);

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        // A CAS is not idempotent: a transport error means the outcome is
        // indeterminate (the write may have been applied server-side), so it
        // must propagate to the caller instead of being retried (issue #239).
        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new GrpcException('Deadline Exceeded', 4));

        $this->expectException(GrpcException::class);

        $this->client->compareAndSwap('key', 'old', 'new');
    }

    public function testCompareAndSwapStillRetriesRegionErrors(): void
    {
        $this->client->setAtomicForCAS(true);

        $region = $this->regionWithPeers();
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('invalidate');
        $this->regionCache->method('switchLeader')->willReturn(true);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $leader = new Peer();
        $leader->setId(30);
        $leader->setStoreId(3);

        $notLeader = new NotLeader();
        $notLeader->setRegionId(1);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $errorResponse = new RawCASResponse();
        $errorResponse->setRegionError($error);

        $successResponse = new RawCASResponse();
        $successResponse->setSucceed(true);
        $successResponse->setPreviousNotExist(true);

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls($errorResponse, $successResponse);

        $result = $this->client->compareAndSwap('key', null, 'new');

        $this->assertTrue($result->swapped);
        $this->assertNull($result->previousValue);
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
        $region = new RegionInfo(1, 1, 1, 1, 1, 'a', 'z');
        // checksum() pre-populates the cache from scanRegions(); the per-region
        // retried closure then resolves the region from that cache (issue #190).
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawChecksumResponse();
        $response->setChecksum(12345);
        $response->setTotalKvs(3);
        $response->setTotalBytes(100);

        // checksum() fans out its per-region send via callAsync (issue #295).
        $this->grpc->method('callAsync')
            ->willReturnCallback(fn(): GrpcFuture => $this->okFuture($response));

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
            ->with('Retrying operation', $this->callback(fn(array $context): bool => is_string($context['key'])
                && str_contains($context['key'], 'bytes')
                && ! str_contains($context['key'], 'key')
                && $context['attempt'] === 0
                // EpochNotMatch has a small jittered backoff since #241
                // (base 2 ms): the sleep is non-zero but still small.
                && $context['backoffType'] === 'EpochNotMatch'
                && $context['sleepMs'] >= 1
                && $context['sleepMs'] <= 2));

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
            ->with('Invalidated region on retry', $this->callback(fn(array $context): bool => is_string($context['key'])
                && str_contains($context['key'], 'bytes')
                && ! str_contains($context['key'], 'key')
                && $context['regionId'] === 1));

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
            ->with('ServerBusy budget exhausted', $this->callback(fn(array $context): bool => is_string($context['key'])
                && str_contains($context['key'], 'bytes')
                && ! str_contains($context['key'], 'key')
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
            ->with('Fatal error, not retrying', $this->callback(fn(array $context): bool => is_string($context['key'])
                && str_contains($context['key'], 'bytes')
                && ! str_contains($context['key'], 'key')
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
        $this->assertTrue(\CrazyGoat\TiKV\Client\Retry\BackoffType::NotLeader->equalJitter());
        $sleep = \CrazyGoat\TiKV\Client\Retry\BackoffType::NotLeader->sleepMs(0);
        // Equal jitter on base 2 ms: delay is in [1, 2] (issue #242).
        $this->assertGreaterThanOrEqual(1, $sleep);
        $this->assertLessThanOrEqual(2, $sleep);
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
        $this->expectException(InvalidStateException::class);
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
        $this->expectException(InvalidStateException::class);
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

    // ========================================================================
    // Ingest (SST bulk import)
    // ========================================================================

    public function testIngestThrowsClientClosedException(): void
    {
        $this->client->close();

        $this->expectException(ClientClosedException::class);
        $this->client->ingest(['key' => 'value']);
    }

    public function testIngestWithEmptyArrayDoesNothing(): void
    {
        $this->grpc->expects($this->never())->method('call');

        $this->client->ingest([]);
    }

    public function testIngestValidatesEmptyKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key must not be empty in ingest');
        $this->client->ingest(['' => 'value']);
    }

    public function testIngestValidatesKeySize(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exceeds maximum allowed size');
        $this->client->ingest([str_repeat('a', RawKvClient::MAX_KEY_SIZE + 1) => 'value']);
    }

    public function testIngestValidatesValueSize(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exceeds maximum allowed size');
        $this->client->ingest(['key' => str_repeat('a', RawKvClient::MAX_VALUE_SIZE + 1)]);
    }
}
