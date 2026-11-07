<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\Proto\ImportSstpb\Error as ImportSstError;
use CrazyGoat\Proto\ImportSstpb\IngestRequest;
use CrazyGoat\Proto\ImportSstpb\IngestResponse;
use CrazyGoat\Proto\ImportSstpb\Pair;
use CrazyGoat\Proto\ImportSstpb\RawWriteRequest;
use CrazyGoat\Proto\ImportSstpb\RawWriteResponse;
use CrazyGoat\Proto\ImportSstpb\SSTMeta;
use CrazyGoat\Proto\ImportSstpb\SwitchMode;
use CrazyGoat\Proto\ImportSstpb\SwitchModeRequest;
use CrazyGoat\Proto\ImportSstpb\SwitchModeResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\IngestPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\IngestTooLargeException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\SstIngestor;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class SstIngestorTest extends TestCase
{
    use GrpcExtensionGate;

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCache $regionCache;
    private RegionResolver $regionResolver;
    private SstIngestor $ingestor;

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
        $this->regionCache = new RegionCache();
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
        $this->ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            regionCache: $this->regionCache,
        );
    }

    /**
     * Future over a mocked gRPC call resolving with the given response.
     * Requires the grpc extension (a real \\Grpc\\Call mock drives it).
     */
    private function okFuture(Message $response): GrpcFuture
    {
        $this->requireGrpcExtension();

        $call = $this->createMock(\Grpc\Call::class);
        $call->method('startBatch')->willReturn([
            'status' => ['code' => 0, 'details' => 'OK'],
            'message' => $response->serializeToString(),
        ]);

        return new GrpcFuture($call, $response::class);
    }

    /**
     * Set up PD client mocks for a standard single-store, single-region scenario.
     */
    private function setupStandardPdMocks(RegionInfo $region, Store $store): void
    {
        // batchResolveRegions calls scanRegions.
        $this->pdClient->method('scanRegions')
            ->willReturn([$region]);

        // resolveStoreAddress calls getStore.
        $this->pdClient->method('getStore')
            ->willReturn($store);

        // getAllStores returns the store.
        $this->pdClient->method('getAllStores')
            ->willReturn([$store]);
    }

    /**
     * Create gRPC mocks: async calls (SwitchMode since issue #295) resolve
     * immediately with SwitchModeResponse, blocking Ingest calls return the
     * given response.
     */
    private function setupGrpcCallMock(?IngestResponse $ingestResponse = null): void
    {
        $ingestResponse ??= new IngestResponse();

        $this->grpc->method('callAsync')
            ->willReturnCallback(fn(): GrpcFuture => $this->okFuture(new SwitchModeResponse()));

        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
            ) use ($ingestResponse): Message {
                if ($method === 'SwitchMode') {
                    return new SwitchModeResponse();
                }

                return $ingestResponse;
            });
    }

    // ========================================================================
    // Empty input
    // ========================================================================

    public function testIngestWithEmptyArrayDoesNothing(): void
    {
        $this->pdClient->expects($this->never())->method('getAllStores');
        $this->grpc->expects($this->never())->method('call');

        $this->ingestor->ingest([]);
    }

    public function testIngestAcceptsPayloadExactlyAtMaxIngestSize(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();
        $this->setupStandardPdMocks($region, $store);
        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));
        $this->grpc->method('call')->willReturn(new IngestResponse());

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->expects($this->once())->method('callStreaming')->willReturn($writeResponse);

        // "ab" + "cde" = 5 bytes, exactly the limit.
        $ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
            maxIngestSize: 5,
        );

        $ingestor->ingest(['ab' => 'cde']);
    }

    public function testIngestRejectsPayloadOverMaxIngestSizeBeforeAnyRpc(): void
    {
        $this->pdClient->expects($this->never())->method('getAllStores');
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callStreaming');

        $ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
            maxIngestSize: 5,
        );

        try {
            $ingestor->ingest(['ab' => 'cdef']);
            self::fail('The payload guard did not reject the ingest');
        } catch (IngestTooLargeException $e) {
            self::assertSame('maxIngestSize', $e->getLimitName());
            self::assertSame(5, $e->getLimit());
            self::assertSame(6, $e->getActual());
            self::assertStringContainsString('maxIngestSize of 5 bytes', $e->getMessage());
        }
    }

    public function testIngestStreamsRequestsLazily(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();
        $this->setupStandardPdMocks($region, $store);
        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));
        $this->grpc->method('call')->willReturn(new IngestResponse());

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);

        $captured = null;
        $this->grpc->method('callStreaming')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                iterable $requests,
            ) use (
                $writeResponse,
                &$captured
): Message {
                $captured = $requests;
                return $writeResponse;
            });

        $pairs = [];
        for ($i = 0; $i < 2500; $i++) {
            $pairs[sprintf('key-%05d', $i)] = str_repeat('v', 32);
        }

        $this->ingestor->ingest($pairs);

        self::assertInstanceOf(\Generator::class, $captured, 'The stream must be lazy, not a pre-built array');
        $requests = iterator_to_array($captured, false);
        // One metadata message plus ceil(2500 / 1024) data chunks.
        self::assertCount(1 + (int) ceil(2500 / 1024), $requests);
        $first = $requests[0];
        self::assertInstanceOf(RawWriteRequest::class, $first);
        $meta = $first->getMeta();
        self::assertNotNull($meta);
        self::assertSame(2500, $meta->getTotalKvs());
        self::assertSame(2500 * (9 + 32), $meta->getTotalBytes());
        self::assertNull($first->getBatch());

        $second = $requests[1];
        self::assertInstanceOf(RawWriteRequest::class, $second);
        $batch = $second->getBatch();
        self::assertNotNull($batch);
        self::assertCount(1024, iterator_to_array($batch->getPairs(), false));
        $last = $requests[count($requests) - 1];
        self::assertInstanceOf(RawWriteRequest::class, $last);
        $lastBatch = $last->getBatch();
        self::assertNotNull($lastBatch);
        self::assertCount(2500 - 2 * 1024, iterator_to_array($lastBatch->getPairs(), false));
    }

    public function testIngestBoundsAChunkByPayloadBytesNotOnlyPairCount(): void
    {
        $region = $this->defaultRegion();
        $store = $this->defaultStore();
        $this->setupStandardPdMocks($region, $store);
        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));
        $this->grpc->method('call')->willReturn(new IngestResponse());

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);

        $captured = null;
        $this->grpc->method('callStreaming')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                iterable $requests,
            ) use (
                $writeResponse,
                &$captured
): Message {
                $captured = $requests;
                return $writeResponse;
            });

        $value = str_repeat('v', 1024 * 1024);
        $pairs = [];
        for ($i = 0; $i < 6; $i++) {
            $pairs[sprintf('key-%03d', $i)] = $value;
        }

        $this->ingestor->ingest($pairs);

        self::assertInstanceOf(\Generator::class, $captured);
        $requests = iterator_to_array($captured, false);
        self::assertCount(3, $requests);

        // Three 1 MiB values plus their keys fit in the 4 MiB chunk budget;
        // the fourth would exceed it by 28 bytes, so the generator starts a
        // second chunk instead of copying all six values into one.
        foreach ([1 => 3, 2 => 3] as $index => $expectedPairs) {
            $request = $requests[$index];
            self::assertInstanceOf(RawWriteRequest::class, $request);
            $batch = $request->getBatch();
            self::assertNotNull($batch);
            self::assertCount($expectedPairs, iterator_to_array($batch->getPairs(), false));
        }
    }

    // ========================================================================
    // Basic ingest flow
    // ========================================================================

    public function testIngestSortsKeysAndGroupsByRegion(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);
        $this->setupGrpcCallMock();

        // Write returns a successful response.
        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);

        $streamingCalled = false;
        $this->grpc->method('callStreaming')
            ->willReturnCallback(function () use ($writeResponse, &$streamingCalled): Message {
                $streamingCalled = true;
                return $writeResponse;
            });

        $this->ingestor->ingest([
            'key3' => 'value3',
            'key1' => 'value1',
            'key2' => 'value2',
        ]);

        // Streaming call should have been invoked.
        $this->assertTrue($streamingCalled, 'Expected callStreaming to be called');
    }

    // ========================================================================
    // SST metadata checksum
    // ========================================================================

    public function testIngestSendsTheTrueCrc32OfThePayload(): void
    {
        $this->assertSame(
            hexdec(hash('crc32b', 'key1' . 'value1')),
            $this->capturedMeta(['key1' => 'value1'])->getCrc32() & 0xFFFFFFFF,
        );
    }

    public function testIngestSendsNonZeroCrc32WhenTheHexDigestStartsWithALetter(): void
    {
        // hash('crc32b', 'key0value0') = "a3b5819c"; the old `(int)` cast of
        // the hex string truncated it to 0 (issue #196).
        self::assertSame('a3b5819c', hash('crc32b', 'key0' . 'value0'));

        // The protobuf PHP runtime normalises a uint32 above 2^31-1 to its
        // signed 32-bit representation, so compare the unsigned wire value.
        $crc32 = $this->capturedMeta(['key0' => 'value0'])->getCrc32() & 0xFFFFFFFF;

        self::assertSame(hexdec('a3b5819c'), $crc32);
        self::assertNotSame(0, $crc32);
    }

    /**
     * Ingest a single-region payload and return the metadata of the first
     * `RawWrite` request.
     *
     * @param array<string, string> $pairs
     */
    private function capturedMeta(array $pairs): SSTMeta
    {
        $region = $this->defaultRegion();
        $store = $this->defaultStore();
        $this->setupStandardPdMocks($region, $store);
        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));
        $this->grpc->method('call')->willReturn(new IngestResponse());

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);

        $captured = null;
        $this->grpc->method('callStreaming')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                iterable $requests,
            ) use (
                $writeResponse,
                &$captured
            ): Message {
                $captured = $requests;
                return $writeResponse;
            });

        $this->ingestor->ingest($pairs);

        self::assertNotNull($captured, 'Expected callStreaming to be called');
        $requests = iterator_to_array($captured, false);
        $first = $requests[0];
        self::assertInstanceOf(RawWriteRequest::class, $first);
        $meta = $first->getMeta();
        self::assertNotNull($meta);

        return $meta;
    }

    // ========================================================================
    // Switch mode error handling
    // ========================================================================

    public function testIngestSwitchesBackToNormalModeOnWriteError(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);
        // The retried closure re-resolves the region after the write error.
        // Return a real RegionInfo explicitly: PHPUnit cannot auto-generate
        // one because RegionInfo is final.
        $this->pdClient->method('getRegion')->willReturn($region);

        // Keep the retryable write error from driving a long backoff loop;
        // the test only asserts that normal mode is restored.
        $this->ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
            maxBackoffMs: 1,
        );

        // SwitchMode goes through callAsync since issue #295.
        $switchModeAddresses = [];
        $this->grpc->method('callAsync')
            ->willReturnCallback(function (string $address) use (&$switchModeAddresses): GrpcFuture {
                $switchModeAddresses[] = $address;

                return $this->okFuture(new SwitchModeResponse());
            });

        // Write fails with gRPC error.
        $this->grpc->method('callStreaming')
            ->willThrowException(new GrpcException('write failed', 13));

        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('write failed');

        try {
            $this->ingestor->ingest(['key1' => 'value1']);
        } finally {
            // Verify SwitchMode(Normal) was called after the error.
            // At least 2 calls: import mode + normal mode.
            $this->assertGreaterThanOrEqual(2, count($switchModeAddresses));
            $this->assertContains('tikv1:20160', $switchModeAddresses);
        }
    }

    // ========================================================================
    // Ingest response error
    // ========================================================================

    public function testIngestThrowsRegionExceptionOnIngestError(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);

        // Write succeeds.
        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        // Ingest returns error.
        $error = new Error();
        $error->setMessage('region not found');
        $ingestResponse = new IngestResponse();
        $ingestResponse->setError($error);

        $this->setupGrpcCallMock($ingestResponse);

        $this->expectException(RegionException::class);

        $this->ingestor->ingest(['key1' => 'value1']);
    }

    // ========================================================================
    // Retry and partial failure (issue #197)
    // ========================================================================

    public function testIngestRetriesEpochNotMatchOnTheSecondRegion(): void
    {
        $store = $this->defaultStore();
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: '',
        );
        // The retry re-resolves the region through PD after the stale cache
        // entry is invalidated; the fresh epoch is what makes the second
        // attempt succeed.
        $freshRegion2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'm',
            endKey: '',
        );

        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getAllStores')->willReturn([$store]);
        $this->pdClient->method('getRegion')->willReturn($freshRegion2);

        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        /** @var list<int> $ingestRegionIds */
        $ingestRegionIds = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                \Google\Protobuf\Internal\Message $request,
            ) use (&$ingestRegionIds): Message {
                if ($method === 'SwitchMode') {
                    return new SwitchModeResponse();
                }

                self::assertInstanceOf(IngestRequest::class, $request);
                $context = $request->getContext();
                self::assertNotNull($context);
                $regionId = (int) $context->getRegionId();
                $ingestRegionIds[] = $regionId;

                $region2Attempts = count(array_filter(
                    $ingestRegionIds,
                    static fn(int $id): bool => $id === 2,
                ));
                if ($regionId === 2 && $region2Attempts === 1) {
                    $error = new Error();
                    $error->setMessage('epoch not match');
                    $error->setEpochNotMatch(new EpochNotMatch());
                    $response = new IngestResponse();
                    $response->setError($error);

                    return $response;
                }

                return new IngestResponse();
            });

        $this->ingestor->ingest(['a' => 'value1', 'z' => 'value2']);

        // Region 1 once, region 2 twice: the EpochNotMatch was retried.
        self::assertSame([1, 2, 2], $ingestRegionIds);
    }

    public function testIngestReportsRegionsActuallyIngestedAfterARetryReResolution(): void
    {
        $store = $this->defaultStore();
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: 't',
        );
        // The retry re-resolves key "n" after the region-2 cache entry is
        // invalidated; the group now lives in region 3.
        $freshRegion3 = new RegionInfo(
            regionId: 3,
            leaderPeerId: 3,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 2,
            startKey: 'm',
            endKey: 't',
        );
        $region4 = new RegionInfo(
            regionId: 4,
            leaderPeerId: 4,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 't',
            endKey: '',
        );

        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2, $region4]);
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getAllStores')->willReturn([$store]);
        $this->pdClient->method('getRegion')->willReturn($freshRegion3);

        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        /** @var array<int, int> $attemptsByRegion */
        $attemptsByRegion = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                \Google\Protobuf\Internal\Message $request,
            ) use (&$attemptsByRegion): Message {
                if ($method === 'SwitchMode') {
                    return new SwitchModeResponse();
                }

                self::assertInstanceOf(IngestRequest::class, $request);
                $context = $request->getContext();
                self::assertNotNull($context);
                $regionId = (int) $context->getRegionId();
                $attemptsByRegion[$regionId] = ($attemptsByRegion[$regionId] ?? 0) + 1;

                if ($regionId === 2) {
                    $error = new Error();
                    $error->setMessage('epoch not match');
                    $error->setEpochNotMatch(new EpochNotMatch());
                    $response = new IngestResponse();
                    $response->setError($error);

                    return $response;
                }

                if ($regionId === 4) {
                    // No typed ErrorKind and no classifier keyword: fatal.
                    $error = new Error();
                    $error->setMessage('permanent ingest failure');
                    $response = new IngestResponse();
                    $response->setError($error);

                    return $response;
                }

                return new IngestResponse();
            });

        try {
            $this->ingestor->ingest(['a' => 'value1', 'n' => 'value2', 'z' => 'value3']);
            self::fail('The ingest did not fail');
        } catch (IngestPartialFailureException $e) {
            // Region 1 was applied, region 3 (not the grouping-time region 2)
            // was applied after the retry, and region 4 failed.
            self::assertSame([1, 3], $e->getAppliedRegionIds());
            self::assertSame(4, $e->getFailedRegionId());
            self::assertInstanceOf(RegionException::class, $e->getPrevious());
        }

        self::assertSame(1, $attemptsByRegion[2] ?? 0);
        self::assertSame(1, $attemptsByRegion[3] ?? 0);
    }

    public function testIngestGivesEachRegionGroupAFreshRetryBudget(): void
    {
        $store = $this->defaultStore();
        $region1 = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'm',
        );
        $region2 = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'm',
            endKey: '',
        );

        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getStore')->willReturn($store);
        $this->pdClient->method('getAllStores')->willReturn([$store]);

        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        /** @var list<int> $ingestRegionIds */
        $ingestRegionIds = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                \Google\Protobuf\Internal\Message $request,
            ) use (&$ingestRegionIds): Message {
                if ($method === 'SwitchMode') {
                    return new SwitchModeResponse();
                }

                self::assertInstanceOf(IngestRequest::class, $request);
                $context = $request->getContext();
                self::assertNotNull($context);
                $regionId = (int) $context->getRegionId();
                $ingestRegionIds[] = $regionId;

                // A slow but successful first group must not consume the
                // second group's retry budget.
                if ($regionId === 1) {
                    usleep(50000);
                }

                return new IngestResponse();
            });

        $ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
            retryDeadlineMs: 20,
        );

        $ingestor->ingest(['a' => 'value1', 'z' => 'value2']);

        self::assertSame([1, 2], $ingestRegionIds);
    }

    public function testIngestRethrowsRetryBudgetExhaustionWhenNoGroupWasApplied(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);
        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                \Google\Protobuf\Internal\Message $request,
            ): Message {
                if ($method === 'SwitchMode') {
                    return new SwitchModeResponse();
                }

                $error = new Error();
                $error->setMessage('epoch not match');
                $error->setEpochNotMatch(new EpochNotMatch());
                $response = new IngestResponse();
                $response->setError($error);

                return $response;
            });

        $ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
            retryDeadlineMs: 1,
        );

        try {
            $ingestor->ingest(['key1' => 'value1']);
            self::fail('The ingest did not fail');
        } catch (RetryBudgetExhaustedException $e) {
            self::assertStringContainsString('Retry deadline (1 ms) exhausted', $e->getMessage());
        } catch (IngestPartialFailureException) {
            self::fail('A first-group failure must not be reported as a partial ingest');
        }
    }

    public function testIngestFailingPermanentlyRestoresEveryStoreToNormalMode(): void
    {
        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress('tikv1:20160');

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress('tikv2:20160');

        $region = $this->defaultRegion();

        $this->pdClient->method('getAllStores')->willReturn([$store1, $store2]);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($store1);

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        $error = new Error();
        $error->setMessage('permanent ingest failure');
        $ingestResponse = new IngestResponse();
        $ingestResponse->setError($error);
        $this->grpc->method('call')->willReturn($ingestResponse);

        /** @var array<string, list<int>> $modesByAddress */
        $modesByAddress = [];
        $this->grpc->method('callAsync')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                \Google\Protobuf\Internal\Message $request,
            ) use (&$modesByAddress): GrpcFuture {
                if ($method === 'SwitchMode' && $request instanceof SwitchModeRequest) {
                    $modesByAddress[$address][] = $request->getMode();
                }

                return GrpcFuture::completed(new SwitchModeResponse());
            });

        try {
            $this->ingestor->ingest(['key1' => 'value1']);
            self::fail('The ingest did not fail');
        } catch (RegionException) {
            // Expected: the single region failed before anything was applied.
        }

        foreach (['tikv1:20160', 'tikv2:20160'] as $address) {
            self::assertSame(
                [SwitchMode::Import, SwitchMode::Normal],
                $modesByAddress[$address] ?? [],
                sprintf('Store %s was not restored to normal mode', $address),
            );
        }
    }

    // ========================================================================
    // RawWrite response error
    // ========================================================================

    /**
     * Regression test for issue #307 (SEC-04): the RawWrite store error used
     * to be JSON-encoded into the RegionException message, and an
     * errorpb.Error carries raw key fields (KeyNotInRegion::key,
     * StoreNotMatch, ...). Only its human-readable message may reach the
     * exception, so a key the server echoes back cannot leak into the logs.
     */
    public function testRawWriteStoreErrorDoesNotLeakRawKey(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();
        $this->setupStandardPdMocks($region, $store);
        $this->grpc->method('callAsync')
            ->willReturn(GrpcFuture::completed(new SwitchModeResponse()));

        $rawKey = 'user_email:alice@example.com';

        $keyNotInRegion = new KeyNotInRegion();
        $keyNotInRegion->setKey($rawKey);
        $keyNotInRegion->setRegionId(1);

        $storeError = new Error();
        $storeError->setMessage('store rejected the region');
        $storeError->setKeyNotInRegion($keyNotInRegion);

        $writeError = new ImportSstError();
        $writeError->setMessage('raw write failed');
        $writeError->setStoreError($storeError);

        $writeResponse = new RawWriteResponse();
        $writeResponse->setError($writeError);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        try {
            $this->ingestor->ingest([$rawKey => 'value']);
            self::fail('Expected RegionException for a RawWrite store error');
        } catch (RegionException $e) {
            $message = $e->getMessage();
            self::assertStringNotContainsString($rawKey, $message);
            self::assertStringContainsString('raw write failed', $message);
            self::assertStringContainsString('store rejected the region', $message);
        }
    }

    // ========================================================================
    // Multiple stores
    // ========================================================================

    public function testIngestSwitchesAllStoresToImportMode(): void
    {
        $store1 = new Store();
        $store1->setId(1);
        $store1->setAddress('tikv1:20160');

        $store2 = new Store();
        $store2->setId(2);
        $store2->setAddress('tikv2:20160');

        $region = $this->defaultRegion();

        $this->pdClient->method('getAllStores')->willReturn([$store1, $store2]);
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getStore')->willReturn($store1);

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);
        $this->grpc->method('callStreaming')->willReturn($writeResponse);

        // Ingest RPCs still use the blocking call(); return a bare response.
        $this->grpc->method('call')->willReturn(new IngestResponse());

        $switchedAddresses = [];
        $this->grpc->method('callAsync')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
            ) use (&$switchedAddresses): GrpcFuture {
                if ($method === 'SwitchMode') {
                    $switchedAddresses[] = $address;
                }

                return $this->okFuture(new SwitchModeResponse());
            });

        $this->ingestor->ingest(['key1' => 'value1']);

        // Both stores should have been switched (at least once each).
        $this->assertContains('tikv1:20160', $switchedAddresses);
        $this->assertContains('tikv2:20160', $switchedAddresses);
    }

    // ========================================================================
    // TTL support
    // ========================================================================

    public function testIngestPassesTtlToWriteBatch(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);
        $this->setupGrpcCallMock();

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);

        // Capture the streaming requests to verify TTL is set.
        $capturedRequests = null;
        $this->grpc->expects($this->once())
            ->method('callStreaming')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                iterable $requests,
            ) use (
                $writeResponse,
                &$capturedRequests
): Message {
                $capturedRequests = $requests;
                return $writeResponse;
            });

        $this->ingestor->ingest(['key1' => 'value1'], 3600);

        // The second request should be a batch with TTL set.
        $this->assertNotNull($capturedRequests);
        // The stream is lazy; materialise it here only for the assertion.
        $capturedRequests = is_array($capturedRequests)
            ? $capturedRequests
            : iterator_to_array($capturedRequests, false);
        $this->assertGreaterThanOrEqual(2, count($capturedRequests));

        $batchRequest = $capturedRequests[1];
        $batch = $batchRequest->getBatch();
        $this->assertSame(3600, $batch->getTtl());
    }

    public function testIngestWithNumericStringKeys(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);
        $this->setupGrpcCallMock();

        $writeResponse = new RawWriteResponse();
        $writeResponse->setMetas([new SSTMeta()]);

        // Capture the streaming requests to verify the numeric-string keys
        // reach the wire as strings (issue #322).
        $capturedRequests = null;
        $this->grpc->expects($this->once())
            ->method('callStreaming')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                iterable $requests,
            ) use (
                $writeResponse,
                &$capturedRequests
): Message {
                $capturedRequests = $requests;
                return $writeResponse;
            });

        // PHP models literal "12345"/"0" array keys as int; build the pairs
        // through a string-typed key so the map reaches ingest() with its
        // declared contract (numeric-string keys must survive to the wire).
        // Leading-zero ("0123") and binary ("\x00\xff") keys exercise the
        // non-coerced string-key path too (issue #192/RAW-07).
        $pairs = $this->stringKeyedPairs('12345', 'v1')
            + $this->stringKeyedPairs('0', 'v2')
            + $this->stringKeyedPairs('0123', 'v3')
            + $this->stringKeyedPairs("\x00\xff", 'v4');
        $this->ingestor->ingest($pairs);

        // requests[0] is the SST meta chunk; the data batch carries the pairs.
        $this->assertNotNull($capturedRequests);
        // The stream is lazy; materialise it here only for the assertion.
        $capturedRequests = is_array($capturedRequests)
            ? $capturedRequests
            : iterator_to_array($capturedRequests, false);
        $this->assertGreaterThanOrEqual(2, count($capturedRequests));
        $keys = array_map(
            static fn(Pair $pair): string => $pair->getKey(),
            iterator_to_array($capturedRequests[1]->getBatch()->getPairs()),
        );
        // ksort(SORT_STRING) orders the keys bytewise: "\x00\xff" < "0" < "0123" < "12345".
        $this->assertSame(["\x00\xff", '0', '0123', '12345'], $keys);
    }

    /**
     * @return array<string, string>
     */
    private function stringKeyedPairs(string $key, string $value): array
    {
        return [$key => $value];
    }

    // ========================================================================
    // Empty store address
    // ========================================================================

    public function testIngestSkipsStoresWithEmptyAddress(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('');

        $this->pdClient->method('getAllStores')->willReturn([$store]);

        // SwitchMode should not be called for empty address.
        $this->grpc->expects($this->never())->method('call');

        // No regions resolved, so no write/ingest calls either.
        $this->grpc->expects($this->never())->method('callStreaming');

        // Since #244 the grouper fails closed: the key cannot be resolved to
        // a region (PD returns nothing resolvable), so ingest() must throw
        // instead of silently doing nothing. The empty-address skip itself is
        // exercised by the SwitchMode path above (no `call` expected).
        $this->expectException(\CrazyGoat\TiKV\Client\Exception\TiKvException::class);

        $this->ingestor->ingest(['key1' => 'value1']);
    }

    // ========================================================================
    // Store address validation (PD-supplied addresses must not reach the
    // channel constructor unvalidated)
    // ========================================================================

    public function testIngestRejectsMalformedStoreAddressBeforeSwitchModeCall(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('unix:/var/run/docker.sock');

        $this->pdClient->method('getAllStores')->willReturn([$store]);

        // SwitchMode is dispatched via callAsync since issue #295.
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->never())->method('callStreaming');

        $this->expectException(InvalidStoreAddressException::class);
        $this->ingestor->ingest(['key1' => 'value1']);
    }

    public function testIngestRejectsStoreAddressWithSchemePrefixBeforeSwitchModeCall(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('dns:///tikv-0.tikv.svc.cluster.local:20160');

        $this->pdClient->method('getAllStores')->willReturn([$store]);

        // SwitchMode is dispatched via callAsync since issue #295.
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->never())->method('callStreaming');

        $this->expectException(InvalidStoreAddressException::class);
        $this->ingestor->ingest(['key1' => 'value1']);
    }

    public function testIngestRejectsStoreAddressOutsideAllowlistBeforeSwitchModeCall(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('10.1.2.3:20160');

        $this->pdClient->method('getAllStores')->willReturn([$store]);

        // SwitchMode is dispatched via callAsync since issue #295.
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->never())->method('callStreaming');

        $this->regionResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            allowedStoreHosts: ['127.0.0.1'],
        );
        $this->ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
        );

        $this->expectException(InvalidStoreAddressException::class);
        $this->ingestor->ingest(['key1' => 'value1']);
    }

    public function testIngestValidatesDefaultPolicyDerivedFromPdEndpoints(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('attacker.example.com:20160');

        $this->pdClient->method('getAllStores')->willReturn([$store]);

        // SwitchMode is dispatched via callAsync since issue #295.
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->never())->method('callStreaming');

        $this->regionResolver = new RegionResolver(
            $this->pdClient,
            $this->regionCache,
            pdEndpoints: ['pd:2379'],
        );
        $this->ingestor = new SstIngestor(
            $this->grpc,
            $this->pdClient,
            $this->regionResolver,
            new TimeoutConfig(),
            new NullLogger(),
            $this->regionCache,
        );

        $this->expectException(InvalidStoreAddressException::class);
        $this->ingestor->ingest(['key1' => 'value1']);
    }
}
