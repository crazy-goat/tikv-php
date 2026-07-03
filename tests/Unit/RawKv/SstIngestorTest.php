<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\ImportSstpb\IngestResponse;
use CrazyGoat\Proto\ImportSstpb\RawWriteResponse;
use CrazyGoat\Proto\ImportSstpb\SSTMeta;
use CrazyGoat\Proto\ImportSstpb\SwitchModeResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\SstIngestor;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class SstIngestorTest extends TestCase
{
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
        );
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
     * Create a gRPC call mock that returns SwitchModeResponse for SwitchMode
     * calls and IngestResponse for Ingest calls.
     */
    private function setupGrpcCallMock(?IngestResponse $ingestResponse = null): void
    {
        $ingestResponse ??= new IngestResponse();

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
    // Switch mode error handling
    // ========================================================================

    public function testIngestSwitchesBackToNormalModeOnWriteError(): void
    {
        $store = $this->defaultStore();
        $region = $this->defaultRegion();

        $this->setupStandardPdMocks($region, $store);

        $switchModeCalls = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (string $address) use (&$switchModeCalls): Message {
                $switchModeCalls[] = $address;
                return new SwitchModeResponse();
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
            $this->assertGreaterThanOrEqual(2, count($switchModeCalls));
            $this->assertContains('tikv1:20160', $switchModeCalls);
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

        $switchedAddresses = [];
        $this->grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
            ) use (&$switchedAddresses): Message {
                if ($method === 'SwitchMode') {
                    $switchedAddresses[] = $address;
                }

                return $method === 'SwitchMode'
                    ? new SwitchModeResponse()
                    : new IngestResponse();
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
                array $requests,
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
        $this->assertGreaterThanOrEqual(2, count($capturedRequests));

        $batchRequest = $capturedRequests[1];
        $batch = $batchRequest->getBatch();
        $this->assertSame(3600, $batch->getTtl());
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

        $this->ingestor->ingest(['key1' => 'value1']);
    }
}
