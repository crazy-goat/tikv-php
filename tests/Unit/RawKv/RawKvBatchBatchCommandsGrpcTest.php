<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsMultiplexer;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Tests\Unit\Batch\FakeBatchCommandsTransport;
use CrazyGoat\TiKV\Tests\Unit\Grpc\GrpcExtensionGate;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * ext-grpc-dependent half of the RawKvBatch BatchCommands integration
 * (issue #418): the flag-off identity and the unary-fallback path both
 * exercise the real unary dispatch (Grpc\Call), so this class lives in the
 * Grpc testsuite (same rule as RawKvBatchTest — see phpunit.xml, PR #484).
 * The pure fakes-only tests of the multiplexed path stay in the Unit suite
 * (RawKvBatchBatchCommandsTest).
 */
final class RawKvBatchBatchCommandsGrpcTest extends TestCase
{
    use GrpcExtensionGate;

    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function stubCluster(): void
    {
        // Region 1: ['', 'm'), region 2: ['m', '') — both led by store 1.
        $regions = [
            1 => $this->region(1, '', 'm'),
            2 => $this->region(2, 'm', ''),
        ];
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn (string $key): RegionInfo => $key < 'm' ? $regions[1] : $regions[2],
        );
        $this->pdClient->method('scanRegions')->willReturn(array_values($regions));
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
    }

    private function createRawKvBatch(?BatchCommandsMultiplexer $multiplexer): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
            batchCommands: $multiplexer,
        );
    }

    private function createRetryExecutor(int $maxAttempts = 1): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
            $maxAttempts,
        );
    }

    public function testBatchGetWithoutMultiplexerUsesUnaryPathOnly(): void
    {
        // Flag default (off): the transport seam must not be consulted at
        // all. The unary path needs ext-grpc (Grpc\Call), so gate like the
        // existing RawKvBatchTest dispatch-count tests.
        $this->requireGrpcExtension();
        $this->stubCluster();
        $this->grpc->method('getChannel')->willReturn(new \Grpc\Channel('127.0.0.1:1', [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]));

        $batch = $this->createRawKvBatch(null);

        $this->expectException(BatchPartialFailureException::class);
        $batch->batchGet(['a', 'z'], $this->createRetryExecutor());
    }

    public function testBatchGetFallsBackToUnaryPathOnStreamFailure(): void
    {
        // A stream-layer failure must transparently re-dispatch every entry
        // on the existing unary fan-out: against a dead endpoint that
        // surfaces as the usual BatchPartialFailureException from the unary
        // path (proving the fallback ran) — and getChannel WAS called.
        $this->requireGrpcExtension();
        $this->stubCluster();
        $this->grpc->expects($this->atLeastOnce())->method('getChannel')->willReturnCallback(
            fn (): \Grpc\Channel => new \Grpc\Channel('127.0.0.1:1', [
                'credentials' => \Grpc\ChannelCredentials::createInsecure(),
            ]),
        );

        $fake = new FakeBatchCommandsTransport(
            throwOnCall: new BatchCommandsStreamException('stream closed'),
        );
        $batch = $this->createRawKvBatch(new BatchCommandsMultiplexer($fake));

        $this->expectException(BatchPartialFailureException::class);
        $batch->batchGet(['a', 'z'], $this->createRetryExecutor());
    }
}
