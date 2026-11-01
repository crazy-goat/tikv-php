<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawBatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Tests\Unit\Support\ClosingPeerGrpcClient;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #283: two {@see GrpcClient} instances against the same address.
 *
 * ext-grpc keeps a process-global table of persistent channels, so two
 * clients with the same address and configuration share one underlying
 * channel. Closing one client must not invalidate the channel the other
 * client holds.
 *
 * Verified against the deployed extension (grpc 1.83.0): the persistent key
 * is `target + sha1(channel arguments) + sha1(credentials)`, and
 * `Channel::close()` is a reference-count decrement, not a destroy — the
 * underlying channel survives until its last holder releases it. These tests
 * pin that behaviour: they fail if a future extension turns `close()` into a
 * destroy, because the second client would then use a released channel —
 * most likely a use-after-free (a crash), and at best a closed-channel
 * error.
 *
 * The address is a port nothing listens on, so every RPC fails with a
 * transport error — the distinction the assertions rely on: a transport
 * error means the channel is alive, a closed-channel error means it is not.
 */
final class GrpcClientChannelIsolationTest extends TestCase
{
    use GrpcExtensionGate;

    private const DEAD_ADDRESS = '127.0.0.1:1';

    private const OTHER_DEAD_ADDRESS = '127.0.0.1:2';

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testClosingOneClientLeavesTheOtherAbleToIssueCalls(): void
    {
        $first = new GrpcClient();
        $second = new GrpcClient();

        try {
            // Both clients open a channel to the same address; ext-grpc
            // hands them the same underlying channel.
            $first->getChannel(self::DEAD_ADDRESS);
            $second->getChannel(self::DEAD_ADDRESS);

            $first->close();

            try {
                $second->call(
                    self::DEAD_ADDRESS,
                    'tikvpb.Tikv',
                    'RawGet',
                    $this->rawGetRequest(),
                    RawGetResponse::class,
                    timeoutMs: 500,
                );
                $this->fail('Expected a transport error against the dead endpoint');
            } catch (GrpcException $e) {
                // Expected: the channel is alive, the endpoint is not. A
                // destroyed channel would fail with a closed-channel error
                // instead ("Channel is already closed" / "Call cannot be
                // constructed from a closed Channel").
                $this->assertSame(GrpcStatusCode::Unavailable->value, $e->grpcStatusCode);
            }
        } finally {
            $first->close();
            $second->close();
        }
    }

    public function testBatchCallStartedBeforeAnotherClientClosedStillSettles(): void
    {
        $first = new GrpcClient();
        $second = new GrpcClient();

        try {
            // The peer holds a handle on the same address, so its close()
            // really releases a reference to the shared underlying channel.
            // Without this the test would pass even if close() destroyed the
            // shared channel: there would be nothing to destroy.
            $first->getChannel(self::DEAD_ADDRESS);
            $second->getChannel(self::DEAD_ADDRESS);
            $this->assertSame(1, $first->getChannelCount());
            $this->assertSame(1, $second->getChannelCount());

            // The batch fan-out (RawKvBatch) sends through callAsync() and
            // an un-awaited GrpcFuture.
            $future = $second->callAsync(
                self::DEAD_ADDRESS,
                'tikvpb.Tikv',
                'RawBatchGet',
                $this->rawBatchGetRequest(),
                RawBatchGetResponse::class,
                timeoutMs: 500,
            );

            // The other client releases its handle while the call is in
            // flight.
            $first->close();

            // The in-flight call must still settle — with the transport
            // error of the dead endpoint, never with a closed-channel error.
            $this->expectException(GrpcException::class);
            $future->wait();
        } finally {
            $first->close();
            $second->close();
        }
    }

    public function testBatchFanOutStartedBeforeAnotherClientClosedStillCompletes(): void
    {
        $first = new GrpcClient();
        $second = new GrpcClient();

        try {
            // The first client holds a channel to both addresses, so closing
            // it releases every handle it owns.
            $first->getChannel(self::DEAD_ADDRESS);
            $first->getChannel(self::OTHER_DEAD_ADDRESS);
            $this->assertSame(2, $first->getChannelCount());

            // The peer is closed while the first sub-batch is in flight: the
            // fan-out asks for its second channel right after it sent the
            // first one.
            $grpc = new ClosingPeerGrpcClient($second, $first);

            $batch = $this->createBatch($grpc);

            try {
                $batch->batchGet(['k1', 'k2'], $this->createRetryExecutor($grpc));
                $this->fail('Expected BatchPartialFailureException from the dead-endpoint channels');
            } catch (BatchPartialFailureException $e) {
                // Both sub-batches settled with a transport failure, so the
                // fan-out completed its dispatch and its waits. A channel
                // invalidated by the peer client's close() would have failed
                // the dispatch instead, and that error is not a
                // TiKvException — it would escape the batch as-is.
                $this->assertCount(2, $e->getRegionErrors());
            }
        } finally {
            $first->close();
            $second->close();
        }
    }

    private function createBatch(GrpcClientInterface $grpc): RawKvBatch
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);

        $first = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: 'k2',
        );
        $second = new RegionInfo(
            regionId: 2,
            leaderPeerId: 2,
            leaderStoreId: 2,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: 'k2',
            endKey: '',
        );

        $regionCache->method('getByKey')->willReturnCallback(
            static fn (string $key): RegionInfo => $key === 'k1' ? $first : $second,
        );
        $pdClient->method('getStore')->willReturnCallback(
            static function (int $storeId): Store {
                $store = new Store();
                $store->setId($storeId);
                $store->setAddress($storeId === 1 ? self::DEAD_ADDRESS : self::OTHER_DEAD_ADDRESS);

                return $store;
            },
        );

        return new RawKvBatch(
            $grpc,
            new RegionResolver($pdClient, $regionCache),
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    private function createRetryExecutor(GrpcClientInterface $grpc): RetryExecutor
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);

        return new RetryExecutor(
            100,
            1000,
            $regionCache,
            $grpc,
            new RegionResolver($pdClient, $regionCache),
            new NullLogger(),
            // One attempt: the wait-phase re-dispatch (#183) would only add
            // dispatches that this test does not care about.
            maxAttempts: 1,
        );
    }

    private function rawGetRequest(): RawGetRequest
    {
        $request = new RawGetRequest();
        $request->setKey('k1');

        return $request;
    }

    private function rawBatchGetRequest(): RawBatchGetRequest
    {
        $request = new RawBatchGetRequest();
        $request->setKeys(['k1']);

        return $request;
    }
}
