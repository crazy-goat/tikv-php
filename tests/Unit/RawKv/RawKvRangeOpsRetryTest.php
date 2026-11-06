<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\RawChecksumResponse;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvRangeOps;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Pins the fixed contract of issue #566: a region error carried in the
 * RESPONSE of a RawDeleteRange/RawChecksum call is retried per clipped
 * sub-range, not surfaced as a hard BatchPartialFailureException.
 *
 * The pre-fix code wrapped only the dispatch (region resolution + gRPC send)
 * in RetryExecutor and returned an un-awaited future, so the response — and
 * every retryable region error in it — was inspected later by
 * BatchAsyncExecutor, outside the retry loop. The wrappers now use
 * CheckedGrpcFuture::fromRetryableDispatch(): the first attempt is still
 * dispatched eagerly (fan-out is preserved) while the wait phase runs the
 * whole dispatch+await cycle under the retry executor.
 *
 * These are pure-PHP Unit tests: the transport is a GrpcFuture::fromWaiter()
 * fake, so no ext-grpc and no mocked \Grpc\Call are needed.
 */
final class RawKvRangeOpsRetryTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);
    }

    private function region(
        string $startKey = '',
        string $endKey = '',
        int $regionId = 1,
        int $leaderStoreId = 1,
    ): RegionInfo {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: 1,
            leaderStoreId: $leaderStoreId,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function store(int $storeId = 1, string $address = 'tikv1:20160'): Store
    {
        $store = new Store();
        $store->setId($storeId);
        $store->setAddress($address);

        return $store;
    }

    /**
     * Stub a single-region layout. The cache always answers, so every retry
     * re-resolves the same region without a PD round trip.
     */
    private function stubSingleRegion(RegionInfo $region): void
    {
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->store());
    }

    private function rangeOps(int $maxBackoffMs = 20000): RawKvRangeOps
    {
        return new RawKvRangeOps(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            new TimeoutConfig(),
            maxBackoffMs: $maxBackoffMs,
            serverBusyBudgetMs: 600000,
            logger: new NullLogger(),
        );
    }

    private function future(Message $response): GrpcFuture
    {
        return GrpcFuture::fromWaiter(static fn(): Message => $response);
    }

    private function notLeaderError(int $regionId = 1): Error
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId($regionId);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        return $error;
    }

    private function epochNotMatchError(): Error
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        return $error;
    }

    private function keyNotInRegionError(string $key = 'a'): Error
    {
        $keyNotInRegion = new KeyNotInRegion();
        $keyNotInRegion->setRegionId(1);
        $keyNotInRegion->setKey($key);
        $keyNotInRegion->setStartKey('');
        $keyNotInRegion->setEndKey('');

        $error = new Error();
        $error->setMessage('key not in region');
        $error->setKeyNotInRegion($keyNotInRegion);

        return $error;
    }

    private function deleteRangeResponse(?Error $regionError = null): RawDeleteRangeResponse
    {
        $response = new RawDeleteRangeResponse();
        if ($regionError instanceof Error) {
            $response->setRegionError($regionError);
        }

        return $response;
    }

    private function checksumResponse(
        ?Error $regionError = null,
        int $checksum = 0,
        int $totalKvs = 0,
        int $totalBytes = 0,
    ): RawChecksumResponse {
        $response = new RawChecksumResponse();
        if ($regionError instanceof Error) {
            $response->setRegionError($regionError);
        }
        $response->setChecksum($checksum);
        $response->setTotalKvs($totalKvs);
        $response->setTotalBytes($totalBytes);

        return $response;
    }

    /**
     * A NotLeader response is retried in the wait phase and the second
     * attempt succeeds; the executor invalidates the region (no leader hint
     * was supplied) before re-resolving it.
     */
    public function testDeleteRangeRetriesResponseBorneNotLeaderAndSucceeds(): void
    {
        $this->stubSingleRegion($this->region());
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(1, 'not_leader');

        $responses = [
            $this->deleteRangeResponse($this->notLeaderError()),
            $this->deleteRangeResponse(),
        ];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(function () use (&$responses): GrpcFuture {
                return $this->future(array_shift($responses) ?? new RawDeleteRangeResponse());
            });

        $this->rangeOps()->deleteRange('a', 'z');
    }

    /**
     * An EpochNotMatch response is retried in the wait phase and the merged
     * checksum is the successful attempt's result, not a partial or zero one.
     */
    public function testChecksumRetriesResponseBorneEpochNotMatchAndSucceeds(): void
    {
        $this->stubSingleRegion($this->region());
        $this->regionCache->expects($this->once())
            ->method('invalidate')
            ->with(1, 'retry_region_error:epoch_not_match');

        $responses = [
            $this->checksumResponse($this->epochNotMatchError()),
            $this->checksumResponse(checksum: 12345, totalKvs: 3, totalBytes: 100),
        ];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(function () use (&$responses): GrpcFuture {
                return $this->future(array_shift($responses) ?? new RawChecksumResponse());
            });

        $result = $this->rangeOps()->checksum('a', 'z');

        $this->assertSame(12345, $result->checksum);
        $this->assertSame(3, $result->totalKvs);
        $this->assertSame(100, $result->totalBytes);
    }

    /**
     * A response that keeps carrying a retryable region error must exhaust
     * the operation-scoped retry budget and then surface as a partial
     * failure — not loop forever and not report success.
     */
    public function testResponseBorneRetryBudgetExhaustionIsReportedAsPartialFailure(): void
    {
        $this->stubSingleRegion($this->region());

        $calls = 0;
        $this->grpc->method('callAsync')
            ->willReturnCallback(function () use (&$calls): GrpcFuture {
                $calls++;

                return $this->future($this->deleteRangeResponse($this->epochNotMatchError()));
            });

        try {
            // EpochNotMatch sleeps 1-2 ms, then 2-4 ms, ... so a 5 ms
            // cumulative budget exhausts after a handful of attempts.
            $this->rangeOps(maxBackoffMs: 5)->deleteRange('a', 'z');
            self::fail('Expected the exhausted retry budget to surface');
        } catch (BatchPartialFailureException $e) {
            $this->assertInstanceOf(RegionException::class, $e->getFirstRegionError());
            $this->assertGreaterThanOrEqual(2, $calls, 'A retryable response error must be retried at least once');
            $this->assertLessThan(30, $calls, 'The retry budget must bound the number of attempts');
        }
    }

    /**
     * A fatal region error must not be retried: the retry executor throws it
     * after exactly one attempt and the fan-out reports it as a partial
     * failure.
     */
    private function assertFatalRegionErrorIsNotRetried(callable $operation): void
    {
        $this->stubSingleRegion($this->region());

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    $response = $request instanceof RawDeleteRangeRequest
                        ? $this->deleteRangeResponse($this->keyNotInRegionError())
                        : $this->checksumResponse($this->keyNotInRegionError());

                    return $this->future($response);
                },
            );

        try {
            $operation($this->rangeOps());
            self::fail('Expected the fatal region error to surface');
        } catch (BatchPartialFailureException $e) {
            $this->assertCount(1, $e->getRegionErrors());
            $first = $e->getFirstRegionError();
            $this->assertInstanceOf(RegionException::class, $first);
            $this->assertSame(ErrorKind::KeyNotInRegion, $first->errorKind);
        }
    }

    public function testDeleteRangeDoesNotRetryFatalRegionError(): void
    {
        $this->assertFatalRegionErrorIsNotRetried(
            static function (RawKvRangeOps $ops): void {
                $ops->deleteRange('a', 'z');
            },
        );
    }

    public function testChecksumDoesNotRetryFatalRegionError(): void
    {
        $this->assertFatalRegionErrorIsNotRetried(
            static function (RawKvRangeOps $ops): void {
                $ops->checksum('a', 'z');
            },
        );
    }

    /**
     * A partial fan-out failure keeps the successful sub-range's result and
     * reports the failing one; the fatal region error is not retried, so the
     * two regions cost exactly two sends.
     */
    public function testPartialFanOutFailureReportsFailedAndSuccessfulRegions(): void
    {
        $region1 = $this->region('', 'm', regionId: 1);
        $region2 = $this->region('m', '', regionId: 2);

        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region1 : $region2,
        );
        $this->regionCache->method('put');
        $this->pdClient->method('scanRegions')->willReturn([$region1, $region2]);
        $this->pdClient->method('getRegion')->willReturnCallback(
            static fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $region1 : $region2,
        );
        $this->pdClient->method('getStore')->willReturn($this->store());

        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    $this->assertInstanceOf(RawDeleteRangeRequest::class, $request);
                    $startKey = $request->getStartKey();

                    return KeyOrder::lt($startKey, 'm')
                        ? $this->future($this->deleteRangeResponse())
                        : $this->future($this->deleteRangeResponse($this->keyNotInRegionError($startKey)));
                },
            );

        try {
            $this->rangeOps()->deleteRange('a', 'z');
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            $this->assertSame(2, $e->getTotalRegions());
            $this->assertSame([1], array_keys($e->getRegionErrors()));
            $this->assertSame([0], array_keys($e->getPartialResults()));
        }
    }

    /**
     * A response with no region error is returned unchanged: one send, no
     * retry, and the checksum result is merged as before.
     */
    public function testSuccessfulResponseIsUnchanged(): void
    {
        $this->stubSingleRegion($this->region());
        $this->regionCache->expects($this->never())->method('invalidate');

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(fn(): GrpcFuture => $this->future(
                $this->checksumResponse(checksum: 42, totalKvs: 1, totalBytes: 7),
            ));

        $result = $this->rangeOps()->checksum('a', 'z');

        $this->assertSame(42, $result->checksum);
        $this->assertSame(1, $result->totalKvs);
        $this->assertSame(7, $result->totalBytes);
    }
}
