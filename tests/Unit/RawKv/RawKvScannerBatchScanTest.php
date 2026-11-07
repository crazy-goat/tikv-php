<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\KeyRange;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchScanResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #433 (GAP-19): batchScan() uses the RawBatchScan RPC, one request
 * per region, instead of a client-side loop of per-range RawScan requests.
 */
final class RawKvScannerBatchScanTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RegionResolver $regionResolver;
    private RawKvScanner $scanner;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        $this->scanner = $this->makeScanner();
    }

    private function makeScanner(
        int $retryDeadlineMs = RetryExecutor::DEFAULT_RETRY_DEADLINE_MS,
        ?TimeoutConfig $timeoutConfig = null,
        int $maxConcurrency = BatchAsyncExecutor::DEFAULT_MAX_CONCURRENCY,
    ): RawKvScanner {
        return new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            $timeoutConfig ?? new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
            retryDeadlineMs: $retryDeadlineMs,
            maxConcurrency: $maxConcurrency,
        );
    }

    /**
     * @param list<RegionInfo> $regions
     */
    private function stubRegions(array $regions): void
    {
        // A warm cache answers the whole chain; clipForward() drops the
        // regions a narrow range does not touch.
        $this->regionCache->method('getRegionsInRange')->willReturn($regions);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): ?RegionInfo => self::findRegion($regions, $key),
        );
        $this->pdClient->method('getStore')->willReturn($this->store());
    }

    /**
     * @param list<RegionInfo> $regions
     */
    private static function findRegion(array $regions, string $key): ?RegionInfo
    {
        foreach ($regions as $region) {
            if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                return $region;
            }
        }

        return null;
    }

    private function store(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        return $store;
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

    /**
     * @param list<array{0: string, 1: string}> $rows key => value
     */
    private function batchScanResponse(array $rows): RawBatchScanResponse
    {
        $pairs = [];
        foreach ($rows as [$key, $value]) {
            $pair = new KvPair();
            $pair->setKey($key);
            $pair->setValue($value);
            $pairs[] = $pair;
        }

        $response = new RawBatchScanResponse();
        $response->setKvs($pairs);

        return $response;
    }

    /**
     * @param list<string> $keys
     */
    private function scanResponse(array $keys): RawScanResponse
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

    public function testBatchScanSendsOneRawBatchScanPerRegionAndDispatchesThemTogether(): void
    {
        $region1 = $this->region(1, 'a', 'm');
        $region2 = $this->region(2, 'm', '');
        $this->stubRegions([$region1, $region2]);

        /** @var list<string> $events */
        $events = [];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$events): GrpcFuture {
                    self::assertSame('RawBatchScan', $method);
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    $events[] = 'dispatch';

                    $key = $request->getRanges()[0]->getStartKey();
                    $response = $this->batchScanResponse([
                        [$key . '1', 'v1'],
                        [$key . '2', 'v2'],
                    ]);

                    return GrpcFuture::fromWaiter(function () use ($response, &$events): Message {
                        $events[] = 'wait';

                        return $response;
                    });
                },
            );

        $result = $this->scanner->batchScan([['a', 'c'], ['n', 'o']], 10, false);

        // Both sends are issued before the first response is awaited.
        self::assertSame(['dispatch', 'dispatch', 'wait', 'wait'], $events);
        self::assertSame([['a1', 'a2'], ['n1', 'n2']], [
            array_column($result[0], 'key'),
            array_column($result[1], 'key'),
        ]);
    }

    public function testBatchScanGroupsMultipleRangesOfOneRegionIntoOneRequest(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $requests = [];
        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$requests): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    $requests[] = $request;

                    return GrpcFuture::completed($this->batchScanResponse([
                        ['a1', 'v1'],
                        ['b1', 'v2'],
                        ['d1', 'v3'],
                        ['e1', 'v4'],
                    ]));
                },
            );

        $result = $this->scanner->batchScan([['a', 'c'], ['d', 'f']], 10, false);

        self::assertCount(1, $requests);
        self::assertSame(
            [['a', 'c'], ['d', 'f']],
            array_map(
                static fn(KeyRange $range): array => [$range->getStartKey(), $range->getEndKey()],
                iterator_to_array($requests[0]->getRanges()),
            ),
        );
        self::assertSame(10, $requests[0]->getEachLimit());
        self::assertSame([['a1', 'b1'], ['d1', 'e1']], [
            array_column($result[0], 'key'),
            array_column($result[1], 'key'),
        ]);
    }

    public function testBatchScanTrimsEachRangeToEachLimitAcrossRegions(): void
    {
        $region1 = $this->region(1, 'a', 'm');
        $region2 = $this->region(2, 'm', 'z');
        $this->stubRegions([$region1, $region2]);

        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    $start = $request->getRanges()[0]->getStartKey();
                    $response = $this->batchScanResponse([
                        [$start . '1', 'v1'],
                        [$start . '2', 'v2'],
                    ]);

                    return GrpcFuture::completed($response);
                },
            );

        $result = $this->scanner->batchScan([['a', 'z']], 2, false);

        // Each region contributes up to eachLimit rows; the merged result is
        // still capped at eachLimit in ascending key order.
        self::assertSame(['a1', 'a2'], array_column($result[0], 'key'));
    }

    public function testBatchScanSplitsOneRangeAcrossRegions(): void
    {
        $region1 = $this->region(1, 'a', 'm');
        $region2 = $this->region(2, 'm', 'z');
        $this->stubRegions([$region1, $region2]);

        $ranges = [];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$ranges): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    $ranges[] = [
                        $request->getRanges()[0]->getStartKey(),
                        $request->getRanges()[0]->getEndKey(),
                    ];

                    return GrpcFuture::completed($this->batchScanResponse([]));
                },
            );

        $this->scanner->batchScan([['a', 'z']], 10, false);

        self::assertSame([['a', 'm'], ['m', 'z']], $ranges);
    }

    public function testBatchScanKeyOnlyReturnsNullValues(): void
    {
        $this->stubRegions([$this->region(1, 'a', '')]);

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    self::assertTrue($request->getKeyOnly());

                    return GrpcFuture::completed($this->batchScanResponse([
                        ['a1', 'ignored'],
                    ]));
                },
            );

        $result = $this->scanner->batchScan([['a', 'b']], 10, true);

        self::assertSame([['key' => 'a1', 'value' => null]], $result[0]);
    }

    public function testBatchScanFallsBackToRawScanOnRegionError(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);
        $this->regionCache->expects($this->atLeastOnce())->method('invalidate');

        $methods = [];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$methods): GrpcFuture {
                    $methods[] = $method;

                    if ($method === 'RawBatchScan') {
                        $error = new Error();
                        $error->setNotLeader(new NotLeader());
                        $response = new RawBatchScanResponse();
                        $response->setRegionError($error);

                        return GrpcFuture::completed($response);
                    }

                    self::assertInstanceOf(RawScanRequest::class, $request);

                    return GrpcFuture::completed($this->scanResponse(['a1', 'a2']));
                },
            );

        $result = $this->scanner->batchScan([['a', 'c']], 10, false);

        self::assertSame(['RawBatchScan', 'RawScan'], $methods);
        self::assertSame(['a1', 'a2'], array_column($result[0], 'key'));
    }

    public function testBatchScanUsesPerRangePathForOverlappingRanges(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $methods = [];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$methods): GrpcFuture {
                    $methods[] = $method;
                    self::assertSame('RawScan', $method);
                    self::assertInstanceOf(RawScanRequest::class, $request);

                    return GrpcFuture::completed($this->scanResponse(['a1']));
                },
            );

        $result = $this->scanner->batchScan([['a', 'm'], ['d', 'z']], 10, false);

        self::assertSame(['RawScan', 'RawScan'], $methods);
        self::assertCount(2, $result);
        self::assertSame(['a1'], array_column($result[0], 'key'));
        self::assertSame(['a1'], array_column($result[1], 'key'));
    }

    public function testBatchScanSendsUnboundedEndKeyForTheLastRegion(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    self::assertSame('a', $request->getRanges()[0]->getStartKey());
                    self::assertSame('', $request->getRanges()[0]->getEndKey());

                    return GrpcFuture::completed($this->batchScanResponse([]));
                },
            );

        $this->scanner->batchScan([['a', '']], 10, false);
    }

    public function testBatchScanKeepsAnUnboundedRangeLastWithinARegion(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    self::assertSame(
                        [['a', 'c'], ['x', '']],
                        array_map(
                            static fn(KeyRange $range): array => [$range->getStartKey(), $range->getEndKey()],
                            iterator_to_array($request->getRanges()),
                        ),
                    );

                    return GrpcFuture::completed($this->batchScanResponse([
                        ['a1', 'v1'],
                        ['x1', 'v2'],
                    ]));
                },
            );

        // Input order puts the unbounded range first; the client sorts it
        // last so TiKV does not read its empty end as the next range's start.
        $result = $this->scanner->batchScan([['x', ''], ['a', 'c']], 10, false);

        self::assertSame(['x1'], array_column($result[0], 'key'));
        self::assertSame(['a1'], array_column($result[1], 'key'));
    }

    public function testBatchScanAllEmptyRangesReturnsEmptyListsWithoutStoreRpc(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $this->grpc->expects($this->never())->method('callAsync');
        $this->pdClient->expects($this->never())->method('scanRegions');

        $result = $this->scanner->batchScan([['a', 'a'], ['b', 'b']], 10, false);

        self::assertSame([[], []], $result);
    }

    public function testBatchScanMixedEmptyAndNonEmptyRangesKeepsTheShape(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    self::assertCount(1, $request->getRanges(), 'empty ranges are not sent');

                    return GrpcFuture::completed($this->batchScanResponse([
                        ['a1', 'v1'],
                    ]));
                },
            );

        $result = $this->scanner->batchScan([['a', 'b'], ['c', 'c']], 10, false);

        self::assertCount(2, $result);
        self::assertSame(['a1'], array_column($result[0], 'key'));
        self::assertSame([], $result[1]);
    }

    public function testBatchScanTrimsToEachLimitWhenTheFirstRegionReturnsFewerRows(): void
    {
        $region1 = $this->region(1, 'a', 'm');
        $region2 = $this->region(2, 'm', 'z');
        $this->stubRegions([$region1, $region2]);

        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);
                    $start = $request->getRanges()[0]->getStartKey();

                    return GrpcFuture::completed($this->batchScanResponse(
                        $start === 'a'
                            ? [['a1', 'v1']]
                            : [['m1', 'v2'], ['m2', 'v3']],
                    ));
                },
            );

        $result = $this->scanner->batchScan([['a', 'z']], 3, false);

        self::assertSame(['a1', 'm1', 'm2'], array_column($result[0], 'key'));
    }

    public function testBatchScanFallsBackToRawScanOnTransportFailure(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $methods = [];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$methods): GrpcFuture {
                    $methods[] = $method;

                    if ($method === 'RawBatchScan') {
                        return GrpcFuture::fromWaiter(static function (): Message {
                            throw new GrpcException('connection refused', 14);
                        });
                    }

                    self::assertInstanceOf(RawScanRequest::class, $request);

                    return GrpcFuture::completed($this->scanResponse(['a1']));
                },
            );

        $result = $this->scanner->batchScan([['a', 'c']], 10, false);

        self::assertSame(['RawBatchScan', 'RawScan'], $methods);
        self::assertSame(['a1'], array_column($result[0], 'key'));
    }

    public function testFallbackStartsWithAFreshRetryBudgetAfterASlowRegionPath(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);
        $this->regionCache->method('invalidate');

        $methods = [];
        $this->grpc->expects($this->exactly(2))
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ) use (&$methods): GrpcFuture {
                    $methods[] = $method;

                    if ($method === 'RawBatchScan') {
                        return GrpcFuture::fromWaiter(static function (): Message {
                            // Longer than the 20 ms retry deadline: the old
                            // shared budget left the fallback dead on arrival.
                            usleep(60_000);
                            $error = new Error();
                            $error->setNotLeader(new NotLeader());
                            $response = new RawBatchScanResponse();
                            $response->setRegionError($error);

                            return $response;
                        });
                    }

                    self::assertInstanceOf(RawScanRequest::class, $request);

                    return GrpcFuture::completed($this->scanResponse(['a1']));
                },
            );

        $scanner = $this->makeScanner(retryDeadlineMs: 20);
        $result = $scanner->batchScan([['a', 'c']], 10, false);

        self::assertSame(['RawBatchScan', 'RawScan'], $methods);
        self::assertSame(['a1'], array_column($result[0], 'key'));
    }

    public function testBatchScanWrapsPdEnumerationFailureInBatchPartialFailure(): void
    {
        $this->regionCache->method('getRegionsInRange')->willReturn([]);
        $this->regionCache->method('getCachedChain')->willReturn([]);
        $this->pdClient->method('scanRegions')->willThrowException(new GrpcException('pd down', 14));

        try {
            $this->scanner->batchScan([['a', 'b'], ['c', 'd']], 10, false);
            self::fail('Expected BatchPartialFailureException');
        } catch (BatchPartialFailureException $e) {
            self::assertInstanceOf(GrpcException::class, $e->getFirstRegionError());
            self::assertSame(2, $e->getTotalRegions());
        }
    }

    public function testBatchDeadlineCoversRegionEnumerationAndDoesNotFallBack(): void
    {
        $this->regionCache->method('getRegionsInRange')->willReturn([]);
        $this->regionCache->method('getCachedChain')->willReturn([]);
        $this->pdClient->method('scanRegions')->willReturnCallback(
            function (): array {
                usleep(80_000);

                return [$this->region(1, '', '')];
            },
        );
        $this->pdClient->method('getStore')->willReturn($this->store());
        $this->grpc->expects($this->never())->method('callAsync');

        $scanner = $this->makeScanner(timeoutConfig: $this->tightTimeoutConfig(50));

        $this->expectException(BatchDeadlineExceededException::class);
        $scanner->batchScan([['a', 'b'], ['c', 'd']], 10, false);
    }

    public function testBatchDeadlineExceededIsRethrownInsteadOfFallingBack(): void
    {
        $region1 = $this->region(1, 'a', 'm');
        $region2 = $this->region(2, 'm', '');
        $this->stubRegions([$region1, $region2]);

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                function (
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture {
                    self::assertSame('RawBatchScan', $method);
                    self::assertInstanceOf(RawBatchScanRequest::class, $request);

                    return GrpcFuture::fromWaiter(static function (): Message {
                        usleep(80_000);

                        return new RawBatchScanResponse();
                    });
                },
            );

        $scanner = $this->makeScanner(
            timeoutConfig: $this->tightTimeoutConfig(50),
            maxConcurrency: 1,
        );

        $this->expectException(BatchDeadlineExceededException::class);
        $scanner->batchScan([['a', '']], 10, false);
    }

    public function testBatchScanUnexpectedResponseTypeIsNotSwallowedByTheFallback(): void
    {
        $region = $this->region(1, 'a', '');
        $this->stubRegions([$region]);

        $this->grpc->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(
                static fn(
                    string $address,
                    string $service,
                    string $method,
                    Message $request,
                ): GrpcFuture => GrpcFuture::completed(new RawScanResponse()),
            );

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('RawBatchScan returned an unexpected response type');

        $this->scanner->batchScan([['a', 'c']], 10, false);
    }

    /**
     * A timeout config whose batch fan-out timeouts are all at or below the
     * batch deadline, as the TimeoutConfig constructor requires.
     */
    private function tightTimeoutConfig(int $batchDeadlineMs): TimeoutConfig
    {
        return new TimeoutConfig(
            writeTimeoutMs: $batchDeadlineMs,
            batchReadTimeoutMs: $batchDeadlineMs,
            batchWriteTimeoutMs: $batchDeadlineMs,
            scanTimeoutMs: $batchDeadlineMs,
            deleteRangeTimeoutMs: $batchDeadlineMs,
            checksumTimeoutMs: $batchDeadlineMs,
            ingestTimeoutMs: $batchDeadlineMs,
            batchDeadlineMs: $batchDeadlineMs,
        );
    }
}
