<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #555: an unbounded reverse scan fans its per-region RawScan sends
 * out concurrently, like the forward unbounded scan (issue #293).
 *
 * The tests pin the five properties the change must keep: descending key
 * order across several regions, more than one send in flight at a time, the
 * bounded path still sequential, a region error falling back to the
 * retrying sequential path, and an empty range issuing no store RPC. The
 * `reverseScan('', '')` boundary is pinned separately: master's #343 guard
 * treats it as the empty range (no RPC), so the fan-out must not change it.
 */
final class RawKvScannerReverseFanOutTest extends TestCase
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

    private function region(string $startKey, string $endKey, int $regionId): RegionInfo
    {
        return new RegionInfo(
            regionId: $regionId,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        return $store;
    }

    /**
     * A warm region cache that already holds the complete chain, so the scan
     * never asks PD for regions.
     *
     * @param list<RegionInfo> $regions ascending by start key
     */
    private function stubWarmChain(array $regions): void
    {
        $this->regionCache->method('getRegionsInRange')->willReturn($regions);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($regions): ?RegionInfo {
                foreach ($regions as $region) {
                    if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                        return $region;
                    }
                }

                return null;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());
    }

    /**
     * An un-waited future over a resolved response; records a 'wait' event
     * when the fan-out awaits it. Needs no ext-grpc (GrpcFuture::fromWaiter).
     *
     * @param list<string>|null $events
     */
    private function okFuture(RawScanResponse $response, ?array &$events = null): GrpcFuture
    {
        return GrpcFuture::fromWaiter(
            static function () use ($response, &$events): Message {
                if (is_array($events)) {
                    $events[] = 'wait';
                }

                return $response;
            },
        );
    }

    private function scanResponse(string ...$keys): RawScanResponse
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

    private function scanner(
        int $maxConcurrency = 16,
        int $scanPageSize = RawKvScanner::MAX_SCAN_LIMIT,
    ): RawKvScanner {
        return new RawKvScanner(
            $this->pdClient,
            $this->grpc,
            $this->regionResolver,
            new TimeoutConfig(),
            maxBackoffMs: 20000,
            serverBusyBudgetMs: 600000,
            regionCache: $this->regionCache,
            logger: new NullLogger(),
            maxConcurrency: $maxConcurrency,
            scanPageSize: $scanPageSize,
        );
    }

    /**
     * @return list<RegionInfo>
     */
    private function threeRegions(): array
    {
        return [
            $this->region('a', 'm', 1),
            $this->region('m', 't', 2),
            $this->region('t', 'z', 3),
        ];
    }

    public function testUnboundedReverseScanFansOutAcrossRegionsInDescendingOrder(): void
    {
        $this->stubWarmChain($this->threeRegions());

        /** @var list<string> $events */
        $events = [];
        $this->grpc->expects($this->exactly(3))->method('callAsync')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$events): GrpcFuture {
                /** @var RawScanRequest $request */
                $this->assertTrue($request->getReverse(), 'the fan-out must send reverse RawScan requests');
                $events[] = 'dispatch';

                return $this->okFuture(
                    match ($request->getStartKey()) {
                        'z' => $this->scanResponse('y', 'x'),
                        't' => $this->scanResponse('s', 'p'),
                        'm' => $this->scanResponse('k', 'b'),
                        default => new RawScanResponse(),
                    },
                    $events,
                );
            },
        );
        $this->grpc->expects($this->never())->method('call');

        $result = $this->scanner()->reverseScan('z', 'a', 0, false);

        // Segments are [z, t), [t, m), [m, a): their descending rows are
        // concatenated in that order.
        $this->assertSame(['y', 'x', 's', 'p', 'k', 'b'], array_column($result, 'key'));

        // Every send is dispatched before the first wait: three RPCs are in
        // flight at the same time, which is the whole point of the fan-out.
        $this->assertSame(
            ['dispatch', 'dispatch', 'dispatch', 'wait', 'wait', 'wait'],
            $events,
        );
    }

    /**
     * The fan-out is applied to every page of the unbounded loop, not just
     * the first one: a two-row page size turns the six-key range into four
     * pages, and each page dispatches its own window.
     */
    public function testUnboundedReverseScanFansOutOnEveryPaginatedPage(): void
    {
        $this->stubWarmChain($this->threeRegions());

        // Descending byte order, so filtering by range preserves it.
        $allKeys = ['y', 'x', 's', 'p', 'k', 'b'];
        $responder = function (RawScanRequest $request) use ($allKeys): RawScanResponse {
            $start = $request->getStartKey();
            $end = $request->getEndKey();

            $keys = [];
            foreach ($allKeys as $key) {
                if (KeyOrder::lt($key, $start) && ($end === '' || KeyOrder::gte($key, $end))) {
                    $keys[] = $key;
                }
            }

            return $this->scanResponse(...array_slice($keys, 0, $request->getLimit()));
        };

        /** @var list<string> $dispatchedStarts */
        $dispatchedStarts = [];
        $this->grpc->expects($this->atLeast(6))->method('callAsync')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (
                &$dispatchedStarts,
                $responder,
            ): GrpcFuture {
                /** @var RawScanRequest $request */
                $this->assertTrue($request->getReverse());
                $dispatchedStarts[] = $request->getStartKey();

                return $this->okFuture($responder($request));
            },
        );

        // A page that ends up with a single clipped segment stays on the
        // sequential path (the last page here), so answer that too.
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use ($responder): Message {
                /** @var RawScanRequest $request */
                $this->assertTrue($request->getReverse());

                return $responder($request);
            },
        );

        $result = $this->scanner(scanPageSize: 2)->reverseScan('z', 'a', 0, false);

        $this->assertSame(['y', 'x', 's', 'p', 'k', 'b'], array_column($result, 'key'));
        // Page 1 dispatched [z, t), [t, m) and [m, a) together.
        $this->assertSame(['z', 't', 'm'], array_slice($dispatchedStarts, 0, 3));
    }

    public function testBoundedReverseScanStaysSequential(): void
    {
        $this->stubWarmChain($this->threeRegions());

        $this->grpc->expects($this->never())->method('callAsync');
        $this->grpc->expects($this->exactly(3))->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ): Message {
                /** @var RawScanRequest $request */
                $this->assertTrue($request->getReverse());

                return match ($request->getStartKey()) {
                    'z' => $this->scanResponse('y', 'x'),
                    't' => $this->scanResponse('s', 'p'),
                    'm' => $this->scanResponse('k', 'b'),
                    default => new RawScanResponse(),
                };
            },
        );

        $result = $this->scanner()->reverseScan('z', 'a', 100, false);

        $this->assertSame(['y', 'x', 's', 'p', 'k', 'b'], array_column($result, 'key'));
    }

    public function testUnboundedReverseScanFallsBackToSequentialOnRegionError(): void
    {
        $this->stubWarmChain([
            $this->region('a', 'm', 1),
            $this->region('m', 'z', 2),
        ]);

        $error = new Error();
        $error->setMessage('not leader');
        $regionError = new RawScanResponse();
        $regionError->setRegionError($error);

        $this->grpc->method('callAsync')->willReturnCallback(
            fn(): GrpcFuture => $this->okFuture($regionError),
        );

        // The fan-out never uses the synchronous call path; the sequential
        // fallback does, so assert it was exercised (not vacuous).
        $this->grpc->expects($this->exactly(2))->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ): Message {
                /** @var RawScanRequest $request */

                return match ($request->getStartKey()) {
                    'z' => $this->scanResponse('s', 'p'),
                    'm' => $this->scanResponse('k', 'b'),
                    default => new RawScanResponse(),
                };
            },
        );

        $result = $this->scanner()->reverseScan('z', 'a', 0, false);

        // Both segments were re-run through the retrying sequential path, in
        // descending order, instead of returning the region error's rows.
        $this->assertSame(['s', 'p', 'k', 'b'], array_column($result, 'key'));
    }

    /**
     * A region that split after the chain was enumerated leaves a hole above
     * the dispatched segment's clipped range. The concurrent result must be
     * discarded and the sequential reverse path re-run, because only
     * executeReverseScanForSubRange() can splice the upper part back in
     * descending order.
     */
    public function testUnboundedReverseScanFallsBackWhenARegionShrankAfterEnumeration(): void
    {
        $stale = [
            $this->region('a', 'm', 1),
            $this->region('m', 'z', 2),
        ];
        $fresh = [
            $this->region('a', 'm', 1),
            $this->region('m', 'q', 2),
            $this->region('q', 'z', 3),
        ];

        $this->regionCache->method('getRegionsInRange')->willReturn($stale);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static function (string $key) use ($fresh): ?RegionInfo {
                foreach ($fresh as $region) {
                    if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                        return $region;
                    }
                }

                return null;
            },
        );
        $this->regionCache->method('put');
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        /** @var list<string> $asyncStarts */
        $asyncStarts = [];
        $this->grpc->method('callAsync')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$asyncStarts): GrpcFuture {
                /** @var RawScanRequest $request */
                $asyncStarts[] = $request->getStartKey();

                return $this->okFuture(match ($request->getStartKey()) {
                    'q' => $this->scanResponse('p'),
                    'm' => $this->scanResponse('k'),
                    default => new RawScanResponse(),
                });
            },
        );

        /** @var list<string> $syncStarts */
        $syncStarts = [];
        $this->grpc->expects($this->exactly(3))->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                Message $request,
            ) use (&$syncStarts): Message {
                /** @var RawScanRequest $request */
                $syncStarts[] = $request->getStartKey();

                return match ($request->getStartKey()) {
                    'q' => $this->scanResponse('p'),
                    'z' => $this->scanResponse('x'),
                    'm' => $this->scanResponse('k'),
                    default => new RawScanResponse(),
                };
            },
        );

        $result = $this->scanner()->reverseScan('z', 'a', 0, false);

        // x lives in [q, z) and was only reachable through the sequential
        // split continuation; the stale fan-out for [z, m) stopped at q.
        $this->assertSame(['x', 'p', 'k'], array_column($result, 'key'));
        $this->assertSame(['q', 'm'], $asyncStarts);
        $this->assertSame(['q', 'z', 'm'], $syncStarts);
    }

    public function testReverseScanEmptyRangeSendsNoStoreRpc(): void
    {
        // reverseScan($upper, $lower) with equal bounds is the valid empty
        // range [a, a): the clipper yields no segment and no store RPC runs.
        $this->regionCache->method('getRegionsInRange')->willReturn([]);
        $this->pdClient->method('scanRegions')->willReturn([]);
        $this->pdClient->expects($this->never())->method('getStore');
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');

        $result = $this->scanner()->reverseScan('a', 'a', 0, false);

        $this->assertSame([], $result);
    }

    /**
     * `reverseScan('', '')` is the empty range: an empty end key is the
     * start of the keyspace, not +infinity, so the scan returns before
     * enumeration and sends no RPC (issue #343). The fan-out must not change
     * that.
     */
    public function testReverseScanOfBothEmptyBoundsIsEmptyWithoutRpc(): void
    {
        $this->regionCache->expects($this->never())->method('getRegionsInRange');
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->expects($this->never())->method('getStore');
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');

        $this->assertSame([], $this->scanner()->reverseScan('', '', 0, false));
    }
}
