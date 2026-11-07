<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use CrazyGoat\TiKV\Client\Util\ShutdownState;
use PHPUnit\Framework\TestCase;

/**
 * Transactional reverse scan (issue #435, GAP-21).
 *
 * `Transaction::reverseScan($upper, $lower)` reads `[lower, upper)` in
 * descending key order at the transaction's snapshot. The regions are walked
 * from the upper bound downwards, so a narrow reverse scan never enumerates
 * the rest of the range; the write-set merge still applies read-your-writes.
 */
final class TransactionReverseScanTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    private PdClientInterface&\PHPUnit\Framework\MockObject\MockObject $pdClient;
    private GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject $grpc;
    private RegionCacheInterface&\PHPUnit\Framework\MockObject\MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($this->grpc);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);
    }

    protected function tearDown(): void
    {
        ShutdownState::resetForTests();
    }

    private function createTransaction(int $maxScanLimit = 10240): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-reverse-scan',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
            maxScanLimit: $maxScanLimit,
        );
    }

    private function makeRegion(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    /**
     * Wire the region cache so the resolver's top-down walk answers from the
     * given regions without a PD call.
     *
     * @param list<RegionInfo> $regions ascending by start key
     */
    private function stubRegions(array $regions): void
    {
        $this->regionCache->method('getRegionBelow')->willReturnCallback(
            static function (string $endKey) use ($regions): ?RegionInfo {
                $best = null;
                foreach ($regions as $region) {
                    if (KeyOrder::gte($region->startKey, $endKey)) {
                        continue;
                    }
                    if ($best === null || KeyOrder::gte($region->startKey, $best->startKey)) {
                        $best = $region;
                    }
                }

                return $best;
            },
        );
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): ?RegionInfo => self::findRegionForKey($regions, $key),
        );
        $this->regionCache->method('put');
    }

    /**
     * @param list<RegionInfo> $regions
     */
    private static function findRegionForKey(array $regions, string $key): ?RegionInfo
    {
        foreach ($regions as $region) {
            if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                return $region;
            }
        }

        return null;
    }

    /**
     * Emulate TiKV's reverse `KvScan`: return the stored keys inside
     * `[end_key, start_key)` in descending order, honouring the row limit.
     *
     * @param list<string>      $stored
     * @param list<ScanRequest> $capturedRequests
     */
    private function stubReverseScan(array $stored, array &$capturedRequests): void
    {
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
            ) use (
                $stored,
                &$capturedRequests,
            ): ScanResponse {
                $this->assertSame('KvScan', $method);
                $this->assertInstanceOf(ScanRequest::class, $request);
                $this->assertTrue($request->getReverse(), 'a reverse scan must set KvScanRequest.reverse');
                $capturedRequests[] = $request;

                $matching = [];
                foreach ($stored as $key) {
                    if (KeyOrder::inRange($key, $request->getEndKey(), $request->getStartKey())) {
                        $matching[] = $key;
                    }
                }
                rsort($matching, SORT_STRING);

                $response = new ScanResponse();
                $pairs = [];
                foreach (array_slice($matching, 0, $request->getLimit()) as $key) {
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v-' . $key);
                    $pairs[] = $pair;
                }
                $response->setPairs($pairs);

                return $response;
            },
        );
    }

    public function testReverseScanWalksTwoRegionsFromTheUpperBoundDown(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');
        $this->stubRegions([$region1, $region2]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'k3', 'k4'], $capturedRequests);

        $result = $this->createTransaction()->reverseScan('k9', '');

        $this->assertSame(['k4', 'k3', 'k2', 'k1'], array_column($result, 'key'));
        $this->assertCount(2, $capturedRequests);

        // The upper region is read first, then the region below it.
        $this->assertSame('k9', $capturedRequests[0]->getStartKey());
        $this->assertSame('k3', $capturedRequests[0]->getEndKey());
        $this->assertSame('k3', $capturedRequests[1]->getStartKey());
        $this->assertSame('', $capturedRequests[1]->getEndKey());
    }

    public function testReverseScanHonoursBothBounds(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->stubRegions([$region]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'k3', 'k4', 'k5'], $capturedRequests);

        // [k2, k4): k4 is the exclusive upper bound, k2 the inclusive lower.
        $result = $this->createTransaction()->reverseScan('k4', 'k2');

        $this->assertSame(['k3', 'k2'], array_column($result, 'key'));
        $this->assertSame('k4', $capturedRequests[0]->getStartKey());
        $this->assertSame('k2', $capturedRequests[0]->getEndKey());
    }

    public function testReverseScanMergesBufferedWritesInDescendingOrder(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');
        $this->stubRegions([$region1, $region2]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'k3', 'k4'], $capturedRequests);

        $txn = $this->createTransaction();
        $txn->set('k5', 'written-by-the-transaction');
        $txn->delete('k2');

        $result = $txn->reverseScan('k9', '');

        // The buffered set appears above k4; the tombstone hides k2.
        $this->assertSame(['k5', 'k4', 'k3', 'k1'], array_column($result, 'key'));
        $this->assertSame('written-by-the-transaction', $result[0]['value']);
    }

    public function testReverseScanWithLimitReturnsTheHighestRows(): void
    {
        $region1 = $this->makeRegion(1, '', 'k3');
        $region2 = $this->makeRegion(2, 'k3', '');
        $this->stubRegions([$region1, $region2]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'k3', 'k4', 'k5'], $capturedRequests);

        $result = $this->createTransaction()->reverseScan('k9', '', 2);

        $this->assertSame(['k5', 'k4'], array_column($result, 'key'));
        $this->assertCount(1, $capturedRequests);
        $this->assertSame(2, $capturedRequests[0]->getLimit());
    }

    public function testReverseScanLimitKeepsTheHighestKeysAfterTheWriteSetMerge(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->stubRegions([$region]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'k3'], $capturedRequests);

        $txn = $this->createTransaction();
        $txn->set('k9', 'written-by-the-transaction');

        // The raw scan returns the two highest stored keys (k3, k2); the
        // buffered k9 sorts above them, so the limit must keep k9 and k3 and
        // drop k2 — not keep the two lowest merged rows.
        $result = $txn->reverseScan('k9x', '', 2);

        $this->assertSame(['k9', 'k3'], array_column($result, 'key'));
    }

    public function testReverseScanOfAnEmptyRangeReturnsNothingWithoutAnRpc(): void
    {
        $this->grpc->expects($this->never())->method('call');

        $this->assertSame([], $this->createTransaction()->reverseScan('k3', 'k3'));
    }

    public function testReverseScanRejectsAnEmptyUpperBoundWithANonEmptyLowerBound(): void
    {
        $this->grpc->expects($this->never())->method('call');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('an empty start key is not a valid upper bound');

        $this->createTransaction()->reverseScan('', 'k1');
    }

    public function testReverseScanRejectsAnInvertedRange(): void
    {
        $this->grpc->expects($this->never())->method('call');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('start key must not sort before end key');

        $this->createTransaction()->reverseScan('k1', 'k9');
    }

    public function testReverseScanProbesBelowTheLowestRowWhenTheCapIsSpent(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->stubRegions([$region]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'k3'], $capturedRequests);

        try {
            $this->createTransaction(2)->reverseScan('k9', '', 0);
            $this->fail('Expected ScanLimitExceededException was not thrown');
        } catch (ScanLimitExceededException $e) {
            $this->assertSame(2, $e->getMaxRows());
        } finally {
            // The first request spends the budget on [k2, k9); the probe asks
            // for one row below k2, in [k1, k2).
            $this->assertCount(2, $capturedRequests);
            $this->assertSame('k2', $capturedRequests[1]->getStartKey());
            $this->assertSame('', $capturedRequests[1]->getEndKey());
            $this->assertSame(1, $capturedRequests[1]->getLimit());
        }
    }

    public function testReverseScanReturnsTheWholeRangeWhenItFitsTheCapExactly(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->stubRegions([$region]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2'], $capturedRequests);

        $result = $this->createTransaction(2)->reverseScan('k9', '', 0);

        $this->assertSame(['k2', 'k1'], array_column($result, 'key'));
        // The probe answers empty, is re-asked once (issue #512) and answers
        // empty again; the range is complete.
        $this->assertCount(3, $capturedRequests);
        $this->assertSame('k1', $capturedRequests[1]->getStartKey());
        $this->assertSame('k1', $capturedRequests[2]->getStartKey());
    }

    public function testReverseScanContinuesAboveASplitRegionAndTrimsTheSplicedBatch(): void
    {
        // The outer enumeration sees the pre-split region [ "", "z"), but the
        // fresh lookup inside the scan sees two halves. The upper remainder
        // ["m", "z") must be spliced BEFORE the lower batch in descending
        // order, and the lower batch must be trimmed to the remaining limit.
        $preSplit = $this->makeRegion(1, '', 'z');
        $freshLower = $this->makeRegion(2, '', 'm');
        $freshUpper = $this->makeRegion(3, 'm', 'z');

        $this->regionCache->method('getRegionBelow')->willReturn($preSplit);
        $this->regionCache->method('getByKey')->willReturnCallback(
            static fn(string $key): RegionInfo => KeyOrder::lt($key, 'm') ? $freshLower : $freshUpper,
        );
        $this->regionCache->method('put');

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubReverseScan(['k1', 'k2', 'm1', 'm2'], $capturedRequests);

        $result = $this->createTransaction()->reverseScan('z', '', 3);

        $this->assertSame(['m2', 'm1', 'k2'], array_column($result, 'key'));

        $this->assertCount(2, $capturedRequests);
        // The lower half is sent first, then the upper remainder.
        $this->assertSame('m', $capturedRequests[0]->getStartKey());
        $this->assertSame('', $capturedRequests[0]->getEndKey());
        $this->assertSame('z', $capturedRequests[1]->getStartKey());
        $this->assertSame('m', $capturedRequests[1]->getEndKey());
    }

    public function testReverseScanReasksATransientEmptyBatchOnce(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->stubRegions([$region]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $call = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
            ) use (
                &$call,
                &$capturedRequests,
            ): ScanResponse {
                $this->assertSame('KvScan', $method);
                $this->assertInstanceOf(ScanRequest::class, $request);
                $this->assertTrue($request->getReverse());
                $capturedRequests[] = $request;

                $response = new ScanResponse();
                if (++$call === 1) {
                    // The diagnosed transient shape: an empty answer while
                    // rows exist. The reverse path must re-ask once.
                    return $response;
                }

                foreach (['k2', 'k1'] as $key) {
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v-' . $key);
                    $response->setPairs([...$response->getPairs(), $pair]);
                }

                return $response;
            },
        );

        $result = $this->createTransaction()->reverseScan('k9', '');

        $this->assertSame(['k2', 'k1'], array_column($result, 'key'));
        $this->assertCount(2, $capturedRequests);
        $this->assertSame('k9', $capturedRequests[0]->getStartKey());
        $this->assertSame('k9', $capturedRequests[1]->getStartKey());
    }

    public function testReverseScanReasksATransientEmptyTruncationProbe(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->stubRegions([$region]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $call = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
            ) use (
                &$call,
                &$capturedRequests,
            ): ScanResponse {
                $this->assertSame('KvScan', $method);
                $this->assertInstanceOf(ScanRequest::class, $request);
                $this->assertTrue($request->getReverse());
                $capturedRequests[] = $request;

                $response = new ScanResponse();
                $call++;

                if ($call === 1) {
                    // The main scan spends the whole cap on [k2, k9).
                    foreach (['k3', 'k2'] as $key) {
                        $pair = new KvPair();
                        $pair->setKey($key);
                        $pair->setValue('v-' . $key);
                        $response->setPairs([...$response->getPairs(), $pair]);
                    }

                    return $response;
                }

                if ($call === 2) {
                    // The truncation probe answers empty transiently.
                    return $response;
                }

                // The re-asked probe finds the row that proves truncation.
                $pair = new KvPair();
                $pair->setKey('k1');
                $pair->setValue('v-k1');
                $response->setPairs([$pair]);

                return $response;
            },
        );

        try {
            $this->createTransaction(2)->reverseScan('k9', '', 0);
            $this->fail('Expected ScanLimitExceededException was not thrown');
        } catch (ScanLimitExceededException) {
            $this->assertCount(3, $capturedRequests);
            $this->assertSame('k2', $capturedRequests[1]->getStartKey());
            $this->assertSame('k2', $capturedRequests[2]->getStartKey());
        }
    }
}
