<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\TestCase;

/**
 * A transactional scan must never truncate its result silently (issue #440).
 *
 * `Transaction::scan()` used to normalise `limit: 0` to the per-transaction
 * cap (`Transaction::MAX_SCAN_LIMIT`, 10240) and return whatever fitted — a
 * range with a million rows came back as 10240 rows, with no error and
 * nothing in the return value to tell "the range had 10240 rows" from "the
 * range had a million and we cut it off". A reconciliation job or an export
 * built on that call quietly processed a fraction of the data and reported
 * success.
 *
 * The scan now fails loudly instead: when the whole-range scan spends its
 * budget it probes for one more row and throws
 * {@see ScanLimitExceededException} when the range really holds more. The
 * probe is what keeps the two cases apart — a range with *exactly* the cap is
 * complete and must be returned, not refused.
 *
 * The cap is a constructor seam here (`maxScanLimit`), the same shape as
 * `RawKvScanner`'s `maxScanRows` / `scanPageSize`: a test seam, not a
 * production knob. The default is `Transaction::MAX_SCAN_LIMIT`.
 */
final class TransactionScanTruncationTest extends TestCase
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

    private function createTransaction(int $maxScanLimit): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-scan-truncation',
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
     * One region covering the whole keyspace, so every key — including the
     * probe's cursor — resolves to it.
     */
    private function stubSingleRegion(): void
    {
        $region = $this->makeRegion(1, '', '');
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->regionCache->method('getByKey')->willReturn($region);
        $this->regionCache->method('put');
    }

    /**
     * Emulate TiKV: return the stored keys inside the requested half-open
     * range, in byte order, honouring the request's row limit.
     *
     * @param list<string>        $stored
     * @param list<ScanRequest>   $capturedRequests
     */
    private function stubScan(array $stored, array &$capturedRequests): void
    {
        sort($stored, SORT_STRING);

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
                $this->assertInstanceOf(ScanRequest::class, $request);
                $capturedRequests[] = $request;

                $response = new ScanResponse();
                $rows = 0;
                foreach ($stored as $key) {
                    if (!KeyOrder::inRange($key, $request->getStartKey(), $request->getEndKey())) {
                        continue;
                    }
                    if ($rows >= $request->getLimit()) {
                        break;
                    }
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v-' . $key);
                    $response->setPairs([...$response->getPairs(), $pair]);
                    $rows++;
                }

                return $response;
            },
        );
    }

    public function testWholeRangeScanThrowsWhenTheRangeHoldsMoreRowsThanTheCap(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan(['k1', 'k2', 'k3', 'k4', 'k5'], $capturedRequests);

        try {
            $this->createTransaction(3)->scan('', '', 0);
            $this->fail('Expected ScanLimitExceededException was not thrown');
        } catch (ScanLimitExceededException $e) {
            $this->assertSame(3, $e->getMaxRows());
            $this->assertSame(3, $e->getScannedRows());
            $this->assertStringContainsString('exceeding the maximum of 3 rows', $e->getMessage());
        }
    }

    public function testWholeRangeScanReturnsTheRangeWhenItFitsTheCapExactly(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan(['k1', 'k2', 'k3'], $capturedRequests);

        $result = $this->createTransaction(3)->scan('', '', 0);

        // A range with exactly the cap is complete: it is returned, not
        // refused. The probe still runs — it is what tells the two apart.
        $this->assertSame(['k1', 'k2', 'k3'], array_column($result, 'key'));
        $this->assertCount(2, $capturedRequests);
        $this->assertSame('k3' . "\x00", $capturedRequests[1]->getStartKey());
        $this->assertSame(1, $capturedRequests[1]->getLimit());
    }

    public function testWholeRangeScanProbesForOneMoreRowAfterTheLastReturnedKey(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan(['k1', 'k2', 'k3', 'k4'], $capturedRequests);

        $this->expectException(ScanLimitExceededException::class);

        try {
            $this->createTransaction(3)->scan('', '', 0);
        } finally {
            // The probe asks for a single row right after the last key the
            // scan returned — that is the only way to tell a complete range
            // from a truncated one.
            $this->assertCount(2, $capturedRequests);
            $this->assertSame('k3' . "\x00", $capturedRequests[1]->getStartKey());
            $this->assertSame(1, $capturedRequests[1]->getLimit());
        }
    }

    public function testScanWithAnExplicitLimitReturnsAtMostThatManyRows(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan(['k1', 'k2', 'k3', 'k4', 'k5'], $capturedRequests);

        $result = $this->createTransaction(10)->scan('', '', 2);

        // An explicit limit truncates by contract; the caller detects it by
        // comparing the returned count with the limit. No exception, and no
        // probe: the limit is the caller's own bound.
        $this->assertSame(['k1', 'k2'], array_column($result, 'key'));
        $this->assertCount(1, $capturedRequests);
    }

    public function testWholeRangeScanKeepsTheTransactionsOwnInRangeWrites(): void
    {
        $this->stubSingleRegion();

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan(['k1', 'k2', 'k3'], $capturedRequests);

        $txn = $this->createTransaction(3);
        $txn->set('k0', 'written-by-the-transaction');

        $result = $txn->scan('', '', 0);

        // The cap bounds the TiKV read; the write-set merge must not drop the
        // transaction's own in-range key on top of it (read-your-writes).
        $this->assertSame(['k0', 'k1', 'k2', 'k3'], array_column($result, 'key'));
        $this->assertSame('written-by-the-transaction', $result[0]['value']);
    }
}
