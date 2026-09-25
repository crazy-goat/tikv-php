<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
use PHPUnit\Framework\TestCase;

/**
 * Byte-order regression tests for the TxnKV scan continuation
 * (issue #186).
 *
 * TxnReader::executeScanForRegion() re-clips a sub-range against the region
 * re-resolved on every attempt and then decides whether to continue past that
 * region. Both decisions used PHP's relational operators, which compare
 * numerically when both operands are numeric strings — so a range boundary
 * like "30" vs "4" (bytewise "30" < "4", numerically 30 > 4) was resolved
 * the wrong way. The observable effects are a wire range that crosses the
 * region boundary (TiKV answers KeyNotInRegion) and a continuation that
 * stops early, dropping the remainder of the scan.
 *
 * Every key below is chosen so that the byte order and the numeric order
 * disagree: bytewise "1" < "15" < "199" < "2" < "20" < "30" < "39" < "4".
 */
final class TxnReaderKeyOrderTest extends TestCase
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

    private function createTransaction(): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-key-order',
            startTs: 1000,
            pessimistic: false,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
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
     * Resolve keys to regions exactly like a real half-open [start, end)
     * layout does — bytewise, never numerically (issue #186).
     *
     * @param RegionInfo[] $regions
     */
    private function stubRegionLookup(array $regions): void
    {
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
    }

    /**
     * Build the stored map from a list of key strings. Building it from an
     * array literal would make PHPStan infer array<int, string> — PHP coerces
     * the numeric-string keys to int — and hide the contract under test
     * (issue #322).
     *
     * @param list<string> $keys
     * @return array<string, string>
     */
    private function stored(array $keys): array
    {
        $stored = [];
        foreach ($keys as $key) {
            $stored[$key] = 'v-' . $key;
        }

        return $stored;
    }

    /**
     * Emulate a TiKV scan: return the stored keys that fall inside the
     * requested half-open range, in byte order.
     *
     * @param list<ScanRequest> $capturedRequests
     * @param array<string, string> $stored
     */
    private function stubScan(array $stored, array &$capturedRequests): void
    {
        // array_keys() coerces numeric-string keys ("30") to int (issue
        // #322), so cast them back to the bytes TiKV actually stores.
        $keys = array_map(strval(...), array_keys($stored));
        sort($keys, SORT_STRING);

        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request
            ) use (
                $keys,
                $stored,
                &$capturedRequests
            ): ScanResponse {
                $this->assertInstanceOf(ScanRequest::class, $request);
                $capturedRequests[] = $request;

                $response = new ScanResponse();
                foreach ($keys as $key) {
                    if (!KeyOrder::inRange($key, $request->getStartKey(), $request->getEndKey())) {
                        continue;
                    }
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue($stored[$key]);
                    $response->setPairs([...$response->getPairs(), $pair]);
                }

                return $response;
            },
        );
    }

    public function testScanClipsTheWireRangeToTheFreshRegionEndBytewise(): void
    {
        // The outer enumeration is the pre-split ['', '4'); the retry
        // closure re-resolves the shrunken ['', '30') first. "30" must be
        // clipped as the wire end because it is bytewise BEFORE "4" — the
        // numeric comparison (30 < 4 is false) either crossed the region
        // boundary or truncated the scan.
        $this->pdClient->method('scanRegions')->willReturn([$this->makeRegion(1, '', '4')]);
        $this->stubRegionLookup([
            $this->makeRegion(2, '', '30'),
            $this->makeRegion(3, '30', '4'),
        ]);

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan($this->stored(['1', '15', '199', '2', '20', '30', '39']), $capturedRequests);

        $result = $this->createTransaction()->scan('1', '4', 100);

        $this->assertSame(
            ['1', '15', '199', '2', '20', '30', '39'],
            array_column($result, 'key'),
        );
        $this->assertCount(2, $capturedRequests);
        $this->assertSame('1', $capturedRequests[0]->getStartKey());
        $this->assertSame('30', $capturedRequests[0]->getEndKey());
        $this->assertSame('30', $capturedRequests[1]->getStartKey());
        $this->assertSame('4', $capturedRequests[1]->getEndKey());
    }

    public function testScanStopsWhenTheRegionAlreadyCoversTheRequestedRange(): void
    {
        // The fresh region ends at "20" while the caller asked for
        // ["1", "100"): bytewise "20" is AFTER "100", so the whole requested
        // range is covered and the scan must end. A numeric "20" < "100"
        // clipped the wire range to "20" AND made the continuation run,
        // re-scanning ["20", "100") for nothing.
        $this->pdClient->method('scanRegions')->willReturn([$this->makeRegion(1, '', '20')]);
        $this->stubRegionLookup([$this->makeRegion(2, '', '20')]);
        $this->pdClient->method('getRegion')->willReturn($this->makeRegion(3, '20', ''));

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        // Bytewise ["1", "100") holds "1" and "10" only: a leading '1'
        // followed by a byte below '0' is required, so "15" and "2" are
        // outside it despite being numerically inside.
        $this->stubScan($this->stored(['1', '10', '15', '2']), $capturedRequests);

        $result = $this->createTransaction()->scan('1', '100', 100);

        $this->assertSame(['1', '10'], array_column($result, 'key'));
        $this->assertCount(1, $capturedRequests);
        $this->assertSame('1', $capturedRequests[0]->getStartKey());
        $this->assertSame('100', $capturedRequests[0]->getEndKey());
    }

    public function testScanStopsWhenTheFreshRegionEndDoesNotAdvanceTheCursor(): void
    {
        // Defensive guard for an inconsistent re-resolution: the cache hands
        // back a region ending at "100" for the cursor "9", so the cursor did
        // not advance (bytewise "100" < "9") and continuing would re-scan a
        // range the region does not own. Numerically 100 > 9, so the old
        // guard continued and issued a second, empty KvScan.
        $this->pdClient->method('scanRegions')->willReturn([$this->makeRegion(1, '', '')]);
        $this->regionCache->method('getByKey')->willReturn($this->makeRegion(2, '', '100'));
        $this->regionCache->method('put');

        /** @var list<ScanRequest> $capturedRequests */
        $capturedRequests = [];
        $this->stubScan($this->stored(['9']), $capturedRequests);

        $result = $this->createTransaction()->scan('9', '', 100);

        // The wire range ["9", "100") is empty bytewise, so nothing comes
        // back — the point is that the scan stops there instead of issuing a
        // second request.
        $this->assertSame([], array_column($result, 'key'));
        $this->assertCount(1, $capturedRequests);
        $this->assertSame('9', $capturedRequests[0]->getStartKey());
        $this->assertSame('100', $capturedRequests[0]->getEndKey());
    }
}
