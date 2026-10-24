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
use PHPUnit\Framework\TestCase;

/**
 * A one-off empty KvScan batch for a still-unsatisfied limit must be
 * re-asked a bounded number of times instead of ending the scan with zero
 * rows (issue #512; the diagnosed mechanism of the
 * testScanWithLimitOneReturnsSingleKey CI flake).
 */
final class TxnReaderEmptyScanReattemptTest extends TestCase
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

        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->regionCache->method('getByKey')->willReturn($region);
    }

    private function createTransaction(): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'test-txn-empty-scan',
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

    /**
     * @param list<list<string>> $answers keys returned by each successive KvScan call
     * @param list<ScanRequest> $captured
     */
    private function stubScanAnswers(array $answers, array &$captured): void
    {
        $call = 0;
        $this->grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                mixed $request,
            ) use (
                $answers,
                &$call,
                &$captured
            ): ScanResponse {
                $this->assertInstanceOf(ScanRequest::class, $request);
                $captured[] = $request;

                $pairs = [];
                foreach ($answers[$call] ?? [] as $key) {
                    $pair = new KvPair();
                    $pair->setKey($key);
                    $pair->setValue('v-' . $key);
                    $pairs[] = $pair;
                }
                $call++;

                $response = new ScanResponse();
                $response->setPairs($pairs);

                return $response;
            },
        );
    }

    public function testOneOffEmptyBatchIsReaskedAndReturnsTheKey(): void
    {
        /** @var list<ScanRequest> $captured */
        $captured = [];
        $this->stubScanAnswers([[], ['k1']], $captured);

        $result = $this->createTransaction()->scan('', '', 1);

        $this->assertSame(['k1'], array_column($result, 'key'));
        $this->assertCount(2, $captured);
        $this->assertSame($captured[0]->getStartKey(), $captured[1]->getStartKey());
    }

    public function testPersistentEmptyAnswerIsAcceptedAfterTheBoundedReattempt(): void
    {
        /** @var list<ScanRequest> $captured */
        $captured = [];
        $this->stubScanAnswers([], $captured);

        $result = $this->createTransaction()->scan('', '', 5);

        $this->assertSame([], $result);
        $this->assertCount(2, $captured);
    }

    public function testNonEmptyBatchIsNotReasked(): void
    {
        /** @var list<ScanRequest> $captured */
        $captured = [];
        $this->stubScanAnswers([['k1']], $captured);

        $result = $this->createTransaction()->scan('', '', 5);

        $this->assertSame(['k1'], array_column($result, 'key'));
        $this->assertCount(1, $captured);
    }
}
