<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\TransactionState;
use CrazyGoat\TiKV\Client\TxnKv\TxnReader;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #725: `TxnReader::scan()` validated the caller's `$limit` but not the
 * `$maxScanLimit` it is compared against. With `limit: 0` and a cap of `0`
 * the normalisation returned `0`, the region loop's `$remaining > 0` guard
 * never fired, and TiKV received the wire limit `0` — "no limit" — so the
 * whole range was buffered in memory.
 *
 * `Transaction` rejects such a cap in its constructor, but `TxnReader` is a
 * public class whose `$maxScanLimit` is a plain scan() parameter, so the
 * reader itself must reject it. The rejection has to happen before any PD or
 * store RPC, which is what the `expects($this->never())` block pins.
 */
#[CoversClass(TxnReader::class)]
final class TxnReaderScanLimitValidationTest extends TestCase
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

    /**
     * @return array<string, array{int}>
     */
    public static function nonPositiveMaxScanLimitProvider(): array
    {
        return [
            'zero disables the cap' => [0],
            'negative cannot bound the scan' => [-1],
        ];
    }

    #[DataProvider('nonPositiveMaxScanLimitProvider')]
    public function testWholeRangeScanRejectsNonPositiveMaxScanLimit(int $maxScanLimit): void
    {
        $this->rejectBeforeAnyRpc('maxScanLimit must be >= 1');

        $this->scan(0, $maxScanLimit);
    }

    #[DataProvider('nonPositiveMaxScanLimitProvider')]
    public function testExplicitLimitRejectsNonPositiveMaxScanLimit(int $maxScanLimit): void
    {
        // Before the fix this combination only failed because 5 > 0; the
        // message blamed the caller's limit even though the cap was invalid.
        $this->rejectBeforeAnyRpc('maxScanLimit must be >= 1');

        $this->scan(5, $maxScanLimit);
    }

    public function testReverseScanRejectsZeroMaxScanLimit(): void
    {
        $this->rejectBeforeAnyRpc('maxScanLimit must be >= 1');

        $this->scan(0, 0, true);
    }

    /**
     * A valid cap still reaches the region loop; the guard must reject only
     * the values that cannot bound a scan.
     */
    public function testPositiveMaxScanLimitIsAccepted(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
        $this->regionCache->method('getByKey')->willReturn($region);

        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');
        $this->pdClient->method('getStore')->willReturn($store);

        $this->grpc->method('call')->willReturn(new ScanResponse());

        $this->assertSame([], $this->scan(0, 1));
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    private function scan(int $limit, int $maxScanLimit, bool $reverse = false): array
    {
        return $this->reader()->scan(
            $reverse ? 'z' : 'a',
            $reverse ? 'a' : 'z',
            $limit,
            new TransactionState(),
            $this->executor(),
            static fn (): RetryVerdict => RetryVerdict::pass(),
            $maxScanLimit,
            $reverse,
        );
    }

    private function rejectBeforeAnyRpc(string $message): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->expects($this->never())->method('getRegion');
        $this->pdClient->expects($this->never())->method('getStore');
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);
    }

    private function reader(): TxnReader
    {
        return new TxnReader(
            startTs: 1000,
            grpc: $this->grpc,
            pdClient: $this->pdClient,
            regionResolver: $this->regionResolver,
            timeoutConfig: new TimeoutConfig(),
            lockResolver: new LockResolver(
                $this->grpc,
                $this->regionResolver,
                $this->regionCache,
                $this->pdClient,
                1000,
            ),
            regionCache: $this->regionCache,
        );
    }

    private function executor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            60000,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            new NullLogger(),
        );
    }
}
