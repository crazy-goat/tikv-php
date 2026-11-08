<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * The negative-limit scan contract of the public RawKvClient entry points
 * (issue #332).
 *
 * A negative limit previously passed through to RawScanRequest::setLimit(),
 * a uint32 protobuf field, so setLimit(-1) serialised to 4294967295 and TiKV
 * ran an effectively unbounded scan. Every public scan entry point must reject
 * negatives before any RPC is issued.
 *
 * This class deliberately needs no ext-grpc: it only builds interface mocks,
 * so the regression runs in the extension-free Unit suite as well as in Grpc.
 */
class RawKvClientScanLimitTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RawKvClient $client;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
        );
    }

    /**
     * Pin that a rejected argument never reaches the wire: neither PD nor a
     * store may be asked for anything (issue #310).
     */
    private function expectNoRpc(): void
    {
        $this->pdClient->expects($this->never())->method('scanRegions');
        $this->pdClient->expects($this->never())->method('getStore');
        $this->grpc->expects($this->never())->method('call');
        $this->grpc->expects($this->never())->method('callAsync');
    }

    /**
     * @param int $limit a negative limit arriving from user input
     */
    #[DataProvider('negativeScanLimitProvider')]
    public function testScanNegativeLimitThrows(int $limit): void
    {
        $this->expectNoRpc();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $this->client->scan('a', 'z', $limit);
    }

    public function testReverseScanNegativeLimitThrows(): void
    {
        $this->expectNoRpc();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $this->client->reverseScan('z', 'a', -1);
    }

    public function testScanPrefixNegativeLimitThrows(): void
    {
        $this->expectNoRpc();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Scan limit must be 0 or greater');

        $this->client->scanPrefix('prefix', -1);
    }

    public function testBatchScanNegativeEachLimitThrows(): void
    {
        // batchScan() uses a `<= 0` guard with its own message; assert
        // THAT existing contract rather than the scan() message.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('eachLimit must be greater than 0');

        $this->client->batchScan([['a', 'z']], -1);
    }

    public function testScanIteratorNegativeBatchSizeThrows(): void
    {
        // ScanIterator's constructor uses a `<= 0` guard with its own
        // message; assert that existing contract.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchSize must be greater than 0');

        $this->client->scanIterator('a', 'z', -1);
    }

    public function testScanPrefixIteratorNegativeBatchSizeThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchSize must be greater than 0');

        $this->client->scanPrefixIterator('prefix', -1);
    }

    /** @return iterable<string, array{int}> */
    public static function negativeScanLimitProvider(): iterable
    {
        yield 'minus one' => [-1];
        yield 'minus large' => [-10240];
        yield 'int min' => [PHP_INT_MIN];
    }
}
