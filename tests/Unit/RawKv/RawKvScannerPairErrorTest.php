<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvScanner;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * The parallel scan paths (scanSegmentsInParallel, batchScan) wait on the raw
 * response without RegionErrorHandler::check(); parseScanPairs() is where a
 * per-pair KeyError must be rejected (issue #281).
 */
class RawKvScannerPairErrorTest extends TestCase
{
    private function parse(RawScanResponse $response): mixed
    {
        $pd = $this->createMock(PdClientInterface::class);
        $cache = $this->createMock(RegionCacheInterface::class);
        $scanner = new RawKvScanner(
            $pd,
            $this->createMock(GrpcClientInterface::class),
            new RegionResolver($pd, $cache),
            new TimeoutConfig(),
            1000,
            1000,
            $cache,
            new NullLogger(),
        );

        return (new \ReflectionMethod($scanner, 'parseScanPairs'))->invoke($scanner, $response, false);
    }

    public function testPerPairKeyErrorIsRejected(): void
    {
        $keyError = new KeyError();
        $keyError->setRetryable('too old');
        $pair = new KvPair();
        $pair->setKey('k');
        $pair->setError($keyError);
        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('per-pair error');

        $this->parse($response);
    }

    public function testCleanPairsAreMapped(): void
    {
        $pair = new KvPair();
        $pair->setKey('k');
        $pair->setValue('v');
        $response = new RawScanResponse();
        $response->setKvs([$pair]);

        self::assertSame([['key' => 'k', 'value' => 'v']], $this->parse($response));
    }
}
