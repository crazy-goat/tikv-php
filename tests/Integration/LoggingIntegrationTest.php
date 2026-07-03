<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Integration;

use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRangeResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\RawScanResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class LoggingIntegrationTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private TestHandler $testHandler;
    private Logger $logger;

    private function defaultRegion(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
        );
    }

    private function defaultStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        return $store;
    }

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->testHandler = new TestHandler();
        $this->logger = new Logger('tikv-test', [$this->testHandler]);
    }

    public function testSuccessfulGetLogsCacheMissAndCached(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('hello');
        $this->grpc->method('call')->willReturn($response);

        $result = $client->get('mykey');

        $this->assertSame('hello', $result);
        $this->assertTrue($this->testHandler->hasDebugThatContains('Region cache miss'));
        $this->assertTrue($this->testHandler->hasDebugThatContains('Region cached'));
    }

    public function testCacheHitLogsDebug(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('hello');
        $this->grpc->method('call')->willReturn($response);

        $client->get('mykey');
        $client->get('mykey');

        $this->assertTrue($this->testHandler->hasDebugThatContains('Region cache hit'));
    }

    public function testRetryLogsWarningWithMonolog(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $response,
            );

        $result = $client->get('retrykey');

        $this->assertSame('recovered', $result);
        $this->assertTrue($this->testHandler->hasWarningThatContains('Retrying operation'));
        $this->assertTrue($this->testHandler->hasInfoThatContains('Invalidated region on retry'));
    }

    public function testScanLogsRetryWarning(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $region = $this->defaultRegion();
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $scanResponse = new RawScanResponse();
        $kv = new KvPair();
        $kv->setKey('found');
        $kv->setValue('data');
        $scanResponse->setKvs([$kv]);

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $scanResponse,
            );

        $result = $client->scan('a', 'z', 10);

        $this->assertCount(1, $result);
        $this->assertSame('found', $result[0]['key']);
        $this->assertTrue($this->testHandler->hasWarningThatContains('Retrying operation'));
    }

    public function testDeleteRangeLogsRetryWarning(): void
    {
        if (!extension_loaded('grpc')) {
            $this->markTestSkipped('gRPC extension not available');
        }

        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $region = $this->defaultRegion();
        $this->pdClient->method('scanRegions')->willReturn([$region]);
        $this->pdClient->method('getRegion')->willReturn($region);

        $deleteRangeResponse = new RawDeleteRangeResponse();

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $deleteRangeResponse,
            );

        $client->deleteRange('a', 'z');

        $this->assertTrue($this->testHandler->hasWarningThatContains('Retrying operation'));
    }

    public function testFatalErrorLogsErrorWithMonolog(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->method('call')
            ->willThrowException(new TiKvException('RaftEntryTooLarge'));

        try {
            $client->get('bigkey');
        } catch (TiKvException) {
        }

        $this->assertTrue($this->testHandler->hasErrorThatContains('Fatal error, not retrying'));
    }

    public function testBudgetExhaustedLogsErrorWithMonolog(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 0, $this->logger, 0);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $this->grpc->method('call')
            ->willThrowException(new TiKvException('ServerIsBusy'));

        try {
            $client->get('busykey');
        } catch (TiKvException) {
        }

        $this->assertTrue($this->testHandler->hasErrorThatContains('budget exhausted'));
    }

    public function testLogContextContainsRedactedKey(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('recovered');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('EpochNotMatch')),
                $response,
            );

        $client->get('contextkey');

        $warningRecords = array_filter(
            $this->testHandler->getRecords(),
            fn (\Monolog\LogRecord $record): bool => $record->level === Level::Warning
                && str_contains($record->message, 'Retrying'),
        );

        $this->assertNotEmpty($warningRecords);
        $record = reset($warningRecords);
        $this->assertInstanceOf(\Monolog\LogRecord::class, $record);
        // Key context should be redacted, not contain the raw key
        $this->assertArrayHasKey('key', $record->context);
        $this->assertStringContainsString('bytes', $record->context['key']);
        $this->assertStringNotContainsString('contextkey', $record->context['key']);
        $this->assertSame(0, $record->context['attempt']);
        $this->assertSame('None', $record->context['backoffType']);
        $this->assertArrayHasKey('sleepMs', $record->context);
        $this->assertArrayHasKey('totalBackoffMs', $record->context);
    }

    public function testRegionCacheInvalidationLogContainsRegionId(): void
    {
        $cache = new RegionCache(logger: $this->logger);
        $client = new RawKvClient($this->pdClient, $this->grpc, $cache, 20000, $this->logger);

        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($this->defaultStore());

        $response = new RawGetResponse();
        $response->setValue('ok');

        $this->grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new TiKvException('RegionNotFound')),
                $response,
            );

        $client->put('regionkey', 'value');

        $response2 = new RawPutResponse();
        $this->grpc->method('call')->willReturn($response2);

        $infoRecords = array_filter(
            $this->testHandler->getRecords(),
            fn (\Monolog\LogRecord $record): bool => $record->level === Level::Info
                && str_contains($record->message, 'Invalidated region'),
        );

        $this->assertNotEmpty($infoRecords);
        $record = reset($infoRecords);
        $this->assertInstanceOf(\Monolog\LogRecord::class, $record);
        $this->assertSame(1, $record->context['regionId']);
    }
}
