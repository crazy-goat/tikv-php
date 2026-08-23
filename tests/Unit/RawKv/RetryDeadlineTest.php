<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Retry\RetryBudgetExhaustedException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #294: the retry executor's blocking usleep() backoff must be bounded
 * by a wall-clock deadline by default, so a sustained ServerIsBusy episode
 * cannot pin a PHP-FPM worker for minutes inside a single request.
 */
class RetryDeadlineTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->regionCache->method('invalidate');

        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');
        $this->pdClient->method('getRegion')->willReturn($this->defaultRegion());
        $this->pdClient->method('getStore')->willReturn($store);
    }

    private function defaultRegion(): \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo
    {
        return new \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    /**
     * GrpcClientInterface double that always answers ServerIsBusy.
     */
    private function alwaysServerBusy(): void
    {
        $this->grpc->method('call')->willThrowException(new TiKvException('ServerIsBusy'));
    }

    public function testDefaultRetryDeadlineIsNonZero(): void
    {
        $this->assertGreaterThan(0, RawKvClient::DEFAULT_RETRY_DEADLINE_MS);
        $this->assertSame(RawKvClient::DEFAULT_RETRY_DEADLINE_MS, 30000);
    }

    public function testGetFailsWithinConfiguredDeadlineWhenServerAlwaysBusy(): void
    {
        // ServerBusy sleeps ~1000-2000ms per attempt; with the budget disabled
        // (0) only the wall-clock deadline can stop the loop.
        $deadlineMs = 50;
        $toleranceMs = 2000;
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            maxBackoffMs: 0,           // disable the non-ServerBusy budget
            serverBusyBudgetMs: 0,     // disable the ServerBusy backoff budget…
            retryDeadlineMs: $deadlineMs, // …so ONLY the deadline can end the loop
        );
        $this->alwaysServerBusy();

        $startMs = (int) (microtime(true) * 1000);

        try {
            $client->get('key');
            $this->fail('Expected the retry deadline to end the ServerBusy retry loop');
        } catch (TiKvException) {
            // Both exits carry the last ServerIsBusy error here; either is fine.
        }

        $elapsedMs = (int) (microtime(true) * 1000) - $startMs;
        $this->assertLessThanOrEqual($deadlineMs + $toleranceMs, $elapsedMs);
    }

    public function testGetThrowsRetryBudgetExhaustedUnderSustainedServerBusy(): void
    {
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            maxBackoffMs: 0,
            // Keep the default ServerBusy budget: ServerBusy backoff alone
            // (2 s base, 10 s cap per attempt) would exhaust it only after
            // minutes. The wall-clock deadline must be the binding bound.
            retryDeadlineMs: 30,
        );
        // A ServerIsBusy region error on every response is classified as
        // BackoffType::ServerBusy and charged to the ServerBusy budget; the
        // deadline then ends the loop with RetryBudgetExhaustedException.
        $error = new \CrazyGoat\Proto\Errorpb\Error();
        $error->setServerIsBusy(new \CrazyGoat\Proto\Errorpb\ServerIsBusy());
        $response = new RawGetResponse();
        $response->setRegionError($error);
        $this->grpc->method('call')->willReturn($response);

        $startMs = (int) (microtime(true) * 1000);

        $this->expectException(RetryBudgetExhaustedException::class);
        $this->expectExceptionMessage('Retry deadline');

        try {
            $client->get('key');
        } finally {
            // First attempt + deadline check happen before any sleep, so the
            // loop must end promptly even though the ServerBusy budget (60 s)
            // is nowhere near exhausted.
            $elapsedMs = (int) (microtime(true) * 1000) - $startMs;
            $this->assertLessThan(2000, $elapsedMs);
        }
    }

    public function testCreateRejectsNegativeRetryDeadlineOption(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['retryDeadlineMs'] must be >= 0");

        RawKvClient::create(['127.0.0.1:2379'], options: ['retryDeadlineMs' => -1]);
    }

    public function testCreateRejectsNonIntRetryDeadlineOption(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['retryDeadlineMs'] must be an int");

        RawKvClient::create(['127.0.0.1:2379'], options: ['retryDeadlineMs' => '1000']);
    }
}
