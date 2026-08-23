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
    /** Tolerance for wall-clock assertions: covers usleep overshoot, mock
     * overhead and millisecond rounding of the elapsed-time measurement. */
    private const ELAPSED_TOLERANCE_MS = 2500;

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
        // ServerBusy sleeps ~1000-2000ms per attempt. Keep the DEFAULT
        // ServerBusy budget (60s): it cannot bind within a few attempts, so
        // the tiny wall-clock deadline below is what actually ends the loop.
        // A budget of 0 would NOT do — budgets have no "disabled" semantics,
        // 0 exhausts on the first charge and would bypass the deadline.
        $deadlineMs = 50;
        $client = new RawKvClient(
            $this->pdClient,
            $this->grpc,
            $this->regionCache,
            maxBackoffMs: 0,              // disable the non-ServerBusy budget
            retryDeadlineMs: $deadlineMs, // the binding bound under test
        );
        $this->alwaysServerBusy();

        $startMs = (int) (microtime(true) * 1000);

        $this->expectException(RetryBudgetExhaustedException::class);
        $this->expectExceptionMessage('Retry deadline');

        try {
            $client->get('key');
        } finally {
            // Deadline is checked before each attempt; the last sleep can
            // overshoot by up to one ServerBusy sleep (~2s at these attempt
            // counts), hence the tolerance.
            $elapsedMs = (int) (microtime(true) * 1000) - $startMs;
            $this->assertLessThanOrEqual($deadlineMs + self::ELAPSED_TOLERANCE_MS, $elapsedMs);
        }
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
            // The deadline is checked BEFORE each attempt, so the first
            // ServerBusy backoff (equal-jitter 1000-2000ms) sleeps before
            // the deadline check that ends the loop — elapsed is ~1-2s.
            // Bound = deadline + one ServerBusy sleep cap + tolerance for
            // usleep overshoot and ms rounding (flake-proof).
            $elapsedMs = (int) (microtime(true) * 1000) - $startMs;
            $this->assertLessThan(30 + self::ELAPSED_TOLERANCE_MS, $elapsedMs);
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
