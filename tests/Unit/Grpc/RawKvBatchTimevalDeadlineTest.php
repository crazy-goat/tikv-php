<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * The batch timeout `RawKvBatch` hands to `GrpcClient::callAsync()` (issue #184).
 *
 * Since #273 the fan-out no longer builds a `Grpc\Call` or a deadline itself:
 * it forwards the resolved millisecond value to `callAsync()`, which derives
 * the `Timeval`. What is left to pin is that the forwarded value is never
 * `0`, which `callAsync()` would read as the explicit "no deadline" opt-out.
 */
class RawKvBatchTimevalDeadlineTest extends TestCase
{
    use GrpcExtensionGate;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    /**
     * `TimeoutConfig::batchReadTimeoutMs = 0` is documented as "no deadline",
     * and `GrpcClient` honours that sentinel — which is why `RawKvBatch`
     * resolves a non-positive value instead of forwarding it. Before the fix
     * the value reached `new Timeval(0)` on the hand-rolled path (a deadline
     * already in the past, so every sub-batch failed with
     * DEADLINE_EXCEEDED) and `Timeval::infFuture()` on the API V2 path.
     */
    public function testAZeroConfiguredTimeoutBecomesTheLibraryDefault(): void
    {
        $config = new TimeoutConfig(batchReadTimeoutMs: 0, batchWriteTimeoutMs: 0);

        self::assertSame(GrpcClient::DEFAULT_TIMEOUT_MS, $this->timeoutMsOf('batch_read', $config));
        self::assertSame(GrpcClient::DEFAULT_TIMEOUT_MS, $this->timeoutMsOf('batch_write', $config));
    }

    private function timeoutMsOf(string $operationType, TimeoutConfig $timeoutConfig): int
    {
        $value = (new \ReflectionMethod(RawKvBatch::class, 'timeoutMs'))
            ->invoke($this->batch($timeoutConfig), $operationType);
        self::assertIsInt($value);

        return $value;
    }

    private function batch(TimeoutConfig $timeoutConfig): RawKvBatch
    {
        return new RawKvBatch(
            $this->createMock(GrpcClientInterface::class),
            new RegionResolver(
                $this->createMock(PdClientInterface::class),
                $this->createMock(RegionCacheInterface::class),
            ),
            $timeoutConfig,
            new NullLogger(),
        );
    }
}
