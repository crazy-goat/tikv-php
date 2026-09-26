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
use Grpc\Timeval;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * The `Grpc\Timeval` the `RawKvBatch` fan-out is actually created with
 * (issue #184).
 *
 * This is the object-level half of {@see RawKvBatchDeadlineTest}: it builds a
 * real `Grpc\Timeval`, so it needs ext-grpc and lives in the `Grpc` suite
 * (`phpunit.xml`, which excludes it from `Unit` for the same reason
 * `GrpcClientTimevalDeadlineTest` is excluded). Nothing here opens a channel —
 * the deadline is computed before the channel is touched, and the assertion is
 * on the object the private `deadlineFor()` seam hands to `new Call(...)`.
 *
 * The regression is a *comparison*, not a value. Before the fix the three
 * `execute*ForRegionAsync()` methods built the deadline inline as
 * `$timeout !== null ? now + $timeout : Timeval::infFuture()`; the `0` that
 * `TimeoutConfig` documents as "no deadline" then took the *finite* arm and
 * produced a deadline already in the past, while the API V2 `callAsync()`
 * branch forwarded the same `0` to `GrpcClient::deadline()`, which reads it as
 * the explicit opt-out and produced `infFuture()`. One seam now derives it,
 * from a value that cannot be zero.
 */
class RawKvBatchTimevalDeadlineTest extends TestCase
{
    use GrpcExtensionGate;

    /** Slack allowed between the expected and the produced deadline. */
    private const TOLERANCE_US = 2_000_000;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testTheDeadlineIsNeverTheInfiniteOne(): void
    {
        $deadline = $this->deadlineFor($this->timeoutMsOf('batch_read', new TimeoutConfig()));

        self::assertSame(
            -1,
            Timeval::compare($deadline, Timeval::infFuture()),
            'The batch fan-out hand-rolls new Call(...) and never reaches '
            . 'GrpcClient::deadline()\'s 30 s backstop, so an unbounded deadline here is not '
            . 'covered by anything else (issue #184).',
        );
    }

    public function testAConfiguredTimeoutIsHonoured(): void
    {
        $expected = Timeval::now()->add(new Timeval(2_500_000));

        self::assertTrue(
            Timeval::similar($this->deadlineFor(2500), $expected, new Timeval(self::TOLERANCE_US)),
        );
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
        $resolvedMs = $this->timeoutMsOf('batch_read', $config);
        $expected = Timeval::now()->add(new Timeval(GrpcClient::DEFAULT_TIMEOUT_MS * 1000));

        self::assertSame(GrpcClient::DEFAULT_TIMEOUT_MS, $resolvedMs);
        self::assertSame(
            -1,
            Timeval::compare($this->deadlineFor($resolvedMs), Timeval::infFuture()),
        );
        self::assertTrue(
            Timeval::similar($this->deadlineFor($resolvedMs), $expected, new Timeval(self::TOLERANCE_US)),
        );
    }

    private function deadlineFor(int $timeoutMs): Timeval
    {
        $value = (new \ReflectionMethod(RawKvBatch::class, 'deadlineFor'))
            ->invoke($this->batch(new TimeoutConfig()), $timeoutMs);
        self::assertInstanceOf(Timeval::class, $value);

        return $value;
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
