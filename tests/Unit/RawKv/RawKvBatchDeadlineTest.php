<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Every batch sub-request of the `RawKvBatch` fan-out carries a finite,
 * strictly positive deadline (issue #184).
 *
 * The fan-out is the one request path in the client that does NOT go through
 * `GrpcClient::call()`: `executeBatchGetForRegionAsync()` and its two write
 * siblings hand-roll `new \Grpc\Call(...)`, so the 30 s backstop #260 added for
 * a `null` timeout does not protect them. They derived the deadline inline as
 * `$timeout !== null ? now + $timeout : Timeval::infFuture()` over a
 * `timeoutMs(): ?int` helper — a `?:` whose *condition* was a nullability the
 * type system could not connect to the deadline it produced, and whose false
 * arm was the one deadline the gRPC C core never expires.
 *
 * The `infFuture()` arm was not reachable while the `match` knew exactly the
 * two operation types its four call sites use, which is precisely why nothing
 * caught it: the bug class is a `null` that means "forever" by accident, and
 * an accidental one is one `match` arm from a live outage. So this pins the
 * *shape* rather than a value:
 *
 *  - {@see testTheTimeoutHelperCannotReturnNull()} — the helper's return type
 *    is a non-nullable `int`, so the `?:` has nothing to switch on;
 *  - {@see testAnUnknownOperationTypeCannotSilentlyMeanNoDeadline()} — the
 *    `default => null` arm is gone, so a new operation type must be decided;
 *  - {@see testEveryConfiguredTimeoutResolvesToAPositiveDeadline()} — including
 *    the `0` that `TimeoutConfig` documents as "no deadline", which used to
 *    reach `new Timeval(0)` (a deadline already in the past) on the hand-rolled
 *    path and `Timeval::infFuture()` on the API V2 `callAsync()` path.
 *
 * The resulting `Timeval` object needs ext-grpc and is asserted in
 * {@see \CrazyGoat\TiKV\Tests\Unit\Grpc\RawKvBatchTimevalDeadlineTest} (the
 * `Grpc` suite); `NoInfiniteDeadlineGuardTest` keeps `Timeval::infFuture()`
 * out of every other site in `src/Client`. This file stays in the `Unit` suite
 * because it mocks no `\Grpc\Call` — it drives the private timeout seam, which
 * is the same reflection-over-a-private-seam pattern as
 * `GrpcClientDeadlineTest`.
 */
class RawKvBatchDeadlineTest extends TestCase
{
    public function testTheTimeoutHelperCannotReturnNull(): void
    {
        $returnType = (new \ReflectionMethod(RawKvBatch::class, 'timeoutMs'))->getReturnType();

        self::assertNotNull($returnType, 'RawKvBatch::timeoutMs() must declare a return type');
        self::assertSame('int', (string) $returnType, 'a nullable return type is what the '
            . '`$timeout !== null ? … : Timeval::infFuture()` ternary switched on (issue #184)');
        self::assertFalse($returnType->allowsNull());
    }

    public function testAnUnknownOperationTypeCannotSilentlyMeanNoDeadline(): void
    {
        // The old `default => null` arm answered this with null, which the
        // caller turned into Timeval::infFuture(). Failing loudly is the
        // alternative: a new operation type has to be classified.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown batch operation type "batch_delete"');

        $this->timeoutMs('batch_delete', new TimeoutConfig());
    }

    /**
     * @return array<string, array{int}>
     */
    public static function provideNonPositiveTimeoutConfigurations(): array
    {
        return [
            // TimeoutConfig documents 0 as "no deadline" for the store fields,
            // and GrpcClient honours that sentinel — which is exactly why
            // RawKvBatch must not forward it: it does not go through
            // GrpcClient::call().
            'zero' => [0],
            'negative' => [-1],
        ];
    }

    #[DataProvider('provideNonPositiveTimeoutConfigurations')]
    public function testEveryConfiguredTimeoutResolvesToAPositiveDeadline(int $configuredMs): void
    {
        $config = new TimeoutConfig(
            batchReadTimeoutMs: $configuredMs,
            batchWriteTimeoutMs: $configuredMs,
        );

        $read = $this->timeoutMs('batch_read', $config);
        $write = $this->timeoutMs('batch_write', $config);

        self::assertSame(GrpcClient::DEFAULT_TIMEOUT_MS, $read);
        self::assertSame(GrpcClient::DEFAULT_TIMEOUT_MS, $write);
        self::assertGreaterThan(
            0,
            $read,
            'A non-positive configured timeout must fall back to the conservative library '
            . 'default, never to an unbounded (or already expired) deadline (issue #184).',
        );
    }

    public function testAConfiguredTimeoutIsUsedUnchanged(): void
    {
        $config = new TimeoutConfig(batchReadTimeoutMs: 4321, batchWriteTimeoutMs: 8765);

        self::assertSame(4321, $this->timeoutMs('batch_read', $config));
        self::assertSame(8765, $this->timeoutMs('batch_write', $config));
    }

    public function testTheDefaultConfigurationIsAlreadyPositive(): void
    {
        $config = new TimeoutConfig();

        self::assertSame($config->batchReadTimeoutMs, $this->timeoutMs('batch_read', $config));
        self::assertSame($config->batchWriteTimeoutMs, $this->timeoutMs('batch_write', $config));
    }

    /**
     * The single derivation seam, asserted structurally: it takes a plain
     * `int` and returns the `Timeval` type, so the three hand-rolled
     * `new Call(...)` sites have no other way to build one and no way to pass
     * it something that means "no deadline".
     */
    public function testDeadlinesAreDerivedThroughOneNonNullableSeam(): void
    {
        $method = new \ReflectionMethod(RawKvBatch::class, 'deadlineFor');
        $parameter = $method->getParameters()[0] ?? null;
        $parameterType = $parameter?->getType();
        $returnType = $method->getReturnType();

        self::assertNotNull($parameter, 'deadlineFor() must take the timeout it converts');
        self::assertNotNull($parameterType);
        self::assertSame('int', (string) $parameterType);
        self::assertFalse($parameterType->allowsNull());
        self::assertNotNull($returnType);
        self::assertSame(\Grpc\Timeval::class, (string) $returnType);
    }

    private function timeoutMs(string $operationType, TimeoutConfig $timeoutConfig): mixed
    {
        $batch = new RawKvBatch(
            $this->createMock(GrpcClientInterface::class),
            new RegionResolver(
                $this->createMock(PdClientInterface::class),
                $this->createMock(RegionCacheInterface::class),
            ),
            $timeoutConfig,
            new NullLogger(),
        );

        $value = (new \ReflectionMethod(RawKvBatch::class, 'timeoutMs'))->invoke($batch, $operationType);
        self::assertIsInt($value, 'timeoutMs() must never answer null — see issue #184');

        return $value;
    }
}
