<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * What deadline a `GrpcClient` call actually gets when the call site passes
 * no timeout (issue #260).
 *
 * Before the fix, `call()` built the deadline as
 * `Timeval::now()->add(...)` for a positive `$timeoutMs` and
 * `Timeval::infFuture()` for everything else, so `null` — which is what every
 * PD, TSO and lock-resolution call site passed — armed **no** deadline at all.
 * `Grpc\Call::startBatch()` blocks inside the C extension, where PHP's
 * `max_execution_time` does not reach, so a half-open connection pinned the
 * worker until the kernel gave up (minutes, or never).
 *
 * `null` now means "the library default". The explicit no-deadline sentinel
 * `0` was removed in issue #276 (GRPC-17): it is also what an unset
 * environment variable coerces to, so it turned a configuration mistake into
 * a call that never expires. A non-positive timeout is now rejected.
 * Asserted here through the private `resolveTimeoutMs()` seam, which is
 * pure PHP: the `Timeval` it feeds needs ext-grpc, so the object-level
 * assertions live in {@see GrpcClientTimevalDeadlineTest} (the `Grpc`
 * suite).
 */
class GrpcClientDeadlineTest extends TestCase
{
    public function testTheLibraryDefaultIsFiniteAndPinned(): void
    {
        // A value, not just a constant: the AC is that a call with no
        // timeout is *bounded*, and 30 s is the conservative default the
        // issue names.
        self::assertSame(30000, GrpcClient::DEFAULT_TIMEOUT_MS);
    }

    public function testNullTimeoutResolvesToTheFiniteLibraryDefault(): void
    {
        self::assertSame(GrpcClient::DEFAULT_TIMEOUT_MS, $this->resolveTimeoutMs(null));
    }

    public function testAnExplicitTimeoutIsPreserved(): void
    {
        self::assertSame(1234, $this->resolveTimeoutMs(1234));
        self::assertSame(1, $this->resolveTimeoutMs(1));
    }

    /**
     * A non-positive timeout never reaches `Timeval::infFuture()`: the
     * resolution rejects it (issue #276, GRPC-17). `0` used to be the
     * documented "no deadline" opt-out, but it is also what an unset
     * environment variable coerces to, so the opt-out turned a configuration
     * mistake into a call that never expires.
     */
    #[DataProvider('provideNonPositiveTimeouts')]
    public function testANonPositiveTimeoutIsRejected(int $timeoutMs): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf('timeoutMs must be greater than 0, got %d', $timeoutMs));

        $this->resolveTimeoutMs($timeoutMs);
    }

    /**
     * The deadline seam refuses a non-positive timeout on its own, even when
     * it is called with a raw value (issue #276, GRPC-17): a negative value
     * would build a deadline already in the past, and ext-grpc does not
     * reject it.
     */
    #[DataProvider('provideNonPositiveTimeouts')]
    public function testTheDeadlineSeamRefusesANonPositiveTimeout(int $timeoutMs): void
    {
        $method = new \ReflectionMethod(GrpcClient::class, 'deadline');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf('timeoutMs must be greater than 0, got %d', $timeoutMs));

        $method->invoke(new GrpcClient(), $timeoutMs);
    }

    /**
     * @return array<string, array{int}>
     */
    public static function provideNonPositiveTimeouts(): array
    {
        return [
            'zero' => [0],
            'negative' => [-1],
        ];
    }

    private function resolveTimeoutMs(?int $timeoutMs): int
    {
        $method = new \ReflectionMethod(GrpcClient::class, 'resolveTimeoutMs');

        $value = $method->invoke(new GrpcClient(), $timeoutMs);
        self::assertIsInt($value);

        return $value;
    }
}
