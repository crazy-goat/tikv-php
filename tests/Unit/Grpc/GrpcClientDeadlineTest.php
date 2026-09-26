<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
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
 * `null` now means "the library default" and the explicit no-deadline
 * sentinel is the non-positive value `0` — the same spelling the rest of the
 * library already uses for "disabled" (`TimeoutConfig::$batchDeadlineMs`).
 * Asserted here through the private `resolveTimeoutMs()` seam, which is pure
 * PHP: the `Timeval` it feeds needs ext-grpc, so the object-level assertions
 * live in {@see GrpcClientTimevalDeadlineTest} (the `Grpc` suite).
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

    public function testZeroIsTheExplicitNoDeadlineSentinel(): void
    {
        // Must survive resolution untouched — it is the documented opt-out,
        // and any coercion to the default here would silently bound a call
        // its caller deliberately left unbounded.
        self::assertSame(0, $this->resolveTimeoutMs(0));
    }

    private function resolveTimeoutMs(?int $timeoutMs): int
    {
        $method = new \ReflectionMethod(GrpcClient::class, 'resolveTimeoutMs');

        $value = $method->invoke(new GrpcClient(), $timeoutMs);
        self::assertIsInt($value);

        return $value;
    }
}
