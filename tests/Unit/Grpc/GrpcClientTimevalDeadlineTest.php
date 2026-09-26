<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use Grpc\Timeval;
use PHPUnit\Framework\TestCase;

/**
 * The `Grpc\Timeval` a call is actually created with (issue #260).
 *
 * This is the object-level half of {@see GrpcClientDeadlineTest}: it builds
 * a real `Grpc\Timeval`, so it needs ext-grpc and lives in the `Grpc` suite
 * (`phpunit.xml`). Nothing here opens a channel — the deadline is computed
 * before the channel is touched, and the assertion is on the `Timeval` the
 * private `deadline()` seam hands to `new Call(...)`.
 *
 * The regression is a *comparison*, not a value: before the fix `deadline()`
 * was the inline expression and a `null` timeout produced
 * `Timeval::infFuture()`.
 */
class GrpcClientTimevalDeadlineTest extends TestCase
{
    use GrpcExtensionGate;

    /** Slack allowed between the expected and the produced deadline. */
    private const TOLERANCE_US = 2_000_000;

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testNullTimeoutNoLongerProducesTheInfiniteDeadline(): void
    {
        $deadline = $this->deadlineOf(null);

        self::assertSame(
            -1,
            Timeval::compare($deadline, Timeval::infFuture()),
            'A null timeout must not produce Timeval::infFuture() again — that '
            . 'armed no deadline at all and a blocking startBatch() could not '
            . 'be interrupted (issue #260).',
        );
    }

    public function testNullTimeoutProducesTheLibraryDefaultFromNow(): void
    {
        $expected = Timeval::now()->add(new Timeval(GrpcClient::DEFAULT_TIMEOUT_MS * 1000));

        self::assertTrue(
            Timeval::similar($this->deadlineOf(null), $expected, new Timeval(self::TOLERANCE_US)),
            'The default deadline must be DEFAULT_TIMEOUT_MS from now, not some '
            . 'other finite value.',
        );
    }

    public function testAnExplicitTimeoutIsHonoured(): void
    {
        $expected = Timeval::now()->add(new Timeval(2_500_000));

        self::assertTrue(
            Timeval::similar($this->deadlineOf(2500), $expected, new Timeval(self::TOLERANCE_US)),
        );
    }

    public function testZeroIsTheOnlyValueThatOptsOutOfADeadline(): void
    {
        self::assertSame(
            0,
            Timeval::compare($this->deadlineOf(0), Timeval::infFuture()),
            '0 must remain the explicit, documented no-deadline sentinel.',
        );
    }

    private function deadlineOf(?int $timeoutMs): Timeval
    {
        $method = new \ReflectionMethod(GrpcClient::class, 'deadline');

        $value = $method->invoke(new GrpcClient(), $timeoutMs);
        self::assertInstanceOf(Timeval::class, $value);

        return $value;
    }
}
