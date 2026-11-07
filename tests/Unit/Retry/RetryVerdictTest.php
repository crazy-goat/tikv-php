<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Retry;

use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use PHPUnit\Framework\TestCase;

/**
 * Issue #534: a classifier result must keep "retry", "fatal" and "no opinion"
 * apart, so a fatal verdict is no longer expressed by throwing from inside the
 * executor's catch block.
 */
class RetryVerdictTest extends TestCase
{
    public function testRetryCarriesItsBackoffType(): void
    {
        $verdict = RetryVerdict::retry(BackoffType::ServerBusy);

        self::assertTrue($verdict->isRetry());
        self::assertFalse($verdict->isFatal());
        self::assertFalse($verdict->isPass());
        self::assertSame(BackoffType::ServerBusy, $verdict->backoffType());
    }

    public function testFatalIsNeitherRetryNorPass(): void
    {
        $verdict = RetryVerdict::fatal();

        self::assertFalse($verdict->isRetry());
        self::assertTrue($verdict->isFatal());
        self::assertFalse($verdict->isPass());
    }

    public function testPassIsNeitherRetryNorFatal(): void
    {
        $verdict = RetryVerdict::pass();

        self::assertFalse($verdict->isRetry());
        self::assertFalse($verdict->isFatal());
        self::assertTrue($verdict->isPass());
    }

    public function testFatalVerdictHasNoBackoffType(): void
    {
        $this->expectException(\LogicException::class);

        RetryVerdict::fatal()->backoffType();
    }

    public function testPassVerdictHasNoBackoffType(): void
    {
        $this->expectException(\LogicException::class);

        RetryVerdict::pass()->backoffType();
    }
}
