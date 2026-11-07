<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Exception;

use CrazyGoat\TiKV\Client\Exception\IngestPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use PHPUnit\Framework\TestCase;

class IngestPartialFailureExceptionTest extends TestCase
{
    public function testMessageBoundsTheRegionIdListButTheAccessorKeepsItWhole(): void
    {
        $applied = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

        $exception = new IngestPartialFailureException(
            $applied,
            99,
            new TiKvException('boom'),
        );

        self::assertSame($applied, $exception->getAppliedRegionIds());
        self::assertSame(99, $exception->getFailedRegionId());
        self::assertStringContainsString('12 region(s) ingested', $exception->getMessage());
        self::assertStringContainsString(
            '(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ... (2 more))',
            $exception->getMessage(),
        );
        self::assertStringNotContainsString('11,', $exception->getMessage());
        self::assertStringNotContainsString('12)', $exception->getMessage());
    }
}
