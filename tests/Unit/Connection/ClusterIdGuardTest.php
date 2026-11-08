<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\ClusterIdGuard;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use PHPUnit\Framework\TestCase;

class ClusterIdGuardTest extends TestCase
{
    public function testExtractFromErrorReturnsReportedId(): void
    {
        $this->assertSame(
            7020123456789,
            ClusterIdGuard::extractFromError('mismatch cluster id, need 7020123456789 but got 0'),
        );
    }

    public function testExtractFromErrorIgnoresUnrelatedMessage(): void
    {
        $this->assertNull(ClusterIdGuard::extractFromError('connection refused'));
    }

    public function testExtractFromErrorRejectsZeroId(): void
    {
        $this->assertNull(
            ClusterIdGuard::extractFromError('mismatch cluster id, need 0 but got 0'),
        );
    }

    public function testZeroHeaderIsMissingWhenExpectedIdIsConfigured(): void
    {
        $guard = new ClusterIdGuard(
            static fn (): ?int => null,
            static function (int|string $id): void {
            },
            expectedClusterId: 12345,
        );

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('Cluster ID missing: expected 12345, response carried none');

        $guard->check(0);
    }

    public function testCheckAdoptsFirstPositiveIdOnce(): void
    {
        $clusterId = null;
        $setCalls = [];
        $guard = new ClusterIdGuard(
            static function () use (&$clusterId): ?int {
                return $clusterId;
            },
            static function (int|string $id) use (&$clusterId, &$setCalls): void {
                $clusterId = $id;
                $setCalls[] = $id;
            },
        );

        $guard->check(100);
        $guard->check(100);

        $this->assertSame(100, $clusterId);
        $this->assertSame([100], $setCalls);
    }
}
