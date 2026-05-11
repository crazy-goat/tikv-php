<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use PHPUnit\Framework\TestCase;

class TimeoutConfigTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $config = new TimeoutConfig();

        $this->assertSame(5000, $config->readTimeoutMs);
        $this->assertSame(5000, $config->writeTimeoutMs);
        $this->assertSame(10000, $config->batchReadTimeoutMs);
        $this->assertSame(10000, $config->batchWriteTimeoutMs);
        $this->assertSame(20000, $config->scanTimeoutMs);
        $this->assertSame(30000, $config->deleteRangeTimeoutMs);
    }

    public function testCustomValues(): void
    {
        $config = new TimeoutConfig(
            readTimeoutMs: 1000,
            writeTimeoutMs: 2000,
            batchReadTimeoutMs: 3000,
            batchWriteTimeoutMs: 4000,
            scanTimeoutMs: 5000,
            deleteRangeTimeoutMs: 6000,
        );

        $this->assertSame(1000, $config->readTimeoutMs);
        $this->assertSame(2000, $config->writeTimeoutMs);
        $this->assertSame(3000, $config->batchReadTimeoutMs);
        $this->assertSame(4000, $config->batchWriteTimeoutMs);
        $this->assertSame(5000, $config->scanTimeoutMs);
        $this->assertSame(6000, $config->deleteRangeTimeoutMs);
    }

    public function testPartialCustomValues(): void
    {
        $config = new TimeoutConfig(
            readTimeoutMs: 1000,
            scanTimeoutMs: 50000,
        );

        $this->assertSame(1000, $config->readTimeoutMs);
        $this->assertSame(5000, $config->writeTimeoutMs);
        $this->assertSame(50000, $config->scanTimeoutMs);
    }
}
