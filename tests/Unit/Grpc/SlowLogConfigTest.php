<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use PHPUnit\Framework\TestCase;

class SlowLogConfigTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $config = new SlowLogConfig();

        $this->assertSame(100, $config->readThresholdMs);
        $this->assertSame(200, $config->writeThresholdMs);
        $this->assertSame(500, $config->batchReadThresholdMs);
        $this->assertSame(500, $config->batchWriteThresholdMs);
        $this->assertSame(1000, $config->scanThresholdMs);
        $this->assertSame(2000, $config->deleteRangeThresholdMs);
        $this->assertSame(3000, $config->checksumThresholdMs);
    }

    public function testCustomValues(): void
    {
        $config = new SlowLogConfig(
            readThresholdMs: 50,
            writeThresholdMs: 100,
            batchReadThresholdMs: 200,
            batchWriteThresholdMs: 300,
            scanThresholdMs: 500,
            deleteRangeThresholdMs: 1000,
            checksumThresholdMs: 2000,
        );

        $this->assertSame(50, $config->readThresholdMs);
        $this->assertSame(100, $config->writeThresholdMs);
        $this->assertSame(200, $config->batchReadThresholdMs);
        $this->assertSame(300, $config->batchWriteThresholdMs);
        $this->assertSame(500, $config->scanThresholdMs);
        $this->assertSame(1000, $config->deleteRangeThresholdMs);
        $this->assertSame(2000, $config->checksumThresholdMs);
    }

    public function testGetThresholdReturnsCorrectValues(): void
    {
        $config = new SlowLogConfig(
            readThresholdMs: 10,
            writeThresholdMs: 20,
            batchReadThresholdMs: 30,
            batchWriteThresholdMs: 40,
            scanThresholdMs: 50,
            deleteRangeThresholdMs: 60,
            checksumThresholdMs: 70,
        );

        $this->assertSame(10, $config->getThreshold('read'));
        $this->assertSame(20, $config->getThreshold('write'));
        $this->assertSame(30, $config->getThreshold('batch_read'));
        $this->assertSame(40, $config->getThreshold('batch_write'));
        $this->assertSame(50, $config->getThreshold('scan'));
        $this->assertSame(60, $config->getThreshold('delete_range'));
        $this->assertSame(70, $config->getThreshold('checksum'));
    }

    public function testGetThresholdReturnsZeroForUnknownOperation(): void
    {
        $config = new SlowLogConfig();

        $this->assertSame(0, $config->getThreshold('unknown'));
        $this->assertSame(0, $config->getThreshold(''));
    }

    public function testZeroThresholdDisablesLogging(): void
    {
        $config = new SlowLogConfig(
            readThresholdMs: 0,
            writeThresholdMs: 0,
        );

        $this->assertSame(0, $config->getThreshold('read'));
        $this->assertSame(0, $config->getThreshold('write'));
        // Non-zero thresholds still work
        $this->assertSame(500, $config->getThreshold('batch_read'));
    }

    // ========================================================================
    // Issue #276 (GRPC-17): `0` means "disabled" here — a documented
    // contract — but a negative threshold is a configuration error.
    // ========================================================================

    public function testANegativeThresholdIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('readThresholdMs must be greater than or equal to 0, got -1');

        new SlowLogConfig(readThresholdMs: -1);
    }
}
