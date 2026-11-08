<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\SlowLogOperation;
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

        $this->assertSame(10, $config->getThreshold(SlowLogOperation::Read));
        $this->assertSame(20, $config->getThreshold(SlowLogOperation::Write));
        $this->assertSame(30, $config->getThreshold(SlowLogOperation::BatchRead));
        $this->assertSame(40, $config->getThreshold(SlowLogOperation::BatchWrite));
        $this->assertSame(50, $config->getThreshold(SlowLogOperation::Scan));
        $this->assertSame(60, $config->getThreshold(SlowLogOperation::DeleteRange));
        $this->assertSame(70, $config->getThreshold(SlowLogOperation::Checksum));
    }

    /**
     * Every case is classified: `getThreshold()` has no `default` arm, so a
     * case the `match` forgot would throw `UnhandledMatchError` here
     * (issue #701).
     */
    public function testEveryOperationTypeIsClassified(): void
    {
        $config = new SlowLogConfig();

        foreach (SlowLogOperation::cases() as $operation) {
            $this->assertGreaterThanOrEqual(0, $config->getThreshold($operation));
        }
    }

    /**
     * The backed values are the documented `operation` context field, so a
     * rename is a documentation change, not a refactor (issue #701).
     */
    public function testOperationValuesAreTheDocumentedVocabulary(): void
    {
        $this->assertSame(
            ['read', 'write', 'batch_read', 'batch_write', 'scan', 'delete_range', 'checksum'],
            array_map(
                static fn(SlowLogOperation $operation): string => $operation->value,
                SlowLogOperation::cases(),
            ),
        );
    }

    public function testZeroThresholdDisablesLogging(): void
    {
        $config = new SlowLogConfig(
            readThresholdMs: 0,
            writeThresholdMs: 0,
        );

        $this->assertSame(0, $config->getThreshold(SlowLogOperation::Read));
        $this->assertSame(0, $config->getThreshold(SlowLogOperation::Write));
        // Non-zero thresholds still work
        $this->assertSame(500, $config->getThreshold(SlowLogOperation::BatchRead));
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
