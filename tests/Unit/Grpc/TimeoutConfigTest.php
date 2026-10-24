<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use PHPUnit\Framework\Attributes\DataProvider;
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

    /**
     * Issue #260: the metadata-plane deadlines have finite defaults. Before
     * the fix TimeoutConfig had no PD/TSO/lock-resolution field at all, so
     * those RPCs fell through to GrpcClient's `null` branch and were given
     * an infinite deadline.
     */
    public function testPdTsoAndLockResolveDefaultsAreFinite(): void
    {
        $config = new TimeoutConfig();

        $this->assertSame(3000, $config->pdTimeoutMs);
        $this->assertSame(3000, $config->tsoTimeoutMs);
        $this->assertSame(5000, $config->lockResolveTimeoutMs);

        // Pinned values, not only "greater than zero": the issue names 3 s for
        // PD and TSO (the default client-go's tikv/pd/client uses) and 5 s for
        // lock resolution, which can wait on a TSO round trip and a
        // transaction that is still being decided.
        $this->assertSame(3000, TimeoutConfig::DEFAULT_PD_TIMEOUT_MS);
        $this->assertSame(3000, TimeoutConfig::DEFAULT_TSO_TIMEOUT_MS);
        $this->assertSame(5000, TimeoutConfig::DEFAULT_LOCK_RESOLVE_TIMEOUT_MS);
    }

    public function testMetadataDeadlinesAreConfigurableIndependentlyOfTheStoreOnes(): void
    {
        $config = new TimeoutConfig(
            readTimeoutMs: 1,
            writeTimeoutMs: 2,
            batchReadTimeoutMs: 3,
            batchWriteTimeoutMs: 4,
            scanTimeoutMs: 5,
            deleteRangeTimeoutMs: 6,
            checksumTimeoutMs: 7,
            ingestTimeoutMs: 8,
            batchDeadlineMs: 9,
            pdTimeoutMs: 10,
            tsoTimeoutMs: 11,
            lockResolveTimeoutMs: 12,
        );

        $this->assertSame(1, $config->readTimeoutMs);
        $this->assertSame(2, $config->writeTimeoutMs);
        $this->assertSame(10, $config->pdTimeoutMs);
        $this->assertSame(11, $config->tsoTimeoutMs);
        $this->assertSame(12, $config->lockResolveTimeoutMs);
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

    // ========================================================================
    // Issue #276 (GRPC-17): a non-positive timeout is a configuration error.
    // ========================================================================

    /**
     * @return array<string, array{string, int}>
     */
    public static function provideNonPositiveTimeouts(): array
    {
        return [
            'readTimeoutMs' => ['readTimeoutMs', 0],
            'writeTimeoutMs' => ['writeTimeoutMs', -1],
            'batchReadTimeoutMs' => ['batchReadTimeoutMs', 0],
            'batchWriteTimeoutMs' => ['batchWriteTimeoutMs', 0],
            'scanTimeoutMs' => ['scanTimeoutMs', 0],
            'deleteRangeTimeoutMs' => ['deleteRangeTimeoutMs', 0],
            'checksumTimeoutMs' => ['checksumTimeoutMs', 0],
            'ingestTimeoutMs' => ['ingestTimeoutMs', 0],
            'pdTimeoutMs' => ['pdTimeoutMs', 0],
            'tsoTimeoutMs' => ['tsoTimeoutMs', 0],
            'lockResolveTimeoutMs' => ['lockResolveTimeoutMs', 0],
        ];
    }

    #[DataProvider('provideNonPositiveTimeouts')]
    public function testANonPositiveTimeoutIsRejected(string $field, int $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf('%s must be greater than 0, got %d', $field, $value));

        new TimeoutConfig(...[$field => $value]);
    }

    /**
     * `batchDeadlineMs` is the one field that keeps `0` as "disabled": it
     * sizes the overall batch deadline, not a gRPC call.
     */
    public function testZeroBatchDeadlineIsStillAllowed(): void
    {
        $config = new TimeoutConfig(batchDeadlineMs: 0);

        $this->assertSame(0, $config->batchDeadlineMs);
    }

    public function testANegativeBatchDeadlineIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchDeadlineMs must be greater than or equal to 0, got -1');

        new TimeoutConfig(batchDeadlineMs: -1);
    }
}
