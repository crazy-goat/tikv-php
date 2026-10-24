<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

/**
 * Configuration for slow operation logging.
 *
 * Thresholds are in milliseconds. A threshold of 0 disables slow logging
 * for that operation type — a documented contract of this class, unlike the
 * gRPC deadlines, where a non-positive value is a configuration error
 * (issue #276, GRPC-17). A negative threshold is rejected here, at
 * construction, and throws `InvalidArgumentException`.
 *
 * @see https://github.com/crazy-goat/tikv-php/issues/31
 */
final readonly class SlowLogConfig
{
    public function __construct(
        public int $readThresholdMs = 100,
        public int $writeThresholdMs = 200,
        public int $batchReadThresholdMs = 500,
        public int $batchWriteThresholdMs = 500,
        public int $scanThresholdMs = 1000,
        public int $deleteRangeThresholdMs = 2000,
        public int $checksumThresholdMs = 3000,
    ) {
        $thresholds = [
            'readThresholdMs' => $this->readThresholdMs,
            'writeThresholdMs' => $this->writeThresholdMs,
            'batchReadThresholdMs' => $this->batchReadThresholdMs,
            'batchWriteThresholdMs' => $this->batchWriteThresholdMs,
            'scanThresholdMs' => $this->scanThresholdMs,
            'deleteRangeThresholdMs' => $this->deleteRangeThresholdMs,
            'checksumThresholdMs' => $this->checksumThresholdMs,
        ];

        foreach ($thresholds as $name => $value) {
            if ($value < 0) {
                throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                    sprintf('%s must be greater than or equal to 0, got %d', $name, $value),
                );
            }
        }
    }

    /**
     * Return the threshold for a given operation type (in milliseconds).
     *
     * Operation types: 'read', 'write', 'batch_read', 'batch_write', 'scan',
     * 'delete_range', 'checksum'.
     *
     * Returns 0 when the operation type is unknown (disables logging).
     */
    public function getThreshold(string $operation): int
    {
        return match ($operation) {
            'read' => $this->readThresholdMs,
            'write' => $this->writeThresholdMs,
            'batch_read' => $this->batchReadThresholdMs,
            'batch_write' => $this->batchWriteThresholdMs,
            'scan' => $this->scanThresholdMs,
            'delete_range' => $this->deleteRangeThresholdMs,
            'checksum' => $this->checksumThresholdMs,
            default => 0,
        };
    }
}
