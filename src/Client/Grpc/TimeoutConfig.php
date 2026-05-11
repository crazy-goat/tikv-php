<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

final readonly class TimeoutConfig
{
    public function __construct(
        public int $readTimeoutMs = 5000,
        public int $writeTimeoutMs = 5000,
        public int $batchReadTimeoutMs = 10000,
        public int $batchWriteTimeoutMs = 10000,
        public int $scanTimeoutMs = 20000,
        public int $deleteRangeTimeoutMs = 30000,
    ) {
    }
}
