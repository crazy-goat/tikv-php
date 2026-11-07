<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch;

/**
 * Identifies one unit of work in a batch fan-out.
 *
 * A TiKV region can produce several sub-batches (its keys are split into
 * request-sized chunks), so a region ID alone cannot name a call: two
 * failures from the same region would collide. `$subBatchIndex` is the
 * zero-based position of the sub-batch inside its region, in dispatch order.
 *
 * `BatchAsyncExecutor` logs the two fields separately and
 * {@see \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException}
 * exposes them through {@see \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException::getFailedRegionIds()}
 * and {@see \CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException::getFailedCallIds()}
 * (issue #285, GRPC-26).
 */
final readonly class BatchCallId implements \Stringable
{
    public function __construct(
        public int $regionId,
        public int $subBatchIndex = 0,
    ) {
    }

    public function __toString(): string
    {
        return sprintf('region=%d,batch=%d', $this->regionId, $this->subBatchIndex);
    }
}
