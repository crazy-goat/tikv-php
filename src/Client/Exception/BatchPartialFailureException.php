<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

final class BatchPartialFailureException extends TiKvException
{
    /**
     * @param array<int, TiKvException> $regionErrors regionId => exception
     * @param int $totalRegions Total number of regions in batch
     */
    public function __construct(
        private readonly array $regionErrors,
        private readonly int $totalRegions,
    ) {
        $firstError = reset($regionErrors);
        parent::__construct(
            sprintf(
                'Batch operation partially failed: %d of %d regions failed. First error: %s',
                count($regionErrors),
                $totalRegions,
                $firstError instanceof TiKvException ? $firstError->getMessage() : 'Unknown',
            )
        );
    }

    /**
     * The first region error in dispatch/wait order.
     *
     * Call sites that fan out per-region RPCs but must preserve the
     * sequential "abort at the first failing region" exception semantics
     * rethrow this instead of the aggregate exception (issue #291): the
     * errors map is filled in dispatch order, so the first entry is exactly
     * the exception the sequential loop would have thrown.
     */
    public function getFirstRegionError(): TiKvException
    {
        $errors = array_values($this->regionErrors);
        $first = $errors[0] ?? null;
        if ($first instanceof TiKvException) {
            return $first;
        }

        return $this;
    }

    /**
     * @return array<int, TiKvException>
     */
    public function getRegionErrors(): array
    {
        return $this->regionErrors;
    }

    public function getTotalRegions(): int
    {
        return $this->totalRegions;
    }
}
