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
        /** @var TiKvException|false $firstError */
        $firstError = $regionErrors === []
            ? false
            : $regionErrors[min(array_keys($regionErrors))];
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
     * The error of the minimum region index — the first error the
     * sequential loop would have thrown.
     *
     * Call sites that fan out per-region RPCs but must preserve the
     * sequential "abort at the first failing region" exception semantics
     * rethrow this instead of the aggregate exception (issue #291). The
     * keys of the errors map are region indices filled in dispatch order,
     * but with the #291 dispatch-phase fan-out a LATER region can fail
     * while an earlier region's error is still only recorded — so the
     * first-inserted entry is not necessarily the lowest index. Selecting
     * the minimum key keeps the sequential contract exact.
     */
    public function getFirstRegionError(): TiKvException
    {
        $firstKey = null;
        foreach (array_keys($this->regionErrors) as $key) {
            if ($firstKey === null || $key < $firstKey) {
                $firstKey = $key;
            }
        }

        $first = $firstKey === null ? null : $this->regionErrors[$firstKey];
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
