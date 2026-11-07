<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

use CrazyGoat\TiKV\Client\Batch\BatchCallId;

final class BatchPartialFailureException extends TiKvException
{
    /**
     * @param array<int, TiKvException> $regionErrors call index => exception
     * @param int $totalRegions Total number of calls in the batch
     * @param array<int, mixed> $partialResults Successful region results keyed by call index
     * @param array<int, BatchCallId> $callIds call index => fan-out descriptor (issue #285)
     */
    public function __construct(
        private readonly array $regionErrors,
        private readonly int $totalRegions,
        private readonly array $partialResults = [],
        private readonly array $callIds = [],
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
     * The error of the minimum call index — the first error the
     * sequential loop would have thrown.
     *
     * Call sites that fan out per-region RPCs but must preserve the
     * sequential "abort at the first failing region" exception semantics
     * rethrow this instead of the aggregate exception (issue #291). The
     * keys of the errors map are fan-out call indices filled in dispatch
     * order, but with the #291 dispatch-phase fan-out a LATER region can
     * fail while an earlier region's error is still only recorded — so the
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
     * The failed calls keyed by the fan-out call index.
     *
     * Every failed slot is present here, whether or not the call site
     * supplied a descriptor; use {@see self::getFailedCallIds()} for the
     * region/sub-batch identity of a slot.
     *
     * @return array<int, TiKvException>
     */
    public function getRegionErrors(): array
    {
        return $this->regionErrors;
    }

    /**
     * The real TiKV region IDs of the failed calls, unique and ascending.
     *
     * This is the accessor to use for correlation with `pd-ctl`, region
     * metrics and targeted retries. `getRegionErrors()` stays keyed by the
     * fan-out call index for backward compatibility; use
     * {@see self::getFailedCallIds()} to map an index to its region and
     * sub-batch.
     *
     * Empty when the throwing call site did not pass
     * {@see \CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor::executeParallel()}
     * labels.
     *
     * @return list<int>
     */
    public function getFailedRegionIds(): array
    {
        $regionIds = [];
        foreach (array_keys($this->regionErrors) as $key) {
            $callId = $this->callIds[$key] ?? null;
            if ($callId instanceof BatchCallId) {
                $regionIds[$callId->regionId] = true;
            }
        }

        $regionIds = array_keys($regionIds);
        sort($regionIds);

        return $regionIds;
    }

    /**
     * The failed calls' region/sub-batch descriptors, keyed by the same
     * fan-out call index as {@see self::getRegionErrors()}.
     *
     * Several sub-batches of one region are reported separately and cannot
     * collide: each carries its own `subBatchIndex`. Keys with no descriptor
     * (a call site that did not pass labels) are omitted.
     *
     * @return array<int, BatchCallId>
     */
    public function getFailedCallIds(): array
    {
        $callIds = [];
        foreach (array_keys($this->regionErrors) as $key) {
            $callId = $this->callIds[$key] ?? null;
            if ($callId instanceof BatchCallId) {
                $callIds[$key] = $callId;
            }
        }

        return $callIds;
    }

    public function getTotalRegions(): int
    {
        return $this->totalRegions;
    }

    /**
     * Results from regions/sub-batches that completed successfully.
     * Callers can use these to make progress or report partial completion.
     *
     * @return array<int, mixed>
     */
    public function getPartialResults(): array
    {
        return $this->partialResults;
    }
}
