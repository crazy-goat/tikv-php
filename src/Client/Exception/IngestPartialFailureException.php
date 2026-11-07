<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

/**
 * An SST ingest failed after at least one region had already been imported.
 *
 * `RawKvClient::ingest()` is not atomic across regions: it imports one region
 * group at a time. When a later group fails permanently (or exhausts its
 * retry budget), the earlier groups keep their data. This exception carries
 * the region IDs that were applied, so a caller can report or reconcile the
 * partial state instead of guessing.
 *
 * Re-running the same key-value pairs is the documented recovery: an SST
 * ingest is idempotent for identical input (a TTL, if any, restarts).
 */
final class IngestPartialFailureException extends TiKvException
{
    /**
     * Upper bound on the region IDs rendered into the message. A bulk import
     * that fails late can have thousands of applied regions; the full list
     * stays available through getAppliedRegionIds().
     */
    private const MAX_REGION_IDS_IN_MESSAGE = 10;

    /**
     * @param list<int> $appliedRegionIds Regions whose SST was ingested successfully
     * @param int $failedRegionId The region whose import failed
     * @param TiKvException $cause The underlying failure
     */
    public function __construct(
        private readonly array $appliedRegionIds,
        private readonly int $failedRegionId,
        TiKvException $cause,
    ) {
        parent::__construct(
            sprintf(
                'SST ingest partially failed: %d region(s) ingested (%s); region %d failed: %s. '
                . 'Re-running the same key-value pairs is safe.',
                count($appliedRegionIds),
                $this->formatRegionIds($appliedRegionIds),
                $failedRegionId,
                $cause->getMessage(),
            ),
            0,
            $cause,
        );
    }

    /**
     * @param list<int> $regionIds
     */
    private function formatRegionIds(array $regionIds): string
    {
        $shown = array_slice($regionIds, 0, self::MAX_REGION_IDS_IN_MESSAGE);
        $formatted = implode(', ', $shown);

        $remaining = count($regionIds) - count($shown);
        if ($remaining > 0) {
            $formatted .= sprintf(', ... (%d more)', $remaining);
        }

        return $formatted;
    }

    /**
     * Region IDs whose data was successfully ingested before the failure.
     *
     * @return list<int>
     */
    public function getAppliedRegionIds(): array
    {
        return $this->appliedRegionIds;
    }

    /**
     * The region whose import failed.
     */
    public function getFailedRegionId(): int
    {
        return $this->failedRegionId;
    }
}
