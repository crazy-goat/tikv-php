<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Grpc;

/**
 * Per-RPC deadline configuration.
 *
 * The TiKV-store fields (read/write/batch/scan/deleteRange/checksum/ingest)
 * size a *store* deadline; `pdTimeoutMs`, `tsoTimeoutMs` and
 * `lockResolveTimeoutMs` size the metadata-plane deadlines, which fail for
 * different reasons: a hung PD (a blackholed connection, a PD that accepts
 * and never answers) takes every lookup and every transaction begin with it,
 * while a slow store is a per-region problem the retry executor already
 * handles. They are therefore configured separately (issue #260).
 *
 * Every field except `batchDeadlineMs` is an explicit number of
 * milliseconds and must be strictly positive; `0` or a negative value is a
 * configuration error (issue #276, GRPC-17). `batchDeadlineMs` is the
 * wall-clock budget of one whole batch fan-out and defaults to
 * {@see self::DEFAULT_BATCH_DEADLINE_MS} (issue #275, GRPC-16); `0` is the
 * explicit opt-out that disables it. A positive value must not be shorter
 * than any per-call timeout used inside a fan-out, because the deadline is
 * checked between waits and cannot cut a blocking wait short.
 */
final readonly class TimeoutConfig
{
    /**
     * Default deadline for a PD metadata RPC (`GetRegion`, `ScanRegions`,
     * `GetStore`, `GetMembers`, …). 3 s matches the default client-go's
     * `tikv/pd/client` uses for PD RPCs (issue #260).
     */
    public const DEFAULT_PD_TIMEOUT_MS = 3000;

    /**
     * Default deadline for a `Tso` RPC. 3 s, same reasoning as
     * {@see self::DEFAULT_PD_TIMEOUT_MS} (issue #260).
     */
    public const DEFAULT_TSO_TIMEOUT_MS = 3000;

    /**
     * Default deadline for a lock-resolution RPC (`KvCheckTxnStatus`,
     * `KvCheckSecondaryLocks`, `KvResolveLock`). Higher than the store
     * read/write defaults because `checkTxnStatus()` fetches a TSO
     * timestamp before the RPC and may wait on a transaction that is still
     * being decided (issue #260).
     */
    public const DEFAULT_LOCK_RESOLVE_TIMEOUT_MS = 5000;

    /**
     * Default wall-clock budget of one batch fan-out (issue #275). It is not
     * lower than the longest per-call timeout used inside a fan-out
     * (`ingestTimeoutMs`, 60 s), so the defaults pass the constructor check.
     */
    public const DEFAULT_BATCH_DEADLINE_MS = 60000;

    /**
     * The per-call timeouts used by calls that run inside a batch fan-out
     * (RawKV batch, scan, range operations, SST ingest and the TxnKV
     * prewrite/commit/read fan-out). `readTimeoutMs` is not listed: only
     * synchronous point reads use it, never a fan-out.
     */
    private const BATCH_FAN_OUT_TIMEOUTS = [
        'writeTimeoutMs',
        'batchReadTimeoutMs',
        'batchWriteTimeoutMs',
        'scanTimeoutMs',
        'deleteRangeTimeoutMs',
        'checksumTimeoutMs',
        'ingestTimeoutMs',
    ];

    public function __construct(
        public int $readTimeoutMs = 5000,
        public int $writeTimeoutMs = 5000,
        public int $batchReadTimeoutMs = 10000,
        public int $batchWriteTimeoutMs = 10000,
        public int $scanTimeoutMs = 20000,
        public int $deleteRangeTimeoutMs = 30000,
        public int $checksumTimeoutMs = 30000,
        public int $ingestTimeoutMs = 60000,
        public int $batchDeadlineMs = self::DEFAULT_BATCH_DEADLINE_MS,
        public int $pdTimeoutMs = self::DEFAULT_PD_TIMEOUT_MS,
        public int $tsoTimeoutMs = self::DEFAULT_TSO_TIMEOUT_MS,
        public int $lockResolveTimeoutMs = self::DEFAULT_LOCK_RESOLVE_TIMEOUT_MS,
    ) {
        $timeouts = [
            'readTimeoutMs' => $this->readTimeoutMs,
            'writeTimeoutMs' => $this->writeTimeoutMs,
            'batchReadTimeoutMs' => $this->batchReadTimeoutMs,
            'batchWriteTimeoutMs' => $this->batchWriteTimeoutMs,
            'scanTimeoutMs' => $this->scanTimeoutMs,
            'deleteRangeTimeoutMs' => $this->deleteRangeTimeoutMs,
            'checksumTimeoutMs' => $this->checksumTimeoutMs,
            'ingestTimeoutMs' => $this->ingestTimeoutMs,
            'pdTimeoutMs' => $this->pdTimeoutMs,
            'tsoTimeoutMs' => $this->tsoTimeoutMs,
            'lockResolveTimeoutMs' => $this->lockResolveTimeoutMs,
        ];

        foreach ($timeouts as $name => $value) {
            if ($value <= 0) {
                throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                    sprintf('%s must be greater than 0, got %d', $name, $value),
                );
            }
        }

        // `batchDeadlineMs` sizes the overall batch deadline and keeps `0`
        // as the documented "disabled" value — only a negative value is a
        // configuration error there.
        if ($this->batchDeadlineMs < 0) {
            throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(
                sprintf('batchDeadlineMs must be greater than or equal to 0, got %d', $this->batchDeadlineMs),
            );
        }

        // The deadline is checked at dispatch and wait boundaries, never
        // during one blocking wait (which can run a region's whole retry
        // loop): a budget shorter than a per-call timeout of the fan-out
        // cannot be honoured, so it is a configuration error (issue #275).
        if ($this->batchDeadlineMs > 0) {
            foreach (self::BATCH_FAN_OUT_TIMEOUTS as $name) {
                if ($this->batchDeadlineMs < $this->{$name}) {
                    throw new \CrazyGoat\TiKV\Client\Exception\InvalidArgumentException(sprintf(
                        'batchDeadlineMs (%d) must not be less than %s (%d): the batch deadline is checked '
                        . 'between waits, so it cannot cut a single call short. Raise batchDeadlineMs, lower '
                        . '%s, or set batchDeadlineMs to 0 to disable the batch deadline',
                        $this->batchDeadlineMs,
                        $name,
                        $this->{$name},
                        $name,
                    ));
                }
            }
        }
    }
}
