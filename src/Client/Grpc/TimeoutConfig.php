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
 * Every field is an explicit number of milliseconds. `0` keeps the
 * historical "no deadline" meaning (documented, not recommended) — see
 * {@see GrpcClient::resolveTimeoutMs()}.
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

    public function __construct(
        public int $readTimeoutMs = 5000,
        public int $writeTimeoutMs = 5000,
        public int $batchReadTimeoutMs = 10000,
        public int $batchWriteTimeoutMs = 10000,
        public int $scanTimeoutMs = 20000,
        public int $deleteRangeTimeoutMs = 30000,
        public int $checksumTimeoutMs = 30000,
        public int $ingestTimeoutMs = 60000,
        public int $batchDeadlineMs = 0,
        public int $pdTimeoutMs = self::DEFAULT_PD_TIMEOUT_MS,
        public int $tsoTimeoutMs = self::DEFAULT_TSO_TIMEOUT_MS,
        public int $lockResolveTimeoutMs = self::DEFAULT_LOCK_RESOLVE_TIMEOUT_MS,
    ) {
    }
}
