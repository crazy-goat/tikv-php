<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

enum BackoffType
{
    case None;
    case EpochNotMatch;
    case ServerBusy;
    case StaleCmd;
    case RegionMiss;
    case TiKvRpc;
    case NotLeader;

    // Additional region error types
    case DiskFull;
    case RegionNotInitialized;
    case ReadIndexNotReady;
    case ProposalInMergingMode;
    case RecoveryInProgress;
    case IsWitness;
    case MaxTimestampNotSynced;

    // Transactional backoff types
    case TxnLock;
    case TxnLockFast;
    case TxnNotFound;
    case CommitTSLag;

    // PD RPC backoff (client-go's BoPDRPC)
    case PDRpc;

    public function baseMs(): int
    {
        return match ($this) {
            self::None => 0,
            // EpochNotMatch: same values as client-go's BoEpochNotMatch —
            // small (2 ms base) but NON-zero and jittered, so a burst of
            // region-split errors cannot drive a zero-delay request storm
            // against PD/TiKV (issue #241, REG-10). The sleep also feeds
            // totalBackoffMs, so repeated errors exhaust the backoff budget.
            self::EpochNotMatch => 2,
            self::ServerBusy => 2000,
            self::StaleCmd => 2,
            self::RegionMiss => 2,
            self::TiKvRpc => 100,
            self::NotLeader => 2,
            // Additional error types
            self::DiskFull => 500,
            self::RegionNotInitialized => 2,
            self::ReadIndexNotReady => 2,
            self::ProposalInMergingMode => 2,
            self::RecoveryInProgress => 100,
            self::IsWitness => 1000,
            self::MaxTimestampNotSynced => 2,
            // Transactional backoff types
            self::TxnLock => 200,
            // The fast/not-found/commit-ts-lag curves mirror client-go's
            // BoTxnLockFast, BoTxnNotFound and BoCommitTSLag (2 ms base,
            // 500 ms cap).
            self::TxnLockFast => 2,
            self::TxnNotFound => 2,
            self::CommitTSLag => 2,
            // client-go's BoPDRPC: a PD RPC gets a slower 500–3000 ms curve
            // because the whole cluster shares the endpoint.
            self::PDRpc => 500,
        };
    }

    public function capMs(): int
    {
        return match ($this) {
            self::None => 0,
            self::EpochNotMatch => 500,
            self::ServerBusy => 10000,
            self::StaleCmd => 1000,
            self::RegionMiss => 500,
            self::TiKvRpc => 2000,
            self::NotLeader => 500,
            // Additional error types
            self::DiskFull => 5000,
            self::RegionNotInitialized => 1000,
            self::ReadIndexNotReady => 500,
            self::ProposalInMergingMode => 500,
            self::RecoveryInProgress => 10000,
            self::IsWitness => 10000,
            self::MaxTimestampNotSynced => 500,
            // Transactional backoff types
            self::TxnLock => 3000,
            self::TxnLockFast => 500,
            self::TxnNotFound => 500,
            self::CommitTSLag => 500,
            self::PDRpc => 3000,
        };
    }

    /**
     * All backoff types use equal jitter. Errors such as NotLeader and
     * RegionMiss are fleet-correlated (a leader transfer or region split is
     * observed by every client at nearly the same instant), so a deterministic
     * delay would make all clients retry in lockstep against the node that
     * most needs to be left alone (issue #242, REG-11).
     */
    public function equalJitter(): bool
    {
        return true;
    }

    public function sleepMs(int $attempt): int
    {
        if ($this === self::None) {
            return 0;
        }

        return Backoff::exponential($this->baseMs(), $this->capMs(), $attempt, $this->equalJitter());
    }
}
