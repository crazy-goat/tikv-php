<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

enum BackoffType
{
    case None;
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

    public function baseMs(): int
    {
        return match ($this) {
            self::None => 0,
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
        };
    }

    public function capMs(): int
    {
        return match ($this) {
            self::None => 0,
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
        };
    }

    public function equalJitter(): bool
    {
        return match ($this) {
            self::ServerBusy, self::TiKvRpc, self::RecoveryInProgress, self::IsWitness,
            self::TxnLock => true,
            default => false,
        };
    }

    public function sleepMs(int $attempt): int
    {
        if ($this === self::None) {
            return 0;
        }

        return Backoff::exponential($this->baseMs(), $this->capMs(), $attempt, $this->equalJitter());
    }
}
