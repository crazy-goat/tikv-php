<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;

final class ErrorClassifier
{
    /**
     * Classify a TiKvException into a BackoffType.
     *
     * The primary classification path uses the typed ErrorKind carried by
     * RegionException, eliminating dependence on message-string matching.
     * For exceptions that do not carry a typed kind (legacy or non-region
     * errors), a message-based fallback is used.
     *
     * @return BackoffType|null BackoffType for retryable errors, null for fatal/non-retryable
     */
    public static function classify(TiKvException $e): ?BackoffType
    {
        // === Primary path: typed error kind on RegionException ===
        if ($e instanceof RegionException && $e->errorKind instanceof \CrazyGoat\TiKV\Client\Retry\ErrorKind) {
            return self::classifyByKind($e->errorKind);
        }

        // === Fallback: message-text matching for exceptions without a typed kind ===
        $message = $e->getMessage();

        // Fatal errors (non-retryable)
        if (str_contains($message, 'RaftEntryTooLarge')) {
            return null;
        }
        if (str_contains($message, 'KeyNotInRegion')) {
            return null;
        }

        // Immediate retry (no backoff)
        if (str_contains($message, 'EpochNotMatch') || str_contains($message, 'epoch not match')) {
            return BackoffType::None;
        }

        // Region errors with specific backoff
        if (str_contains($message, 'ServerIsBusy')) {
            return BackoffType::ServerBusy;
        }
        if (str_contains($message, 'StaleCommand')) {
            return BackoffType::StaleCmd;
        }
        if (str_contains($message, 'RegionNotFound')) {
            return BackoffType::RegionMiss;
        }
        if (str_contains($message, 'NotLeader')) {
            return BackoffType::NotLeader;
        }
        if (str_contains($message, 'DiskFull')) {
            return BackoffType::DiskFull;
        }
        if (str_contains($message, 'RegionNotInitialized')) {
            return BackoffType::RegionNotInitialized;
        }
        if (str_contains($message, 'ReadIndexNotReady')) {
            return BackoffType::ReadIndexNotReady;
        }
        if (str_contains($message, 'ProposalInMergingMode')) {
            return BackoffType::ProposalInMergingMode;
        }
        if (str_contains($message, 'RecoveryInProgress')) {
            return BackoffType::RecoveryInProgress;
        }
        if (str_contains($message, 'IsWitness')) {
            return BackoffType::IsWitness;
        }
        if (str_contains($message, 'MaxTimestampNotSynced')) {
            return BackoffType::MaxTimestampNotSynced;
        }

        // Fatal flashback errors
        if (str_contains($message, 'FlashbackInProgress')) {
            return null;
        }
        if (str_contains($message, 'FlashbackNotPrepared')) {
            return null;
        }

        // Generic exception type checks
        if ($e instanceof GrpcException) {
            return BackoffType::TiKvRpc;
        }

        if ($e instanceof RegionException) {
            return BackoffType::RegionMiss;
        }

        return null;
    }

    /**
     * Map a typed ErrorKind to the corresponding BackoffType.
     *
     * This method is the single source of truth for the error-kind → backoff
     * mapping.  It is used as the primary classification path and should be
     * kept in sync with the message-based fallback above for consistency.
     *
     * @return BackoffType|null BackoffType for retryable errors, null for fatal
     */
    public static function classifyByKind(ErrorKind $kind): ?BackoffType
    {
        return match ($kind) {
            ErrorKind::RaftEntryTooLarge,
            ErrorKind::KeyNotInRegion,
            ErrorKind::FlashbackInProgress,
            ErrorKind::FlashbackNotPrepared => null,

            ErrorKind::EpochNotMatch => BackoffType::None,

            ErrorKind::ServerIsBusy => BackoffType::ServerBusy,
            ErrorKind::StaleCommand => BackoffType::StaleCmd,
            ErrorKind::RegionNotFound => BackoffType::RegionMiss,
            ErrorKind::NotLeader => BackoffType::NotLeader,
            ErrorKind::DiskFull => BackoffType::DiskFull,
            ErrorKind::RegionNotInitialized => BackoffType::RegionNotInitialized,
            ErrorKind::ReadIndexNotReady => BackoffType::ReadIndexNotReady,
            ErrorKind::ProposalInMergingMode => BackoffType::ProposalInMergingMode,
            ErrorKind::RecoveryInProgress => BackoffType::RecoveryInProgress,
            ErrorKind::IsWitness => BackoffType::IsWitness,
            ErrorKind::MaxTimestampNotSynced => BackoffType::MaxTimestampNotSynced,

            // Unmapped kinds default to region-miss retry.
            ErrorKind::StoreNotMatch,
            ErrorKind::DataIsNotReady,
            ErrorKind::MismatchPeerId,
            ErrorKind::BucketVersionNotMatch,
            ErrorKind::UndeterminedResult => BackoffType::RegionMiss,
        };
    }
}
