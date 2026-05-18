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
     * @return BackoffType|null BackoffType for retryable errors, null for fatal/non-retryable
     */
    public static function classify(TiKvException $e): ?BackoffType
    {
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
}
