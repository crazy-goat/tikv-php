<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;

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
        // A rejected store address is always fatal. Checked before any message
        // matching: the raw address inside the exception may contain retry
        // keywords (e.g. "EpochNotMatch"), which would otherwise classify the
        // rejection as retryable and hide it behind up to 30 retries.
        if ($e instanceof InvalidStoreAddressException) {
            return null;
        }

        // A per-pair KeyError is not a region error and must never be retried
        // as a region miss (issue #251, REG-20).
        if ($e instanceof KeyErrorException) {
            return null;
        }

        // === Primary path: typed error kind on RegionException ===
        if ($e instanceof RegionException && $e->errorKind instanceof ErrorKind) {
            return self::classifyByKind($e->errorKind);
        }

        // === GC aborted: a transaction whose start timestamp GC has passed
        // is fatal — retrying the same startTs can only produce the same
        // server error. Checked before the message fallback so the GC
        // message can never be misread as retryable (issue #422).
        if ($e instanceof TxnAbortedByGcException) {
            return null;
        }

        // === gRPC transport errors: classified by status code (issue #240, REG-09) ===
        //
        // Every GrpcException used to be retried with TiKvRpc backoff, so a
        // permanent failure (e.g. UNAUTHENTICATED after a bad TLS rollout)
        // was retried up to 30 times per operation — a ~30x load multiplier
        // on the cluster at the worst possible moment. Retry decisions now
        // follow the standard gRPC retry guidance:
        //
        //   retryable:        UNAVAILABLE, ABORTED, INTERNAL, UNKNOWN,
        //                     DEADLINE_EXCEEDED (outcome indeterminate, REG-08),
        //   server-busy:      RESOURCE_EXHAUSTED,
        //   fatal (no retry): CANCELLED, INVALID_ARGUMENT, NOT_FOUND,
        //                     PERMISSION_DENIED, UNAUTHENTICATED, UNIMPLEMENTED,
        //                     FAILED_PRECONDITION, ALREADY_EXISTS, OUT_OF_RANGE,
        //                     DATA_LOSS, and any unrecognised code.
        if ($e instanceof GrpcException) {
            return self::classifyGrpcStatus($e->grpcStatusCode);
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

        // Immediate retry is reserved for DataIsNotReady (replica-lag
        // signal, issue #421). EpochNotMatch deliberately does NOT use
        // BackoffType::None: a zero-sleep classification drove a
        // zero-delay request storm against PD/TiKV during region churn
        // (issue #241, REG-10) — it now gets a small jittered backoff.
        if (str_contains($message, 'DataIsNotReady')) {
            return BackoffType::None;
        }
        if (str_contains($message, 'EpochNotMatch') || str_contains($message, 'epoch not match')) {
            return BackoffType::EpochNotMatch;
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

        // Generic exception type checks. An unmapped RegionException (no typed
        // ErrorKind) is fatal by default: unknown errors must not be retried
        // (issue #251, REG-20). Retryable cases are enumerated explicitly
        // above and via the typed ErrorKind path. A kind-less RegionException
        // therefore falls through to the final fatal return below.
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

            // EpochNotMatch gets a small jittered backoff (2–500 ms, like
            // client-go's BoEpochNotMatch) instead of zero — a zero-delay
            // classification drove a request storm against PD/TiKV during
            // region churn (issue #241, REG-10).
            ErrorKind::EpochNotMatch => BackoffType::EpochNotMatch,

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
            ErrorKind::MismatchPeerId,
            ErrorKind::BucketVersionNotMatch,
            ErrorKind::UndeterminedResult => BackoffType::RegionMiss,

            // DataIsNotReady is the replica-lag signal TiKV returns on
            // follower/replica reads when the peer's applied index (or
            // safe_ts for stale reads) is behind the request. Retry
            // immediately; the read closure excludes the failing store and
            // falls back to another replica or the leader (issue #421).
            ErrorKind::DataIsNotReady => BackoffType::None,
        };
    }

    /**
     * Map a gRPC status code to a BackoffType (issue #240, REG-09).
     *
     * Retryable statuses follow the standard gRPC retry guidance:
     * UNAVAILABLE and ABORTED are transient transport/concurrency errors,
     * INTERNAL and UNKNOWN are retried conservatively (they may carry
     * transient server failures), and DEADLINE_EXCEEDED leaves the outcome
     * indeterminate so it is retried — the idempotency distinction for
     * non-idempotent writes is REG-08's scope. RESOURCE_EXHAUSTED maps to
     * the ServerBusy backoff (bounded by the ServerBusy budget).
     * Everything else is permanent for the given request and must not be
     * retried — notably UNAUTHENTICATED and PERMISSION_DENIED (bad TLS or
     * credentials must fail fast, not 30x every request) and CANCELLED
     * (the caller went away).
     *
     * Unknown codes are treated as fatal (fail closed), never retried.
     */
    public static function classifyGrpcStatus(int $code): ?BackoffType
    {
        $status = GrpcStatusCode::tryFrom($code);

        return match ($status) {
            GrpcStatusCode::Unavailable,
            GrpcStatusCode::Aborted,
            GrpcStatusCode::Internal,
            GrpcStatusCode::Unknown,
            GrpcStatusCode::DeadlineExceeded => BackoffType::TiKvRpc,

            GrpcStatusCode::ResourceExhausted => BackoffType::ServerBusy,

            default => null,
        };
    }
}
