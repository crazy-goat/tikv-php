<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

use CrazyGoat\TiKV\Client\Exception\CasIndeterminateException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\KeyErrorException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreUnavailableException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;

final class ErrorClassifier
{
    /**
     * Classify a TiKvException into a retry verdict.
     *
     * The built-in classifier always has an opinion: a retryable error gets
     * the mapped {@see BackoffType}, anything else is {@see RetryVerdict::fatal()}.
     * It never returns {@see RetryVerdict::pass()}, which exists for custom
     * classifiers that want to defer to this one (issue #534).
     *
     * A RegionException is classified from its typed ErrorKind only. An
     * errorpb.Error whose variant the enum does not know yet (a newer TiKV
     * release) gets a bounded RegionMiss retry instead of being matched
     * against its message or failing immediately (issue #444, DIV-08). A
     * synthetic RegionException without a kind stays fatal. Non-region
     * exceptions without a typed kind still use the message-based fallback,
     * which exists for errors such as PdException.
     */
    public static function classify(TiKvException $e): RetryVerdict
    {
        $backoffType = self::classifyToBackoffType($e);

        return $backoffType instanceof BackoffType
            ? RetryVerdict::retry($backoffType)
            : RetryVerdict::fatal();
    }

    /**
     * Map a TiKvException to a BackoffType, or null when it is fatal.
     *
     * @return BackoffType|null BackoffType for retryable errors, null for fatal/non-retryable
     */
    private static function classifyToBackoffType(TiKvException $e): ?BackoffType
    {
        // A rejected store address is always fatal. Checked before any message
        // matching: the raw address inside the exception may contain retry
        // keywords (e.g. "EpochNotMatch"), which would otherwise classify the
        // rejection as retryable and hide it behind up to 30 retries.
        if ($e instanceof InvalidStoreAddressException) {
            return null;
        }

        // A store in its health cooldown is a transport-level transient
        // condition, not a routing error: wait it out through the normal RPC
        // backoff so the next attempt is a half-open probe (issue #424).
        if ($e instanceof StoreUnavailableException) {
            return BackoffType::TiKvRpc;
        }

        // A per-pair KeyError is not a region error and must never be retried
        // as a region miss (issue #251, REG-20).
        if ($e instanceof KeyErrorException) {
            return null;
        }

        // A CAS whose outcome is indeterminate must never be retried, even if
        // its message happens to contain a retryable-looking fragment (the
        // redacted key comes from a user-supplied redactor). The verdict is
        // carried by the type, not by the message-text fallback below.
        if ($e instanceof CasIndeterminateException) {
            return null;
        }

        // === Primary path: typed error kind on RegionException ===
        //
        // A region error never falls through to message matching: the typed
        // oneof is the contract, and an unrecognised variant (a newer TiKV
        // release) gets a bounded RegionMiss retry rather than an immediate
        // failure. The retry path invalidates the cached region for a
        // kind-less RegionException, so the next attempt re-resolves it.
        if ($e instanceof RegionException) {
            if ($e->errorKind instanceof ErrorKind) {
                return self::classifyByKind($e->errorKind);
            }

            // Only an errorpb.Error is an unrecognised REGION error. A
            // synthetic RegionException (a response-level error string or a
            // top-level KeyError) is not a routing signal and stays fatal.
            return $e->fromErrorpb ? BackoffType::RegionMiss : null;
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

        // A retry-budget exhaustion is terminal: an outer executor must not
        // retry a give-up. The library's ServerBusy-budget message says
        // "ServerBusy" and does not match the fallback's "ServerIsBusy"
        // keyword, but a message that does name the region error would
        // otherwise classify as retryable. The typed arm keeps the verdict
        // fatal either way (issue #256).
        if ($e instanceof RetryBudgetExhaustedException) {
            return null;
        }

        // === Fallback: message-text matching for non-region exceptions ===
        //
        // RegionException returned above, so this path only sees errors that
        // carry no typed variant at all (for example PdException, whose
        // message is the only classification signal). It is never reached for
        // an errorpb.Error.
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

        // A non-region error with no recognised text is fatal by default:
        // unknown errors must not be retried (issue #251, REG-20). Region
        // errors never reach this point — they are classified by their typed
        // kind or get the bounded RegionMiss retry above (issue #444).
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
