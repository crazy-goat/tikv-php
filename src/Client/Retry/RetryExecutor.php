<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;

final readonly class RetryExecutor
{
    /**
     * Default maximum number of attempts per call. Bounds the retry loop
     * as a termination backstop — it limits the number of requests, NOT
     * their rate. Rate limiting comes from the backoff classes themselves:
     * every retryable error sleeps for a non-zero, jittered interval and
     * contributes to a per-operation budget (BackoffType::None, the only
     * zero-sleep class, is reserved for the DataIsNotReady replica-lag
     * signal, where the very next attempt uses a different peer —
     * issue #241, REG-10). The cap still catches zero-backoff errors so
     * they cannot drive an infinite loop.
     */
    public const DEFAULT_MAX_ATTEMPTS = 30;

    /**
     * Canonical default wall-clock deadline (ms) for one operation's retry
     * loop — the bound that keeps the blocking usleep() backoff from pinning
     * a PHP-FPM worker for minutes (issue #294). Client classes
     * (RawKvClient, Transaction, TxnKvClient, RawKvScanner, RawKvRangeOps)
     * reference this single constant so the default cannot drift.
     */
    public const DEFAULT_RETRY_DEADLINE_MS = 30000;

    public function __construct(
        private int $maxBackoffMs,
        private int $serverBusyBudgetMs,
        private RegionCacheInterface $regionCache,
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private LoggerInterface $logger,
        private int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        private int $deadlineMs = self::DEFAULT_RETRY_DEADLINE_MS,
        private MetricsInterface $metrics = new NoOpMetrics(),
    ) {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException('maxAttempts must be >= 1');
        }
        if ($deadlineMs < 0) {
            throw new \InvalidArgumentException('deadlineMs must be >= 0');
        }
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @param (callable(TiKvException): ?BackoffType)|null $classifier Custom error classifier
     * @param RetryContext|null $context Optional per-invocation view of the
     *        wall-clock budget. The executor configures it with its deadline
     *        and operation start time, so an operation can bound a nested
     *        call (lock resolution, for example) by the same deadline instead
     *        of adding an independent timeout (issue #569).
     * @return T
     */
    public function execute(
        string $key,
        callable $operation,
        ?callable $classifier = null,
        ?RetryContext $context = null,
    ): mixed {
        // Per-invocation retry state lives in locals, not instance fields:
        // a single executor is reused across many operations, and
        // maxBackoffMs/serverBusyBudgetMs are documented as per-operation
        // limits, so every call must start with a full budget. Keeping them
        // local also makes execute() reentrant (issue #271).
        $totalBackoffMs = 0;
        $serverBusyBackoffMs = 0;
        $attempt = 0;
        $startTimeMs = $this->deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;
        $lastError = null;

        while (true) {
            // Enforce absolute attempt cap before running the operation.
            // 'attempt' counts completed runs, so on the next retry we would
            // run call #attempt+1; cap once that would exceed maxAttempts.
            // This is a termination backstop (BackoffType::None no longer
            // covers EpochNotMatch — issue #241 — so retryable errors also
            // spend a real backoff budget); the cap catches any remaining
            // zero-backoff classification.
            if ($attempt >= $this->maxAttempts) {
                $this->logger->error('Retry attempt cap exhausted', [
                    'key' => KeyRedactor::redact($key),
                    'attempt' => $attempt,
                    'maxAttempts' => $this->maxAttempts,
                    'totalBackoffMs' => $totalBackoffMs,
                ]);
                throw new RetryBudgetExhaustedException(
                    sprintf(
                        'Retry attempt cap (%d) exhausted for key %s',
                        $this->maxAttempts,
                        KeyRedactor::redact($key),
                    ),
                    $attempt,
                    $totalBackoffMs,
                    $lastError,
                    rawKey: $key,
                );
            }

            // Enforce wall-clock deadline (if configured) before each attempt.
            if ($this->deadlineMs > 0) {
                $this->assertDeadlineNotExhausted($key, $startTimeMs, $attempt, $lastError);
            }

            // Publish the budget for nested calls (issue #569). The context
            // computes the remaining value at call time, so time spent inside
            // the attempt's RPC is accounted for as well.
            $context?->configure($this->deadlineMs, $startTimeMs);

            try {
                return $operation();
            } catch (TiKvException $e) {
                $lastError = $e;
                $attemptBeforeInspection = $attempt;

                $backoffType = $this->handleNotLeader($e, $key);

                if (!$backoffType instanceof BackoffType) {
                    if ($classifier !== null) {
                        $backoffType = $classifier($e);
                    }

                    if (!$backoffType instanceof BackoffType) {
                        $backoffType = $this->classifyError($e);
                    }

                    if (!$backoffType instanceof BackoffType) {
                        // Issue #233 (REG-02): retryability and cache
                        // invalidation are two independent decisions, so the
                        // invalidation runs ABOVE this throw instead of below
                        // it. A routing error means the cached region that
                        // produced it is wrong, so the entry has to go even
                        // though the request itself is not retryable —
                        // otherwise the misroute survives for the whole
                        // lifetime of the entry (ttlSeconds + jitterSeconds,
                        // 660 s with the defaults) and every later request for
                        // the key repeats the same fatal error, with nothing
                        // the application can do in between. The decision
                        // itself lives in self::invalidatesRoutingOnFatal(),
                        // deliberately separate from the retry decision above.
                        //
                        // Metric emission stays with RegionCache::invalidate()
                        // (#474 — see invalidateRegionForFatalError()) and the
                        // gRPC channel is left alone: closeChannel() sits
                        // downstream of the retry decision, so no fatal error
                        // can reach it.
                        $this->invalidateRegionForFatalError($e, $key);

                        $this->logger->error('Fatal error, not retrying', [
                            'key' => KeyRedactor::redact($key),
                            'error' => $e->getMessage(),
                        ]);
                        throw $e;
                    }

                    // Issue #245 (REG-14): only a routing error means the
                    // cached region may be wrong. A non-routing retryable
                    // error (ServerIsBusy, DiskFull, IsWitness,
                    // RecoveryInProgress, …) reached the region that owns the
                    // key, so dropping the entry would only force a PD round
                    // trip on every attempt — a load spike on top of an
                    // already degraded cluster. An unclassified error (a
                    // transport failure or a message-only classification)
                    // still invalidates conservatively.
                    if (self::invalidatesRoutingOnRetry($e)) {
                        $cached = $this->regionCache->getByKey($key);
                        if ($cached instanceof RegionInfo) {
                            // The cache itself emits regionInvalidated() with
                            // the kind-tagged reason — do not emit here too.
                            $this->regionCache->invalidate(
                                $cached->regionId,
                                $this->retryInvalidationReason($e),
                            );
                            $this->logger->info('Invalidated region on retry', [
                                'key' => KeyRedactor::redact($key),
                                'regionId' => $cached->regionId,
                                'errorKind' => $e instanceof RegionException ? $e->errorKind?->value : null,
                            ]);

                            if ($e instanceof GrpcException) {
                                try {
                                    $address = $this->regionResolver->resolveStoreAddress($cached->leaderStoreId);
                                    $this->grpc->closeChannel($address);
                                } catch (StoreNotFoundException) {
                                }
                            }
                        }
                    }
                }

                $sleepMs = $backoffType->sleepMs($attemptBeforeInspection);

                if ($backoffType === BackoffType::ServerBusy) {
                    $serverBusyBackoffMs += $sleepMs;
                    if ($serverBusyBackoffMs > $this->serverBusyBudgetMs) {
                        $this->logger->error('ServerBusy budget exhausted', [
                            'key' => KeyRedactor::redact($key),
                            'attempt' => $attemptBeforeInspection,
                            'serverBusyBackoffMs' => $serverBusyBackoffMs,
                            'serverBusyBudgetMs' => $this->serverBusyBudgetMs,
                        ]);
                        throw $e;
                    }
                } else {
                    $totalBackoffMs += $sleepMs;
                    if ($totalBackoffMs > $this->maxBackoffMs) {
                        $this->logger->error('Retry budget exhausted', [
                            'key' => KeyRedactor::redact($key),
                            'attempt' => $attemptBeforeInspection,
                            'totalBackoffMs' => $totalBackoffMs,
                            'maxBackoffMs' => $this->maxBackoffMs,
                        ]);
                        throw $e;
                    }
                }

                $this->logger->warning('Retrying operation', [
                    'key' => KeyRedactor::redact($key),
                    'attempt' => $attemptBeforeInspection,
                    'backoffType' => $backoffType->name,
                    'sleepMs' => $sleepMs,
                    'totalBackoffMs' => $totalBackoffMs,
                ]);

                $this->metrics->retryAttempted($backoffType->name);

                if ($sleepMs > 0) {
                    // Issue #237: the deadline must also be checked (and the
                    // sleep clamped) BEFORE the backoff usleep(), not only
                    // before the next attempt — otherwise a single sleep
                    // (ServerBusy caps at 10 s) can overshoot the configured
                    // wall-clock budget by one full backoff interval.
                    $remainingMs = $this->remainingDeadlineMs($startTimeMs);
                    if ($remainingMs <= 0) {
                        $this->assertDeadlineNotExhausted($key, $startTimeMs, $attempt, $lastError);
                    }
                    $sleepMs = min($sleepMs, $remainingMs);
                    usleep($sleepMs * 1000);
                }

                $attempt++;
            }
        }
    }

    /**
     * Remaining wall-clock budget in ms; PHP_INT_MAX when the deadline is
     * disabled ($deadlineMs <= 0).
     */
    private function remainingDeadlineMs(int $startTimeMs): int
    {
        if ($this->deadlineMs <= 0) {
            return \PHP_INT_MAX;
        }

        $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;

        return $this->deadlineMs - $elapsedMs;
    }

    /**
     * @throws RetryBudgetExhaustedException when the wall-clock deadline has
     * been reached.
     */
    private function assertDeadlineNotExhausted(
        string $key,
        int $startTimeMs,
        int $attempt,
        ?TiKvException $lastError,
    ): void {
        $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
        if ($elapsedMs < $this->deadlineMs) {
            return;
        }
        $this->logger->error('Retry deadline exhausted', [
            'key' => KeyRedactor::redact($key),
            'attempt' => $attempt,
            'elapsedMs' => $elapsedMs,
            'deadlineMs' => $this->deadlineMs,
        ]);
        throw new RetryBudgetExhaustedException(
            sprintf(
                'Retry deadline (%d ms) exhausted for key %s',
                $this->deadlineMs,
                KeyRedactor::redact($key),
            ),
            $attempt,
            $elapsedMs,
            $lastError,
            rawKey: $key,
        );
    }

    /**
     * Whether an ErrorKind means the CACHED REGION that produced the error may
     * be wrong, i.e. whether the region-cache entry must be dropped.
     *
     * This is the single routing/non-routing table, shared by the fatal path
     * (invalidatesRoutingOnFatal(), issue #233) and the retryable path
     * (invalidatesRoutingOnRetry(), issue #245). A non-routing kind means the
     * request reached the region that owns the key and the region rejected the
     * request itself — a busy/overloaded store, a full disk, a witness peer,
     * recovery in progress, an oversized entry, a flashback, … — or the retry
     * must pick a different peer. The cached entry is correct, so keep it.
     *
     * The match has no `default` arm, so PHPStan reports match.unhandled at
     * level 9 and a new ErrorKind case cannot be added without deciding here.
     */
    public static function invalidatesRoutingKind(ErrorKind $kind): bool
    {
        return match ($kind) {
            ErrorKind::KeyNotInRegion,
            ErrorKind::EpochNotMatch,
            ErrorKind::RegionNotFound,
            ErrorKind::StoreNotMatch,
            ErrorKind::NotLeader,
            ErrorKind::RegionNotInitialized => true,

            // The request DID reach the region that owns the key; the region
            // rejected the request itself or the retry must pick a different
            // peer. The cached entry is correct, so keep it.
            ErrorKind::RaftEntryTooLarge,
            ErrorKind::FlashbackInProgress,
            ErrorKind::FlashbackNotPrepared,
            ErrorKind::ServerIsBusy,
            ErrorKind::DiskFull,
            ErrorKind::IsWitness,
            ErrorKind::RecoveryInProgress,
            ErrorKind::StaleCommand,
            ErrorKind::DataIsNotReady,
            ErrorKind::ReadIndexNotReady,
            ErrorKind::ProposalInMergingMode,
            ErrorKind::MaxTimestampNotSynced,
            ErrorKind::MismatchPeerId,
            ErrorKind::BucketVersionNotMatch,
            ErrorKind::UndeterminedResult => false,
        };
    }

    /**
     * Whether a FATAL (non-retryable) error must still drop the cached region
     * the request was routed through (issue #233, REG-02).
     *
     * Retryability and cache invalidation are independent decisions, and this
     * is the second one: client-go's RegionRequestSender.onRegionError drops
     * the region for a routing error whether or not the request is then
     * retried, because a routing error is evidence about the CACHED REGION,
     * not about the request. KeyNotInRegion is the extreme case — it is fatal
     * precisely because the key is not in the region the client believed owned
     * it, so the entry that caused it must not survive the throw.
     *
     * A StoreNotFoundException DOES invalidate (issue #627), but only the
     * region it is about. It is raised by RegionResolver::resolveStoreAddress()
     * while routing, when PD no longer knows a store the cached region names
     * (its leader, or a follower chosen by a replica read) and the store cache
     * has no address for it either. That is evidence about the CACHED REGION,
     * exactly like a routing error: keeping the entry would fail every later
     * request for the key with the same fatal error until the TTL ran out. The
     * exception is still thrown (fail closed, #104, #480); only the next request
     * asks PD again. See invalidateRegionForFatalError() for the store match:
     * the same exception can also come from another region (for example the
     * primary key's region during lock resolution), and that case does not
     * touch the executor key's entry.
     *
     * Any OTHER non-RegionException fatal error must NOT invalidate. A
     * GrpcException with a fatal status (UNAUTHENTICATED, PERMISSION_DENIED,
     * …), an InvalidStoreAddressException or a TxnAbortedByGcException says
     * nothing about which region the key lives in — they are credentials, a PD
     * answer and a GC verdict respectively — so dropping the entry would only
     * trade one re-resolve for another. A RegionException without a typed kind
     * carries no routing verdict either. This is a decision, not an omission.
     *
     * The routing/non-routing table itself lives in invalidatesRoutingKind(),
     * shared with the retryable path. ErrorKind::NotLeader is in that table
     * for completeness, not because it can reach a fatal path today:
     * handleNotLeader() returns BackoffType::NotLeader for ANY RegionException
     * carrying a notLeader oneof, before the custom classifier is ever
     * consulted, so control never enters the fatal block and this predicate is
     * never asked about it.
     */
    public static function invalidatesRoutingOnFatal(TiKvException $e): bool
    {
        if ($e instanceof StoreNotFoundException) {
            return true;
        }

        if (!$e instanceof RegionException) {
            return false;
        }

        $kind = $e->errorKind;
        if (!$kind instanceof ErrorKind) {
            return false;
        }

        return self::invalidatesRoutingKind($kind);
    }

    /**
     * Whether a RETRYABLE error must drop the cached region before the next
     * attempt (issue #245, REG-14).
     *
     * Only a routing error is evidence about the cached region. A non-routing
     * kind (ServerIsBusy, DiskFull, IsWitness, RecoveryInProgress, …) means
     * the request reached the right region, so the entry stays and the retry
     * reuses it — otherwise every attempt would force a PD GetRegion at
     * exactly the moment the cluster is under stress.
     *
     * An error without a typed ErrorKind (a GrpcException transport failure, a
     * message-classified TiKvException, or a kind-less RegionException)
     * invalidates conservatively: an unclassified error may be a routing
     * error, and keeping a stale entry would repeat the misroute until its TTL
     * expires. This is deliberately the opposite default from
     * invalidatesRoutingOnFatal(), where an unclassified error must not drop a
     * region it cannot be shown to be about.
     */
    public static function invalidatesRoutingOnRetry(TiKvException $e): bool
    {
        if (!$e instanceof RegionException) {
            return true;
        }

        $kind = $e->errorKind;
        if (!$kind instanceof ErrorKind) {
            return true;
        }

        return self::invalidatesRoutingKind($kind);
    }

    /**
     * Metric reason for a retryable routing invalidation.
     *
     * The label names the triggering ErrorKind so a dashboard can separate,
     * for example, an epoch change from a region miss:
     * 'retry_region_error:epoch_not_match'. An unclassified error keeps the
     * historical 'retry_region_error' label, which also keeps existing
     * dashboards working for the transport/message-only paths.
     */
    private function retryInvalidationReason(TiKvException $e): string
    {
        if ($e instanceof RegionException && $e->errorKind instanceof ErrorKind) {
            return 'retry_region_error:' . $e->errorKind->value;
        }

        return 'retry_region_error';
    }

    /**
     * Drop the cached region behind a fatal routing error (issue #233).
     *
     * Split from the retry decision on purpose — see
     * self::invalidatesRoutingOnFatal(). The retryable path's own
     * invalidation uses the same kind table through
     * self::invalidatesRoutingOnRetry() (issue #245).
     */
    private function invalidateRegionForFatalError(TiKvException $e, string $key): void
    {
        if (!self::invalidatesRoutingOnFatal($e)) {
            return;
        }

        $cached = $this->regionCache->getByKey($key);
        if (!$cached instanceof RegionInfo) {
            // Either nothing was cached under this key, or the source of the
            // region error (RegionErrorHandler::check()) already dropped the
            // entry and emitted for it. Either way there is no state change to
            // count (issue #474's single-emission rule).
            return;
        }

        // A StoreNotFoundException may be about another region (lock
        // resolution routes the primary key inside this call). Only drop the
        // entry when the missing store belongs to it (issue #627).
        if ($e instanceof StoreNotFoundException && !$this->regionUsesStore($cached, $e->storeId)) {
            return;
        }

        // The cache itself emits regionInvalidated() with the reason passed
        // here — do not emit here too.
        $this->regionCache->invalidate($cached->regionId, 'fatal_region_error');
        $this->logger->info('Invalidated region on fatal routing error', [
            'key' => KeyRedactor::redact($key),
            'regionId' => $cached->regionId,
            'errorKind' => $e instanceof RegionException ? $e->errorKind?->value : null,
        ]);
    }

    private function regionUsesStore(RegionInfo $region, int $storeId): bool
    {
        if ($region->leaderStoreId === $storeId) {
            return true;
        }

        foreach ($region->peers as $peer) {
            if ($peer->storeId === $storeId) {
                return true;
            }
        }

        return false;
    }

    private function handleNotLeader(TiKvException $e, string $key): ?BackoffType
    {
        if (!$e instanceof RegionException || !$e->notLeader instanceof NotLeader) {
            return null;
        }

        $regionId = (int) $e->notLeader->getRegionId();
        $leader = $e->notLeader->getLeader();

        if ($leader !== null) {
            $leaderStoreId = (int) $leader->getStoreId();
            $switched = $this->regionCache->switchLeader($regionId, $leaderStoreId);
            if (!$switched) {
                // The cache emits regionInvalidated('not_leader') itself.
                $this->regionCache->invalidate($regionId, 'not_leader');
                $this->logger->info('NotLeader hint peer unknown, invalidated region', [
                    'key' => KeyRedactor::redact($key),
                    'regionId' => $regionId,
                    'hintStoreId' => $leaderStoreId,
                ]);
            }
        } else {
            // The cache emits regionInvalidated('not_leader') itself.
            $this->regionCache->invalidate($regionId, 'not_leader');
            $this->logger->info('NotLeader without hint, invalidated region', [
                'key' => KeyRedactor::redact($key),
                'regionId' => $regionId,
            ]);
        }

        return BackoffType::NotLeader;
    }

    private function classifyError(TiKvException $e): ?BackoffType
    {
        return ErrorClassifier::classify($e);
    }
}
