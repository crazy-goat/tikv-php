<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Exception\CasIndeterminateException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\StoreNotFoundException;
use CrazyGoat\TiKV\Client\Exception\StoreUnavailableException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyOrder;
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
        /**
         * Budget shared by every execute() call on this instance, or null for
         * a standalone executor whose calls each start a fresh budget. Only
         * {@see self::beginOperation()} sets it; see {@see RetryBudget}.
         */
        private ?RetryBudget $budget = null,
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
     * @param (callable(TiKvException): RetryVerdict)|null $classifier Custom
     *        error classifier (issue #534): return
     *        {@see RetryVerdict::retry()} to retry, {@see RetryVerdict::fatal()}
     *        to stop, or {@see RetryVerdict::pass()} to fall through to the
     *        built-in classifier.
     * @param (callable(): ?int)|null $targetStoreId Provider of the store ID
     *        the FAILED attempt was actually addressed to. Pass it for reads
     *        that may target a follower (`ReplicaReadPolicy`): a transport
     *        failure then closes and invalidates that follower's address
     *        instead of the cached leader's (issue #247). Without it the
     *        executor falls back to the cached region's leader, which is
     *        correct for leader-only operations.
     * @return T
     */
    public function execute(
        string $key,
        callable $operation,
        ?callable $classifier = null,
        ?callable $targetStoreId = null,
    ): mixed {
        // Retry budget lives in a RetryBudget object, not in instance fields:
        // a single executor is reused across many operations, and
        // maxBackoffMs/serverBusyBudgetMs/deadlineMs are documented as
        // per-operation limits, so every operation must start with a full
        // budget. A standalone executor (budget === null) starts a fresh one
        // per call, preserving issue #271's per-execute() reset; an executor
        // returned by beginOperation() shares one budget across every call of
        // one logical operation's fan-out (issue #223). Keeping the state in
        // an object also makes execute() reentrant.
        $budget = $this->budget ?? $this->newBudget();
        $attempt = 0;
        $startTimeMs = $budget->startTimeMs();
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
                    'totalBackoffMs' => $budget->totalBackoffMs(),
                ]);
                throw new RetryBudgetExhaustedException(
                    sprintf(
                        'Retry attempt cap (%d) exhausted for key %s',
                        $this->maxAttempts,
                        KeyRedactor::redact($key),
                    ),
                    $attempt,
                    $budget->totalBackoffMs(),
                    $lastError,
                    rawKey: $key,
                );
            }

            // Enforce wall-clock deadline (if configured) before each attempt.
            if ($this->deadlineMs > 0) {
                $this->assertDeadlineNotExhausted($key, $startTimeMs, $attempt, $lastError);
            }

            try {
                $result = $operation();
                $this->recordStoreSuccess($key, $targetStoreId);

                return $result;
            } catch (TiKvException $e) {
                $lastError = $e;
                $attemptBeforeInspection = $attempt;

                $backoffType = $this->handleNotLeader($e, $key);

                if (!$backoffType instanceof BackoffType) {
                    // Issue #534: a classifier answers with a RetryVerdict, so
                    // "no opinion" (Pass) is distinct from "fatal". Only a
                    // Pass falls through to the built-in classifier.
                    $verdict = $classifier !== null ? $classifier($e) : RetryVerdict::pass();

                    if ($verdict->isPass()) {
                        $verdict = $this->classifyError($e);
                    }

                    if ($verdict->isFatal()) {
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
                        // Issue #534: a transport failure marked fatal by a
                        // classifier (RawKvAtomic's CAS classifier) still gets
                        // the broken-channel cleanup the retryable path runs.
                        // Before the RetryVerdict contract the classifier had
                        // to throw to force a fatal verdict, which skipped
                        // this block and left the dead channel open.
                        if ($e instanceof GrpcException && $this->indicatesBrokenConnection($e)) {
                            $cached = $this->regionCache->getByKey($key);
                            $failedStoreId = $this->targetStoreIdOf($targetStoreId)
                                ?? ($cached instanceof RegionInfo ? $cached->leaderStoreId : null);
                            $this->closeBrokenChannel($e, $failedStoreId);
                        }

                        // Metric emission stays with RegionCache::invalidate()
                        // (#474 — see invalidateRegionForFatalError()).
                        $this->invalidateRegionForFatalError($e, $key);

                        $this->logger->error('Fatal error, not retrying', [
                            'key' => KeyRedactor::redact($key),
                            'error' => $e->getMessage(),
                        ]);
                        $this->recordStoreFailure($e);
                        throw $e;
                    }

                    $backoffType = $verdict->backoffType();

                    // Issue #246 (REG-15): an EpochNotMatch usually carries
                    // the regions that now cover the stale range in
                    // `current_regions`. Refresh the cache from that payload
                    // instead of dropping the entry and paying a PD
                    // GetRegion on the next attempt. The payload path is
                    // all-or-nothing: an empty, undecodable, leaderless,
                    // older-epoch or non-covering payload falls through to
                    // the ordinary invalidation below.
                    //
                    // Issue #245 (REG-14): only a routing error means the
                    // cached region may be wrong. A non-routing retryable
                    // error (ServerIsBusy, DiskFull, IsWitness,
                    // RecoveryInProgress, …) reached the region that owns the
                    // key, so dropping the entry would only force a PD round
                    // trip on every attempt — a load spike on top of an
                    // already degraded cluster. An unclassified error (a
                    // transport failure or a message-only classification)
                    // still invalidates conservatively.
                    if (
                        !$this->refreshRegionCacheFromEpochNotMatch($e, $key)
                        && self::invalidatesRoutingOnRetry($e)
                    ) {
                        $cached = $this->regionCache->getByKey($key);
                        // Issue #247 (REG-16): the region cache and the store
                        // address cache are separate. A transport failure may
                        // have hit a follower (replica read), not the cached
                        // leader, so the provider is authoritative when the
                        // caller supplies one. Fall back to the cached
                        // leader only for leader-targeted operations.
                        $failedStoreId = $this->targetStoreIdOf($targetStoreId)
                            ?? ($cached instanceof RegionInfo ? $cached->leaderStoreId : null);

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
                        }

                        if ($e instanceof GrpcException) {
                            $this->closeBrokenChannel($e, $failedStoreId);
                        }

                        // A StoreNotMatch says the address the request went
                        // to is wrong; a transport failure says the address
                        // it could not reach may be wrong (a rescheduled pod
                        // keeps the store ID and changes the address). Drop
                        // that entry so the next attempt asks PD instead of
                        // retrying the same dead endpoint for the rest of its
                        // 660 s TTL.
                        //
                        // The region may already be gone here: TxnKV call
                        // sites run RegionErrorHandler::check() before the
                        // executor sees the exception, so the StoreNotMatch
                        // payload's request_store_id (or the target/region
                        // captured for this attempt) is the fallback.
                        $this->invalidateStoreForRetry(
                            $e,
                            $cached instanceof RegionInfo
                                ? $cached
                                : ($e instanceof RegionException ? $e->cachedRegion : null),
                            $failedStoreId,
                        );
                    }
                }

                $this->recordStoreFailure($e);

                $sleepMs = $backoffType->sleepMs($attemptBeforeInspection);

                if ($backoffType === BackoffType::ServerBusy) {
                    $serverBusyBackoffMs = $budget->addServerBusy($sleepMs);
                    if ($serverBusyBackoffMs > $this->serverBusyBudgetMs) {
                        $this->logger->error('ServerBusy budget exhausted', [
                            'key' => KeyRedactor::redact($key),
                            'attempt' => $attemptBeforeInspection,
                            'serverBusyBackoffMs' => $serverBusyBackoffMs,
                            'serverBusyBudgetMs' => $this->serverBusyBudgetMs,
                        ]);
                        throw new RetryBudgetExhaustedException(
                            sprintf(
                                'ServerBusy budget (%d ms) exhausted for key %s',
                                $this->serverBusyBudgetMs,
                                KeyRedactor::redact($key),
                            ),
                            $attemptBeforeInspection,
                            $serverBusyBackoffMs,
                            $e,
                            rawKey: $key,
                        );
                    }
                } else {
                    $totalBackoffMs = $budget->addBackoff($sleepMs);
                    if ($totalBackoffMs > $this->maxBackoffMs) {
                        $this->logger->error('Retry budget exhausted', [
                            'key' => KeyRedactor::redact($key),
                            'attempt' => $attemptBeforeInspection,
                            'totalBackoffMs' => $totalBackoffMs,
                            'maxBackoffMs' => $this->maxBackoffMs,
                        ]);
                        throw new RetryBudgetExhaustedException(
                            sprintf(
                                'Retry backoff budget (%d ms) exhausted for key %s',
                                $this->maxBackoffMs,
                                KeyRedactor::redact($key),
                            ),
                            $attemptBeforeInspection,
                            $totalBackoffMs,
                            $e,
                            rawKey: $key,
                        );
                    }
                }

                $this->logger->warning('Retrying operation', [
                    'key' => KeyRedactor::redact($key),
                    'attempt' => $attemptBeforeInspection,
                    'backoffType' => $backoffType->name,
                    'sleepMs' => $sleepMs,
                    'totalBackoffMs' => $budget->totalBackoffMs(),
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
     * Start one logical operation and return an executor whose execute()
     * calls all charge the same retry budget (issue #223, TXN-18).
     *
     * The client's fan-out operations call this once at their public entry
     * point and pass the returned executor down: every region and page of one
     * `scan()`, `batchGet()`, `commit()` (and so on) therefore shares one
     * `maxBackoffMs`, one `serverBusyBudgetMs` and one wall-clock deadline,
     * instead of getting a fresh budget per region. A later operation calls
     * beginOperation() again (or uses a standalone executor) and starts from
     * zero, so it cannot inherit the earlier operation's attempts or elapsed
     * time.
     *
     * Calling this on an executor that already has a budget deliberately
     * starts a NEW budget: each call marks a new logical operation.
     */
    public function beginOperation(): self
    {
        return new self(
            $this->maxBackoffMs,
            $this->serverBusyBudgetMs,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
            $this->maxAttempts,
            $this->deadlineMs,
            $this->metrics,
            $this->newBudget(),
        );
    }

    /**
     * Budget shared by the execute() calls of the current logical operation,
     * or null for a standalone executor (whose execute() calls each start a
     * fresh budget).
     *
     * A caller that needs the live remaining deadline while an operation runs
     * — lock resolution is the first user (issue #569) — must use an executor
     * returned by {@see self::beginOperation()} and read
     * {@see RetryBudget::remainingDeadlineMs()} from this budget. The object
     * is the same one execute() charges, so there is no second source of
     * truth for the deadline.
     */
    public function currentBudget(): ?RetryBudget
    {
        return $this->budget;
    }

    private function newBudget(): RetryBudget
    {
        return new RetryBudget(
            $this->deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0,
            $this->deadlineMs,
        );
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
        // A store in its health cooldown is not evidence that the cached
        // region is wrong: the region is intact, the store is simply not
        // routable until the cooldown ends (issue #424). Dropping the entry
        // would force a PD round trip on every probe attempt.
        if ($e instanceof StoreUnavailableException) {
            return false;
        }

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
     * Replace the stale cached region with the regions TiKV returned in an
     * EpochNotMatch payload (issue #246, REG-15).
     *
     * Returns true only when the whole payload was usable and at least one
     * returned region contains the key; then the cache is updated and the
     * caller must NOT invalidate the entry. Any other outcome returns false
     * and leaves the ordinary invalidate-and-resolve-through-PD fallback in
     * charge. The cache update itself is not an error-driven drop: `put()`
     * removes the superseded overlapping entries silently, exactly as for a
     * PD scan (issue #238), so no regionInvalidated() metric is emitted for
     * a successful refresh. A fallback still uses
     * 'retry_region_error:epoch_not_match'.
     */
    private function refreshRegionCacheFromEpochNotMatch(TiKvException $e, string $key): bool
    {
        if (!$e instanceof RegionException || $e->errorKind !== ErrorKind::EpochNotMatch) {
            return false;
        }

        $payload = $e->epochNotMatch;
        if (!$payload instanceof EpochNotMatch) {
            return false;
        }

        // RegionErrorHandler::check() captures the entry before invalidating
        // it at TxnKV call sites; RawKV checks run without a cache, so the
        // entry is still there. Either way the leader store is needed to
        // seed the new regions, because metapb.Region carries no leader.
        $staleRegion = $e->cachedRegion ?? $this->regionCache->getByKey($key);
        $regions = $this->regionResolver->regionsFromEpochNotMatch($payload, $staleRegion);
        if ($regions === []) {
            $this->logger->info('EpochNotMatch current_regions unusable; falling back to PD', [
                'key' => KeyRedactor::redact($key),
            ]);

            return false;
        }

        $coveringRegion = null;
        foreach ($regions as $region) {
            if (KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
                $coveringRegion = $region;
                break;
            }
        }

        if (!$coveringRegion instanceof RegionInfo) {
            $this->logger->info('EpochNotMatch current_regions do not cover key; falling back to PD', [
                'key' => KeyRedactor::redact($key),
            ]);

            return false;
        }

        foreach ($regions as $region) {
            $this->regionCache->put($region);
        }

        $this->logger->info('Region cache refreshed from EpochNotMatch current_regions', [
            'key' => KeyRedactor::redact($key),
            'regionId' => $coveringRegion->regionId,
            'regionIds' => array_map(
                static fn (RegionInfo $region): int => $region->regionId,
                $regions,
            ),
        ]);

        return true;
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
     * Drop the store-address cache entry that a retryable error invalidated
     * (issue #247, REG-16).
     *
     * Two errors are evidence about the STORE address rather than the region:
     *
     * - `StoreNotMatch`: TiKV answered because the request reached the wrong
     *   store. The payload names the store ID the request carried, which is
     *   the cache entry to drop. When the payload is absent (a hand-built
     *   exception or a server that left the fields unset), the cached
     *   region's leader store is the best remaining answer.
     * - a `GrpcException`: the transport could not reach the store. The
     *   address may be stale because a rescheduled TiKV pod keeps its store
     *   ID and comes back on a new address; closing the channel alone makes
     *   every retry reopen it against the same dead endpoint.
     *
     * Non-routing retryable errors (`ServerIsBusy`, `DiskFull`, …) never
     * reach this method, so a busy but correctly addressed store keeps its
     * cached address (issue #245).
     *
     * `$cached` is the region the request was routed through, when known.
     * RegionErrorHandler::check() may already have removed it at TxnKV call
     * sites, in which case the StoreNotMatch payload still names the store.
     * `$failedStoreId` is the store the failed attempt actually targeted
     * (from the caller's ReplicaReadTarget), when known.
     */
    private function invalidateStoreForRetry(
        TiKvException $e,
        ?RegionInfo $cached,
        ?int $failedStoreId,
    ): void {
        if ($e instanceof RegionException && $e->errorKind === ErrorKind::StoreNotMatch) {
            $storeId = (int) ($e->storeNotMatch?->getRequestStoreId() ?? 0);
            if ($storeId <= 0) {
                $storeId = $failedStoreId ?? ($cached instanceof RegionInfo ? $cached->leaderStoreId : 0);
            }

            $this->regionResolver->invalidateStore($storeId, 'store_not_match');

            return;
        }

        if ($e instanceof GrpcException) {
            $storeId = $failedStoreId ?? ($cached instanceof RegionInfo ? $cached->leaderStoreId : null);
            if ($storeId !== null) {
                $this->regionResolver->invalidateStore($storeId, 'transport_error');
            }
        }
    }

    /**
     * Record a successful attempt against the store it was routed to
     * (issue #424, GAP-10).
     *
     * The provider is authoritative for reads that may target a follower;
     * the cached region's leader is the fallback for leader-only operations.
     * A null answer (no region cached, no provider) records nothing.
     *
     * @param (callable(): ?int)|null $targetStoreId
     */
    private function recordStoreSuccess(string $key, ?callable $targetStoreId): void
    {
        if ($targetStoreId === null && !$this->regionResolver->hasTrackedStoreFailures()) {
            return;
        }

        $storeId = $this->failedStoreIdFor($key, $targetStoreId);
        if ($storeId !== null) {
            $this->regionResolver->recordStoreSuccess($storeId);
        }
    }

    /**
     * Record a failed attempt against the store the RPC actually targeted
     * (issue #424, GAP-10).
     *
     * The store is taken from the exception's own `targetAddress`, not from
     * the outer operation's provider: a nested RPC (lock resolution, a
     * fan-out sub-call) can fail against a different store, and PD is called
     * inside the same operation. An address that was never resolved to a
     * store — a PD endpoint, a synthetic test address, a parse error — is not
     * guessed and records nothing. This also unwraps
     * {@see CasIndeterminateException}, whose transport failure is otherwise
     * invisible to the tracker (issue #239 keeps the CAS outcome typed).
     *
     * Only `UNAVAILABLE` and `DEADLINE_EXCEEDED` say something about the
     * store's reachability. A region error means the request reached the
     * store, a `StoreNotMatch` means the address was wrong, and a fatal status
     * such as `UNAUTHENTICATED` is a client or request problem, not a dead
     * store.
     */
    private function recordStoreFailure(TiKvException $e): void
    {
        if ($e instanceof CasIndeterminateException) {
            $previous = $e->getPrevious();
            if (!$previous instanceof GrpcException) {
                return;
            }
            $e = $previous;
        }

        if (!$e instanceof GrpcException || $e->targetAddress === null) {
            return;
        }

        // Only statuses that say something about the store's reachability:
        // UNAVAILABLE (the connection failed) and DEADLINE_EXCEEDED (the
        // store did not answer in time). A fatal status such as
        // UNAUTHENTICATED or a ServerBusy-style RESOURCE_EXHAUSTED is about
        // the request or the client configuration, not the store's health,
        // and must not demote it.
        $status = GrpcStatusCode::tryFrom($e->grpcStatusCode);
        if ($status !== GrpcStatusCode::Unavailable && $status !== GrpcStatusCode::DeadlineExceeded) {
            return;
        }

        // A PD transport failure must never demote a TiKV store (issue #424).
        if ($this->regionResolver->isPdEndpointAddress($e->targetAddress)) {
            return;
        }

        $storeId = $this->regionResolver->storeIdForAddress($e->targetAddress);
        if ($storeId === null) {
            return;
        }

        $this->regionResolver->recordStoreFailure($storeId);
    }

    /**
     * The store the last attempt was addressed to, from the caller's provider
     * or the cached region's leader.
     *
     * @param (callable(): ?int)|null $targetStoreId
     */
    private function failedStoreIdFor(string $key, ?callable $targetStoreId): ?int
    {
        $storeId = $this->targetStoreIdOf($targetStoreId);
        if ($storeId !== null) {
            return $storeId;
        }

        $cached = $this->regionCache->getByKey($key);

        return $cached instanceof RegionInfo ? $cached->leaderStoreId : null;
    }

    /**
     * Read the caller's failed-attempt target store ID.
     *
     * A non-positive or non-int answer means "not known" — an unset protobuf
     * field or a caller that did not resolve a target must not evict store 0.
     */
    private function targetStoreIdOf(?callable $provider): ?int
    {
        if ($provider === null) {
            return null;
        }

        $storeId = $provider();

        return is_int($storeId) && $storeId > 0 ? $storeId : null;
    }

    /**
     * Whether a gRPC failure is evidence that the channel itself is broken
     * (issue #248, REG-17).
     *
     * A `DEADLINE_EXCEEDED` on a slow but healthy connection is a retryable
     * transport failure (issue #240), but tearing the channel down for it
     * forces a re-handshake on every slow scan and adds connection churn
     * under load. Only UNAVAILABLE says the connection could not be used at
     * all; the issue's acceptance criteria make that the only status that
     * closes a channel. The store-address invalidation is deliberately not
     * gated by this: a stale address is worth re-resolving even after a
     * timeout.
     */
    private function indicatesBrokenConnection(GrpcException $e): bool
    {
        return $e->grpcStatusCode === GrpcStatusCode::Unavailable->value;
    }

    /**
     * Close the channel behind a transport failure that proves the connection
     * is unusable (issue #248), shared by the retryable and fatal paths
     * (issue #534).
     *
     * The exception carries the address the failed attempt was sent to (set by
     * GrpcClient / GrpcFuture at throw time). That is authoritative even when
     * the RPC was nested inside the retried operation — lock resolution talks
     * to the primary's region, not the read's/write's region. Fall back to the
     * provider/cached leader only for exceptions that predate the field or
     * synthetic test failures.
     */
    private function closeBrokenChannel(GrpcException $e, ?int $failedStoreId): void
    {
        if (!$this->indicatesBrokenConnection($e)) {
            return;
        }

        $failedAddress = $e->targetAddress;
        if ($failedAddress === null && $failedStoreId !== null) {
            // Resolve the channel address BEFORE dropping the store entry:
            // resolveStoreAddress() reads the store cache, and invalidating
            // first would force a PD GetStore just to close a channel.
            try {
                $failedAddress = $this->regionResolver->resolveStoreAddress($failedStoreId);
            } catch (StoreNotFoundException | StoreUnavailableException) {
            }
        }

        if ($failedAddress !== null) {
            $this->grpc->closeChannel($failedAddress);
        }
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

    private function classifyError(TiKvException $e): RetryVerdict
    {
        return ErrorClassifier::classify($e);
    }
}
