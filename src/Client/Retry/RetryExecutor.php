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
     * @return T
     */
    public function execute(string $key, callable $operation, ?callable $classifier = null): mixed
    {
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
                        'Retry attempt cap (%d) exhausted for key "%s"',
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
                        $this->logger->error('Fatal error, not retrying', [
                            'key' => KeyRedactor::redact($key),
                            'error' => $e->getMessage(),
                        ]);
                        throw $e;
                    }

                    $cached = $this->regionCache->getByKey($key);
                    if ($cached instanceof RegionInfo) {
                        // The cache itself emits regionInvalidated() with
                        // reason 'retry_region_error' — do not emit here too.
                        $this->regionCache->invalidate($cached->regionId, 'retry_region_error');
                        $this->logger->info('Invalidated region on retry', [
                            'key' => KeyRedactor::redact($key),
                            'regionId' => $cached->regionId,
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
                'Retry deadline (%d ms) exhausted for key "%s"',
                $this->deadlineMs,
                KeyRedactor::redact($key),
            ),
            $attempt,
            $elapsedMs,
            $lastError,
            rawKey: $key,
        );
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
