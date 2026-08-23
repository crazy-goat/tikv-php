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
     * independently of accumulated backoff time — ensures that errors
     * classified as BackoffType::None (e.g. EpochNotMatch) with sleepMs=0
     * cannot drive an infinite, zero-delay busy loop.
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
        private int $deadlineMs = 0,
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
            // Catches zero-backoff errors (e.g. EpochNotMatch classified as
            // BackoffType::None) that would otherwise drive an infinite loop.
            if ($attempt >= $this->maxAttempts) {
                $this->logger->error('Retry attempt cap exhausted', [
                    'key' => KeyRedactor::redact($key),
                    'attempt' => $attempt,
                    'maxAttempts' => $this->maxAttempts,
                    'totalBackoffMs' => $totalBackoffMs,
                ]);
                throw new RetryBudgetExhaustedException(
                    sprintf('Retry attempt cap (%d) exhausted for key "%s"', $this->maxAttempts, $key),
                    $attempt,
                    $totalBackoffMs,
                    $lastError,
                );
            }

            // Enforce wall-clock deadline (if configured).
            if ($this->deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $this->deadlineMs) {
                    $this->logger->error('Retry deadline exhausted', [
                        'key' => KeyRedactor::redact($key),
                        'attempt' => $attempt,
                        'elapsedMs' => $elapsedMs,
                        'deadlineMs' => $this->deadlineMs,
                    ]);
                    throw new RetryBudgetExhaustedException(
                        sprintf('Retry deadline (%d ms) exhausted for key "%s"', $this->deadlineMs, $key),
                        $attempt,
                        $elapsedMs,
                        $lastError,
                    );
                }
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
                        $this->regionCache->invalidate($cached->regionId);
                        $this->metrics->regionInvalidated('retry_region_error');
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
                    usleep($sleepMs * 1000);
                }

                $attempt++;
            }
        }
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
                $this->regionCache->invalidate($regionId);
                $this->logger->info('NotLeader hint peer unknown, invalidated region', [
                    'key' => KeyRedactor::redact($key),
                    'regionId' => $regionId,
                    'hintStoreId' => $leaderStoreId,
                ]);
            }
        } else {
            $this->regionCache->invalidate($regionId);
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
