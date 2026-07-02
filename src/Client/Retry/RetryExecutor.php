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
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use Psr\Log\LoggerInterface;

final class RetryExecutor
{
    /**
     * Default maximum number of attempts per call. Bounds the retry loop
     * independently of accumulated backoff time — ensures that errors
     * classified as BackoffType::None (e.g. EpochNotMatch) with sleepMs=0
     * cannot drive an infinite, zero-delay busy loop.
     */
    public const DEFAULT_MAX_ATTEMPTS = 30;

    private int $totalBackoffMs = 0;
    private int $serverBusyBackoffMs = 0;
    private int $attempt = 0;

    public function __construct(
        private readonly int $maxBackoffMs,
        private readonly int $serverBusyBudgetMs,
        private readonly RegionCacheInterface $regionCache,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionResolver $regionResolver,
        private readonly LoggerInterface $logger,
        private readonly int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        private readonly int $deadlineMs = 0,
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
        $this->attempt = 0;
        $startTimeMs = $this->deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;
        $lastError = null;

        while (true) {
            // Enforce absolute attempt cap before running the operation.
            // Catches zero-backoff errors (e.g. EpochNotMatch classified as
            // BackoffType::None) that would otherwise drive an infinite loop.
            if ($this->attempt >= $this->maxAttempts) {
                $this->logger->error('Retry attempt cap exhausted', [
                    'key' => $key,
                    'attempt' => $this->attempt,
                    'maxAttempts' => $this->maxAttempts,
                    'totalBackoffMs' => $this->totalBackoffMs,
                ]);
                throw new RetryBudgetExhaustedException(
                    sprintf('Retry attempt cap (%d) exhausted for key "%s"', $this->maxAttempts, $key),
                    $this->attempt,
                    $this->totalBackoffMs,
                    $lastError,
                );
            }

            // Enforce wall-clock deadline (if configured).
            if ($this->deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $this->deadlineMs) {
                    $this->logger->error('Retry deadline exhausted', [
                        'key' => $key,
                        'attempt' => $this->attempt,
                        'elapsedMs' => $elapsedMs,
                        'deadlineMs' => $this->deadlineMs,
                    ]);
                    throw new RetryBudgetExhaustedException(
                        sprintf('Retry deadline (%d ms) exhausted for key "%s"', $this->deadlineMs, $key),
                        $this->attempt,
                        $elapsedMs,
                        $lastError,
                    );
                }
            }

            try {
                return $operation();
            } catch (TiKvException $e) {
                $lastError = $e;
                $this->attempt++;

                $backoffType = $this->handleNotLeader($e, $key);

                if (!$backoffType instanceof BackoffType) {
                    if ($classifier !== null) {
                        $backoffType = $classifier($e);
                    }

                    if (!$backoffType instanceof BackoffType) {
                        $backoffType = $this->classifyError($e);
                    }

                    if (!$backoffType instanceof BackoffType) {
                        $this->logger->error('Fatal error, not retrying', ['key' => $key, 'error' => $e->getMessage()]);
                        throw $e;
                    }

                    $cached = $this->regionCache->getByKey($key);
                    if ($cached instanceof RegionInfo) {
                        $this->regionCache->invalidate($cached->regionId);
                        $this->logger->info('Invalidated region on retry', [
                            'key' => $key,
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

                $sleepMs = $backoffType->sleepMs($this->attempt);

                if ($backoffType === BackoffType::ServerBusy) {
                    $this->serverBusyBackoffMs += $sleepMs;
                    if ($this->serverBusyBackoffMs > $this->serverBusyBudgetMs) {
                        $this->logger->error('ServerBusy budget exhausted', [
                            'key' => $key,
                            'attempt' => $this->attempt,
                            'serverBusyBackoffMs' => $this->serverBusyBackoffMs,
                            'serverBusyBudgetMs' => $this->serverBusyBudgetMs,
                        ]);
                        throw $e;
                    }
                } else {
                    $this->totalBackoffMs += $sleepMs;
                    if ($this->totalBackoffMs > $this->maxBackoffMs) {
                        $this->logger->error('Retry budget exhausted', [
                            'key' => $key,
                            'attempt' => $this->attempt,
                            'totalBackoffMs' => $this->totalBackoffMs,
                            'maxBackoffMs' => $this->maxBackoffMs,
                        ]);
                        throw $e;
                    }
                }

                $this->logger->warning('Retrying operation', [
                    'key' => $key,
                    'attempt' => $this->attempt,
                    'backoffType' => $backoffType->name,
                    'sleepMs' => $sleepMs,
                    'totalBackoffMs' => $this->totalBackoffMs,
                ]);

                if ($sleepMs > 0) {
                    usleep($sleepMs * 1000);
                }
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
                    'key' => $key,
                    'regionId' => $regionId,
                    'hintStoreId' => $leaderStoreId,
                ]);
            }
        } else {
            $this->regionCache->invalidate($regionId);
            $this->logger->info('NotLeader without hint, invalidated region', [
                'key' => $key,
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
