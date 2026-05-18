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
    ) {
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @param (callable(TiKvException): ?BackoffType)|null $classifier Custom error classifier
     * @return T
     */
    public function execute(string $key, callable $operation, ?callable $classifier = null): mixed
    {
        $this->totalBackoffMs = 0;
        $this->serverBusyBackoffMs = 0;
        $this->attempt = 0;

        while (true) {
            try {
                return $operation();
            } catch (TiKvException $e) {
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

                $this->attempt++;
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
