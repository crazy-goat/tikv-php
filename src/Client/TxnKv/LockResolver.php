<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class LockResolver
{
    public function __construct(
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private RegionCacheInterface $regionCache,
        private int $maxBackoffMs = 20000,
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function resolveLock(string $key, int $lockTs, int $callerStartTs): void
    {
        $this->logger->debug('Resolving lock', ['key' => KeyRedactor::redact($key), 'lockTs' => $lockTs]);

        $status = $this->checkTxnStatus($key, $lockTs, $callerStartTs);

        $commitTs = $status['commitTs'] ?? null;
        $action = $status['action'] ?? null;

        if ($commitTs !== null && $commitTs > 0) {
            $this->resolveLockCommitted($key, $lockTs, $commitTs);
        } elseif ($action === 'Lock' || $commitTs === 0) {
            $ttl = $status['lockTtl'] ?? 0;
            if ($ttl > 0) {
                $sleepMs = min($ttl, $this->maxBackoffMs);
                $this->logger->debug('Lock still active, waiting', [
                    'key' => KeyRedactor::redact($key),
                    'ttl' => $ttl,
                    'sleepMs' => $sleepMs,
                ]);
                usleep($sleepMs * 1000);
            }

            $status = $this->checkTxnStatus($key, $lockTs, $callerStartTs);
            $commitTs = $status['commitTs'] ?? null;

            if ($commitTs !== null && $commitTs > 0) {
                $this->resolveLockCommitted($key, $lockTs, $commitTs);
            } else {
                $this->resolveLockRolledBack($key, $lockTs);
            }
        } else {
            $this->resolveLockRolledBack($key, $lockTs);
        }

        $this->invalidateRegionFor($key);
    }

    public function getGrpc(): GrpcClientInterface
    {
        return $this->grpc;
    }

    /**
     * @return array{commitTs: ?int, action: ?string, lockTtl: ?int}
     */
    private function checkTxnStatus(string $primaryLock, int $lockTs, int $callerStartTs): array
    {
        $region = $this->regionResolver->getRegionInfo($primaryLock);
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new CheckTxnStatusRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setPrimaryKey($primaryLock);
        $request->setLockTs($lockTs);
        $request->setCallerStartTs($callerStartTs);
        $request->setCurrentTs((int) (hrtime(true) / 1_000_000));
        $request->setRollbackIfNotExist(true);

        $this->logger->debug('CheckTxnStatus', [
            'primaryLock' => KeyRedactor::redact($primaryLock),
            'lockTs' => $lockTs,
        ]);

        /** @var CheckTxnStatusResponse $response */
        $response = $this->grpc->call(
            $address,
            'tikvpb.Tikv',
            'KvCheckTxnStatus',
            $request,
            CheckTxnStatusResponse::class,
        );

        RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

        $error = $response->getError();
        if ($error !== null) {
            $this->logger->warning('CheckTxnStatus returned error', [
                'primaryLock' => KeyRedactor::redact($primaryLock),
                'lockTs' => $lockTs,
            ]);
        }

        $action = $response->getAction();
        $actionName = null;
        if ($action !== 0) {
            $actionName = (string) $action;
        }

        return [
            'commitTs' => (int) $response->getCommitVersion(),
            'action' => $actionName,
            'lockTtl' => (int) $response->getLockTtl(),
        ];
    }

    private function resolveLockCommitted(string $key, int $lockTs, int $commitTs): void
    {
        $this->logger->debug('Resolving lock as committed', [
            'key' => KeyRedactor::redact($key),
            'lockTs' => $lockTs,
            'commitTs' => $commitTs,
        ]);

        $region = $this->regionResolver->getRegionInfo($key);
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new ResolveLockRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setStartVersion($lockTs);
        $request->setCommitVersion($commitTs);

        $this->grpc->call($address, 'tikvpb.Tikv', 'KvResolveLock', $request, ResolveLockResponse::class);
    }

    private function resolveLockRolledBack(string $key, int $lockTs): void
    {
        $this->logger->debug('Resolving lock as rolled back', [
            'key' => KeyRedactor::redact($key),
            'lockTs' => $lockTs,
        ]);

        $region = $this->regionResolver->getRegionInfo($key);
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new ResolveLockRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setStartVersion($lockTs);
        $request->setCommitVersion(0);

        $this->grpc->call($address, 'tikvpb.Tikv', 'KvResolveLock', $request, ResolveLockResponse::class);
    }

    private function invalidateRegionFor(string $key): void
    {
        $region = $this->regionCache->getByKey($key);
        if ($region instanceof RegionInfo) {
            $this->regionCache->invalidate($region->regionId);
        }
    }
}
