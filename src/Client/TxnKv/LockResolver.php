<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
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

    /**
     * Resolve a lock by checking the transaction's status and either
     * committing it (if committed elsewhere) or rolling it back.
     *
     * @param string $primaryLock The primary key of the transaction (from LockInfo::getPrimaryLock()).
     *                            If the lock has no primary info (e.g. pessimistic), pass the locked key itself.
     * @param LockInfo $lock The lock information from the error response.
     */
    public function resolveLock(string $primaryLock, LockInfo $lock): void
    {
        $lockTs = (int) $lock->getLockVersion();
        $this->logger->debug('Resolving lock', [
            'key' => KeyRedactor::redact((string) $lock->getKey()),
            'lockTs' => $lockTs,
        ]);

        $status = $this->checkTxnStatus($primaryLock, $lockTs);

        $commitTs = $status['commitTs'] ?? null;

        if ($commitTs !== null && $commitTs > 0) {
            $this->resolveLockCommitted($lock, $lockTs, $commitTs);
        } elseif ($commitTs === 0) {
            $ttl = $status['lockTtl'] ?? 0;
            if ($ttl > 0) {
                $sleepMs = min($ttl, $this->maxBackoffMs);
                $this->logger->debug('Lock still active, waiting', [
                    'key' => KeyRedactor::redact((string) $lock->getKey()),
                    'ttl' => $ttl,
                    'sleepMs' => $sleepMs,
                ]);
                usleep($sleepMs * 1000);
            }

            $status = $this->checkTxnStatus($primaryLock, $lockTs);
            $commitTs = $status['commitTs'] ?? null;

            if ($commitTs !== null && $commitTs > 0) {
                $this->resolveLockCommitted($lock, $lockTs, $commitTs);
            } else {
                $this->resolveLockRolledBack($lock, $lockTs);
            }
        } else {
            $this->resolveLockRolledBack($lock, $lockTs);
        }

        $this->invalidateRegionFor((string) $lock->getKey());
    }

    public function getGrpc(): GrpcClientInterface
    {
        return $this->grpc;
    }

    /**
     * @return array{commitTs: ?int, lockTtl: ?int}
     */
    private function checkTxnStatus(string $primaryKey, int $lockTs): array
    {
        $region = $this->regionResolver->getRegionInfo($primaryKey);
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new CheckTxnStatusRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setPrimaryKey($primaryKey);
        $request->setLockTs($lockTs);
        $request->setCallerStartTs((int) (hrtime(true) / 1_000_000));
        $request->setCurrentTs((int) (hrtime(true) / 1_000_000));
        $request->setRollbackIfNotExist(true);

        $this->logger->debug('CheckTxnStatus', [
            'primaryKey' => KeyRedactor::redact($primaryKey),
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
                'primaryKey' => KeyRedactor::redact($primaryKey),
                'lockTs' => $lockTs,
            ]);
        }

        $action = $response->getAction();
        $lockTtl = 0;

        // Use Action enum constants instead of string comparison.
        // When action is NoAction or MinCommitTSPushed, the lock is still
        // active (caller should wait/retry).
        if ($action === Action::NoAction || $action === Action::MinCommitTSPushed) {
            $lockTtl = (int) $response->getLockTtl();
        }

        return [
            'commitTs' => (int) $response->getCommitVersion(),
            'lockTtl' => $lockTtl,
        ];
    }

    private function resolveLockCommitted(LockInfo $lock, int $lockTs, int $commitTs): void
    {
        $key = (string) $lock->getKey();
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

    private function resolveLockRolledBack(LockInfo $lock, int $lockTs): void
    {
        $key = (string) $lock->getKey();
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
