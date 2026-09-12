<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusRequest;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockRequest;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
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
        private PdClientInterface $pdClient,
        private int $callerStartTs,
        private TimeoutConfig $timeoutConfig = new TimeoutConfig(),
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
     * @param int $remainingDeadlineMs Remaining wall-clock budget (ms) of the calling
     *                                 operation's retry deadline. When > 0, the lock-TTL
     *                                 wait is capped by it so a single lock encounter
     *                                 cannot push the operation past its deadline
     *                                 (issue #470); 0 keeps the legacy maxBackoffMs-only cap.
     * @param bool $notLeaderOwnedByRetryExecutor Whether a RetryExecutor owns NotLeader
     *                                            handling around this resolveLock() call
     *                                            (see RegionErrorHandler::check()). Call
     *                                            sites with no enclosing execute() — e.g.
     *                                            pessimisticLockBatch()'s do-while — must
     *                                            pass false so a NotLeader-carrying region
     *                                            error still invalidates instead of
     *                                            stranding a stale cache entry up to TTL.
     *                                            Kept last so the #470 positional
     *                                            signature stays stable.
     */
    public function resolveLock(
        string $primaryLock,
        LockInfo $lock,
        int $remainingDeadlineMs = 0,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): void {
        $lockTs = (int) $lock->getLockVersion();
        $this->logger->debug('Resolving lock', [
            'key' => KeyRedactor::redact((string) $lock->getKey()),
            'lockTs' => $lockTs,
        ]);

        $status = $this->checkTxnStatus($primaryLock, $lockTs, $notLeaderOwnedByRetryExecutor);

        $commitTs = $status['commitTs'] ?? null;

        if ($commitTs !== null && $commitTs > 0) {
            $this->resolveLockCommitted($lock, $lockTs, $commitTs);
        } elseif ($commitTs === 0) {
            // Async-commit lock (issue #419): the primary lock carries the
            // commit decision, so before spending the TTL wait ask the
            // secondary regions for their state via KvCheckSecondaryLocks.
            /** @var list<string> $asyncSecondaries */
            $asyncSecondaries = iterator_to_array($lock->getSecondaries());
            $secondary = $lock->getUseAsyncCommit() && $asyncSecondaries !== []
                ? $this->checkSecondaryLocks($lock, $notLeaderOwnedByRetryExecutor)
                : null;

            if ($secondary !== null) {
                // Determined (committed/rolled back) or undecided-but-
                // help-committable: finalize every lock of the transaction.
                $this->resolveAsyncCommitLocks(
                    $lock,
                    $lockTs,
                    $secondary['commitTs'],
                    $asyncSecondaries,
                );
                $this->invalidateRegionFor((string) $lock->getKey());
                return;
            }

            $ttl = $status['lockTtl'] ?? 0;
            if ($ttl > 0) {
                // The wait used to be charged to no budget (issue #470): cap
                // it by the caller's remaining retry deadline when provided.
                $deadlineCap = $remainingDeadlineMs > 0 ? $remainingDeadlineMs : $this->maxBackoffMs;
                $sleepMs = min($ttl, $deadlineCap);
                $this->logger->debug('Lock still active, waiting', [
                    'key' => KeyRedactor::redact((string) $lock->getKey()),
                    'ttl' => $ttl,
                    'sleepMs' => $sleepMs,
                    'remainingDeadlineMs' => $remainingDeadlineMs,
                ]);
                usleep($sleepMs * 1000);
            }

            $status = $this->checkTxnStatus($primaryLock, $lockTs, $notLeaderOwnedByRetryExecutor);
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
    private function checkTxnStatus(
        string $primaryKey,
        int $lockTs,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): array {
        $region = $this->regionResolver->getRegionInfo($primaryKey);
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new CheckTxnStatusRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setPrimaryKey($primaryKey);
        $request->setLockTs($lockTs);
        // Both fields must be PD TSO MVCC timestamps: a monotonic-clock value
        // breaks TiKV's TTL expiry and min-commit-ts logic (see issue #270).
        $request->setCallerStartTs($this->callerStartTs);
        // Low-resolution timestamp (issue #420, GAP-06 / DIV-02): lock
        // resolution is staleness-tolerant — with a configured staleness
        // bound it reuses the cached TSO timestamp and saves a PD round
        // trip. Without a bound this is a fresh TSO fetch, unchanged.
        $request->setCurrentTs($this->pdClient->getLowResolutionTimestamp($this->timeoutConfig->writeTimeoutMs));
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


        RegionErrorHandler::check(
            $response,
            $this->regionCache,
            $region->regionId,
            notLeaderOwnedByRetryExecutor: $notLeaderOwnedByRetryExecutor,
        );

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

    /**
     * Check the secondary locks of an async-commit transaction
     * (KvCheckSecondaryLocks, issue #419).
     *
     * Mirrors client-go's checkAllSecondaries()/addKeys() recovery protocol:
     * - Some secondary already determined (its lock is gone) → the response's
     *   commit_ts is the transaction's final commit_ts (0 = rolled back).
     * - Every secondary is still locked → the transaction is undecided but
     *   the reader may help-commit it: the commit ts is max() over the
     *   primary lock's and all secondary locks' min_commit_ts, and ALL keys
     *   (secondaries + primary) are resolved with ResolveLock at that ts.
     *
     * @return array{state: 'committed'|'rolledback'|'undecided', commitTs: int}|null
     *         null when the lock carries no secondaries or a region error
     *         occurs (caller falls back to the TTL-wait path).
     */
    private function checkSecondaryLocks(
        LockInfo $lock,
        bool $notLeaderOwnedByRetryExecutor,
    ): ?array {
        /** @var list<string> $secondaries */
        $secondaries = array_map(
            static fn (string $key): string => $key,
            iterator_to_array($lock->getSecondaries()),
        );
        if ($secondaries === []) {
            return null;
        }

        try {
            $keysByRegion = RegionGrouper::groupKeysByRegionBatch(
                $secondaries,
                $this->regionResolver,
            );
        } catch (RegionException) {
            return null;
        }

        $lockTs = (int) $lock->getLockVersion();
        // client-go initializes the derived commit ts with the primary
        // lock's min_commit_ts.
        $maxMinCommitTs = (int) $lock->getMinCommitTs();
        $maxCommitTs = 0;
        $undecided = true;
        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new CheckSecondaryLocksRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKeys($regionKeys);
            $request->setStartVersion((int) $lock->getLockVersion());

            $this->logger->debug('CheckSecondaryLocks', [
                'regionId' => $region->regionId,
                'keyCount' => count($regionKeys),
            ]);

            /** @var CheckSecondaryLocksResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvCheckSecondaryLocks',
                $request,
                CheckSecondaryLocksResponse::class,
            );

            RegionErrorHandler::check(
                $response,
                $this->regionCache,
                $region->regionId,
                notLeaderOwnedByRetryExecutor: $notLeaderOwnedByRetryExecutor,
            );

            $responseLocks = $response->getLocks();
            if (count($responseLocks) < count($regionKeys)) {
                // A secondary lock is missing: the transaction has been
                // determined (committed or rolled back) — the response's
                // commit_ts is the final decision.
                $undecided = false;
                $maxCommitTs = max($maxCommitTs, (int) $response->getCommitTs());
            } else {
                // All secondaries of this region still locked: collect
                // their min_commit_ts for the help-commit derivation.
                foreach ($responseLocks as $secondaryLock) {
                    if ((int) $secondaryLock->getLockVersion() !== $lockTs) {
                        continue;
                    }
                    $maxMinCommitTs = max($maxMinCommitTs, (int) $secondaryLock->getMinCommitTs());
                }
            }
        }

        if ($undecided) {
            // The transaction is still undecided, but the reader may
            // help-commit it at max(min_commit_ts) — exactly what client-go
            // does in resolveAsyncResolveData() after checkAllSecondaries().
            return ['state' => 'undecided', 'commitTs' => $maxMinCommitTs];
        }
        return [
            'state' => $maxCommitTs > 0 ? 'committed' : 'rolledback',
            'commitTs' => $maxCommitTs,
        ];
    }

    /**
     * Finalize an async-commit transaction's locks: resolve every key
     * (all secondaries + the primary) with ResolveLock at the derived
     * commit ts (0 rolls the transaction back).
     *
     * @param list<string> $secondaries
     */
    private function resolveAsyncCommitLocks(LockInfo $lock, int $lockTs, int $commitTs, array $secondaries): void
    {
        $keys = [...$secondaries, (string) $lock->getKey()];

        try {
            $keysByRegion = RegionGrouper::groupKeysByRegionBatch($keys, $this->regionResolver);
        } catch (RegionException $e) {
            $this->logger->warning('Async-commit resolve failed to group keys', [
                'key' => KeyRedactor::redact((string) $lock->getKey()),
                'lockTs' => $lockTs,
                'commitTs' => $commitTs,
                'error' => $e->getMessage(),
            ]);
            return;
        }

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new ResolveLockRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartVersion($lockTs);
            $request->setCommitVersion($commitTs);

            $this->logger->debug('Async-commit resolve locks', [
                'regionId' => $region->regionId,
                'keyCount' => count($regionData['keys']),
                'lockTs' => $lockTs,
                'commitTs' => $commitTs,
            ]);

            /** @var ResolveLockResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvResolveLock',
                $request,
                ResolveLockResponse::class,
            );

            RegionErrorHandler::check(
                $response,
                $this->regionCache,
                $region->regionId,
                notLeaderOwnedByRetryExecutor: false,
            );
        }
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
            // The cache emits regionInvalidated('lock_resolve') itself.
            $this->regionCache->invalidate($region->regionId, 'lock_resolve');
        }
    }
}
