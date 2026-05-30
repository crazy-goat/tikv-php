<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchGetResponse;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\Mutation;
use CrazyGoat\Proto\Kvrpcpb\Op;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticLockResponse;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\PessimisticRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest\PessimisticAction;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\RawKv\RegionGrouper;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class Transaction
{
    private ?int $commitTs = null;
    private TransactionStatus $status = TransactionStatus::Active;
    /** @var array<string, ?string> key => value. null value means delete */
    private array $writeSet = [];
    /** @var array<string, ?string> key => value read (for read-set tracking) */
    private array $readSet = [];
    /** @var string[] keys pending pessimistic lock (batched at commit time) */
    private array $pendingLockKeys = [];
    private bool $closed = false;
    private ?RetryExecutor $retryExecutor = null;

    private const OPTIMISTIC_LOCK_TTL_MS = 3000;
    private const PESSIMISTIC_LOCK_TTL_MS = 30000;
    /** TiKV KvScan treats limit=0 as "return 0 results"; use uint32 max for "unlimited" */
    private const DEFAULT_SCAN_LIMIT = 4294967295;

    public function __construct(
        private readonly string $txnId,
        private readonly int $startTs,
        private readonly bool $pessimistic,
        private readonly int $priority,
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache,
        private readonly LockResolver $lockResolver,
        private readonly RegionResolver $regionResolver,
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function getTxnId(): string
    {
        return $this->txnId;
    }

    public function getStartTs(): int
    {
        return $this->startTs;
    }

    public function getCommitTs(): ?int
    {
        return $this->commitTs;
    }

    public function getStatus(): TransactionStatus
    {
        return $this->status;
    }

    public function isPessimistic(): bool
    {
        return $this->pessimistic;
    }

    public function getPriority(): int
    {
        return $this->priority;
    }

    public function get(string $key): ?string
    {
        $this->ensureActive();

        if (array_key_exists($key, $this->writeSet)) {
            return $this->writeSet[$key];
        }

        return $this->executeWithRetry($key, function () use ($key): ?string {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new GetRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKey($key);
            $request->setVersion($this->startTs);

            /** @var GetResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvGet',
                $request,
                GetResponse::class,
            );

            $this->handleRegionError($response, $region);

            $error = $response->getError();
            if ($error !== null) {
                $locked = $error->getLocked();
                if ($locked !== null) {
                    $this->lockResolver->resolveLock(
                        $key,
                        (int) $locked->getLockVersion(),
                        $this->startTs,
                    );
                    throw new TiKvException('Lock encountered, resolved - retry');
                }

                $retryable = $error->getRetryable();
                if ($retryable !== '') {
                    throw new TransactionConflictException($retryable);
                }
            }

            if ($response->getNotFound()) {
                $this->readSet[$key] = null;
                return null;
            }

            $value = $response->getValue();
            $this->readSet[$key] = $value !== '' ? $value : null;
            return $this->readSet[$key];
        });
    }

    /**
     * @param string[] $keys
     * @return array<string, ?string>
     */
    public function batchGet(array $keys): array
    {
        $this->ensureActive();

        $results = [];
        $remaining = [];
        foreach ($keys as $key) {
            if (array_key_exists($key, $this->writeSet)) {
                $results[$key] = $this->writeSet[$key];
            } else {
                $remaining[] = $key;
            }
        }

        if ($remaining !== []) {
            $remoteResults = $this->batchGetFromTiKV($remaining);
            $results = array_merge($results, $remoteResults);
        }

        $ordered = [];
        foreach ($keys as $key) {
            $ordered[$key] = $results[$key] ?? null;
        }
        return $ordered;
    }

    public function set(string $key, string $value): void
    {
        $this->ensureActive();

        if ($this->pessimistic) {
            $this->pendingLockKeys[] = $key;
        }

        $this->writeSet[$key] = $value;
    }

    public function delete(string $key): void
    {
        $this->ensureActive();

        if ($this->pessimistic) {
            $this->pendingLockKeys[] = $key;
        }

        $this->writeSet[$key] = null;
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    public function scan(string $startKey, string $endKey, int $limit = 0): array
    {
        $this->ensureActive();

        $regions = $this->pdClient->scanRegions($startKey, $endKey, 0);
        $results = [];
        $remaining = $limit;

        foreach ($regions as $region) {
            $scanStart = $startKey > $region->startKey ? $startKey : $region->startKey;
            $scanEnd = $endKey === ''
                ? $region->endKey
                : ($region->endKey !== '' && $endKey > $region->endKey ? $region->endKey : $endKey);

            if ($scanStart >= $scanEnd && $scanEnd !== '') {
                continue;
            }

            $regionLimit = $remaining === 0 ? self::DEFAULT_SCAN_LIMIT : $remaining;
            $regionResults = $this->executeScanForRegion(
                $region,
                $scanStart,
                $scanEnd,
                $regionLimit,
            );
            array_push($results, ...$regionResults);

            if ($remaining > 0) {
                $remaining -= count($regionResults);
                if ($remaining <= 0) {
                    break;
                }
            }
        }

        $merged = [];
        foreach ($results as $entry) {
            $key = $entry['key'];
            if (array_key_exists($key, $this->writeSet)) {
                if ($this->writeSet[$key] !== null) {
                    $merged[] = ['key' => $key, 'value' => $this->writeSet[$key]];
                }
            } else {
                $merged[] = $entry;
            }
        }

        return $merged;
    }

    public function commit(): void
    {
        $this->ensureActive();

        if ($this->writeSet === []) {
            $this->status = TransactionStatus::Committed;
            $this->closeTransaction();
            return;
        }

        if ($this->pessimistic) {
            $this->commitPessimistic();
        } else {
            $this->commitOptimistic();
        }
    }

    public function rollback(): void
    {
        $this->ensureActive();

        if ($this->writeSet === []) {
            $this->status = TransactionStatus::RolledBack;
            $this->closeTransaction();
            return;
        }

        if ($this->pessimistic) {
            $this->pessimisticRollbackAll();
        }

        $this->batchRollback(array_keys($this->writeSet));

        $this->writeSet = [];
        $this->readSet = [];
        $this->commitTs = null;
        $this->status = TransactionStatus::RolledBack;
        $this->closeTransaction();
    }

    public function heartbeat(int $adviseLockTtlMs = 10000): int
    {
        $this->ensureActive();

        $primary = $this->getPrimaryKey();

        return $this->executeWithRetry($primary, function () use ($primary, $adviseLockTtlMs): int {
            $region = $this->regionResolver->getRegionInfo($primary);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new TxnHeartBeatRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setPrimaryLock($primary);
            $request->setStartVersion($this->startTs);
            $request->setAdviseLockTtl($adviseLockTtlMs);

            $this->logger->debug('TxnHeartBeat', [
                'txnId' => $this->txnId,
                'primary' => $primary,
                'adviseLockTtlMs' => $adviseLockTtlMs,
            ]);

            /** @var TxnHeartBeatResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvTxnHeartBeat',
                $request,
                TxnHeartBeatResponse::class,
            );

            $this->handleRegionError($response, $region);

            $error = $response->getError();
            if ($error !== null) {
                throw new TiKvException('Heartbeat failed: key error');
            }

            return (int) $response->getLockTtl();
        });
    }

    /**
     * @return array<string, ?string>
     */
    public function getWriteSet(): array
    {
        return $this->writeSet;
    }

    /**
     * @return array<string, ?string>
     */
    public function getReadSet(): array
    {
        return $this->readSet;
    }

    private function commitOptimistic(): void
    {
        $primary = $this->getPrimaryKey();
        $mutations = $this->buildMutations();
        $keysByRegion = $this->groupMutationsByRegion($mutations);

        $allKeys = array_keys($this->writeSet);
        $firstRegionKeys = null;

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionMutations = $regionData['mutations'];
            $isPrimaryRegion = $firstRegionKeys === null;

            if ($firstRegionKeys === null) {
                $firstRegionKeys = array_map(
                    fn(Mutation $m) => $m->getKey(),
                    $regionMutations,
                );
            }

            $this->prewriteForRegion($region, $regionMutations, $primary);
        }

        $this->commitTs = $this->pdClient->getTimestamp();
        $this->commitKeys($allKeys);

        $this->status = TransactionStatus::Committed;
        $this->closeTransaction();
    }

    private function commitPessimistic(): void
    {
        $primary = $this->getPrimaryKey();
        $this->pessimisticLockBatch($primary);

        $mutations = $this->buildMutations();
        $keysByRegion = $this->groupMutationsByRegion($mutations);
        $allKeys = array_keys($this->writeSet);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionMutations = $regionData['mutations'];
            $this->prewriteForRegion($region, $regionMutations, $primary);
        }

        $this->commitTs = $this->pdClient->getTimestamp();
        $this->commitKeys($allKeys);

        $this->status = TransactionStatus::Committed;
        $this->closeTransaction();
    }

    /**
     * @param Mutation[] $mutations
     */
    private function prewriteForRegion(
        RegionInfo $region,
        array $mutations,
        string $primary,
    ): void {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new PrewriteRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setMutations($mutations);
        $request->setPrimaryLock($primary);
        $request->setStartVersion($this->startTs);
        $request->setLockTtl(self::OPTIMISTIC_LOCK_TTL_MS);

        if ($this->pessimistic) {
            $forUpdateTs = $this->startTs;
            $request->setForUpdateTs($forUpdateTs);
            $request->setLockTtl(self::PESSIMISTIC_LOCK_TTL_MS);
            $actions = [];
            foreach ($mutations as $mutation) {
                $actions[] = PessimisticAction::DO_PESSIMISTIC_CHECK;
            }
            $request->setPessimisticActions($actions);
        }

        $this->logger->debug('Prewrite', [
            'txnId' => $this->txnId,
            'regionId' => $region->regionId,
            'keyCount' => count($mutations),
            'pessimistic' => $this->pessimistic,
        ]);

        /** @var PrewriteResponse $response */
        $response = $this->grpc->call(
            $address,
            'tikvpb.Tikv',
            'KvPrewrite',
            $request,
            PrewriteResponse::class,
        );
        $this->handleRegionError($response, $region);

        $errors = $response->getErrors();
        if (count($errors) > 0) {
            $this->handlePrewriteErrors($errors);
        }
    }

    /**
     * @param iterable<KeyError> $errors
     */
    private function handlePrewriteErrors(iterable $errors): void
    {
        foreach ($errors as $keyError) {
            $locked = $keyError->getLocked();
            if ($locked !== null) {
                $this->lockResolver->resolveLock(
                    $locked->getKey(),
                    (int) $locked->getLockVersion(),
                    $this->startTs,
                );
                throw new TiKvException('Lock conflict during prewrite, resolved - retry');
            }

            $conflict = $keyError->getConflict();
            if ($conflict !== null) {
                throw new TransactionConflictException('Write conflict during prewrite');
            }

            $retryable = $keyError->getRetryable();
            if ($retryable !== '') {
                throw new TransactionConflictException($retryable);
            }

            $abort = $keyError->getAbort();
            if ($abort !== '') {
                throw new TransactionConflictException($abort);
            }
        }
    }

    private function handleCommitError(KeyError $error): void
    {
        $retryable = $error->getRetryable();
        if ($retryable !== '') {
            throw new TransactionConflictException($retryable);
        }
        $abort = $error->getAbort();
        if ($abort !== '') {
            throw new TransactionConflictException($abort);
        }
    }

    /**
     * @param string[] $keys
     */
    private function commitKeys(array $keys): void
    {
        if ($keys === []) {
            return;
        }

        $keysByRegion = $this->groupStringsByRegion($keys);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $this->commitForRegion($region, $regionKeys);
        }
    }

    /**
     * @param string[] $keys
     */
    private function commitForRegion(RegionInfo $region, array $keys): void
    {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new CommitRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setStartVersion($this->startTs);
        $request->setKeys($keys);
        $request->setCommitVersion($this->commitTs ?? 0);

        $this->logger->debug('Commit', [
            'txnId' => $this->txnId,
            'regionId' => $region->regionId,
            'keyCount' => count($keys),
            'commitTs' => $this->commitTs,
        ]);

        /** @var CommitResponse $response */
        $response = $this->grpc->call(
            $address,
            'tikvpb.Tikv',
            'KvCommit',
            $request,
            CommitResponse::class,
        );
        $this->handleRegionError($response, $region);

        $error = $response->getError();
        if ($error !== null) {
            $this->handleCommitError($error);
        }
    }

    /**
     * @param string[] $keys
     */
    private function batchRollback(array $keys): void
    {
        $keysByRegion = $this->groupStringsByRegion($keys);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new BatchRollbackRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartVersion($this->startTs);
            $request->setKeys($regionKeys);

            $this->logger->debug('BatchRollback', [
                'txnId' => $this->txnId,
                'regionId' => $region->regionId,
                'keyCount' => count($regionKeys),
            ]);

            $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvBatchRollback',
                $request,
                BatchRollbackResponse::class,
            );
        }
    }

    private function pessimisticLockBatch(string $primary): void
    {
        if ($this->pendingLockKeys === []) {
            return;
        }

        $keys = array_values(array_unique($this->pendingLockKeys));
        $this->pendingLockKeys = [];

        $keysByRegion = $this->groupStringsByRegion($keys);
        $isFirstLock = true;

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $mutations = [];
            foreach ($regionKeys as $key) {
                $mutation = new Mutation();
                $mutation->setOp(Op::PessimisticLock);
                $mutation->setKey($key);
                $mutations[] = $mutation;
            }

            $request = new PessimisticLockRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setMutations($mutations);
            $request->setPrimaryLock($primary);
            $request->setStartVersion($this->startTs);
            $request->setLockTtl(self::PESSIMISTIC_LOCK_TTL_MS);
            $request->setForUpdateTs($this->startTs);
            $request->setIsFirstLock($isFirstLock);
            $request->setReturnValues(true);

            $this->logger->debug('PessimisticLock', [
                'txnId' => $this->txnId,
                'regionId' => $region->regionId,
                'keyCount' => count($regionKeys),
            ]);

            try {
                /** @var PessimisticLockResponse $response */
                $response = $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'KvPessimisticLock',
                    $request,
                    PessimisticLockResponse::class,
                );

                $this->handleRegionError($response, $region);

                $errors = $response->getErrors();
                if (count($errors) > 0) {
                    foreach ($errors as $keyError) {
                        $deadlock = $keyError->getDeadlock();
                        if ($deadlock !== null) {
                            throw new DeadlockException(
                                message: 'Deadlock detected during pessimistic lock',
                                deadlockKey: $deadlock->getDeadlockKey() !== '' ? $deadlock->getDeadlockKey() : null,
                                deadlockKeyHash: (int) $deadlock->getDeadlockKeyHash(),
                                lockTs: (int) $deadlock->getLockTs(),
                            );
                        }

                        $locked = $keyError->getLocked();
                        if ($locked !== null) {
                            $this->lockResolver->resolveLock(
                                $locked->getKey(),
                                (int) $locked->getLockVersion(),
                                $this->startTs,
                            );
                        }

                        $conflict = $keyError->getConflict();
                        if ($conflict !== null) {
                            throw new TransactionConflictException(
                                'Write conflict during pessimistic lock',
                            );
                        }
                    }
                }
            } catch (GrpcException $e) {
                $this->logger->warning('Pessimistic lock failed, will retry on prewrite', [
                    'regionId' => $region->regionId,
                    'keyCount' => count($regionKeys),
                    'error' => $e->getMessage(),
                ]);
            }

            $isFirstLock = false;
        }
    }

    private function pessimisticRollbackAll(): void
    {
        $pessimisticKeys = array_keys($this->writeSet);
        if ($pessimisticKeys === []) {
            return;
        }

        $keysByRegion = $this->groupStringsByRegion($pessimisticKeys);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new PessimisticRollbackRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartVersion($this->startTs);
            $request->setForUpdateTs($this->startTs);
            $request->setKeys($regionKeys);

            $this->logger->debug('PessimisticRollback', [
                'txnId' => $this->txnId,
                'regionId' => $region->regionId,
            ]);

            $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KVPessimisticRollback',
                $request,
                PessimisticRollbackResponse::class,
            );
        }
    }

    /**
     * @param string[] $keys
     * @return array<string, ?string>
     */
    private function batchGetFromTiKV(array $keys): array
    {
        $results = [];
        $keysByRegion = $this->groupStringsByRegion($keys);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new BatchGetRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKeys($regionKeys);
            $request->setVersion($this->startTs);

            /** @var BatchGetResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvBatchGet',
                $request,
                BatchGetResponse::class,
            );
            $this->handleRegionError($response, $region);

            foreach ($response->getPairs() as $pair) {
                $results[$pair->getKey()] = $pair->getValue() !== '' ? $pair->getValue() : null;
            }
        }

        foreach ($keys as $key) {
            if (!array_key_exists($key, $results)) {
                $results[$key] = null;
            }
            $this->readSet[$key] = $results[$key];
        }

        return $results;
    }

    /**
     * @return array<array{key: string, value: ?string}>
     */
    private function executeScanForRegion(
        RegionInfo $region,
        string $startKey,
        string $endKey,
        int $limit,
    ): array {
        return $this->executeWithRetry($startKey, function () use ($region, $startKey, $endKey, $limit): array {
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new ScanRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setStartKey($startKey);
            if ($endKey !== '') {
                $request->setEndKey($endKey);
            }
            $request->setLimit($limit > 0 ? $limit : self::DEFAULT_SCAN_LIMIT);
            $request->setVersion($this->startTs);

            /** @var ScanResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvScan',
                $request,
                ScanResponse::class,
            );
            $this->handleRegionError($response, $region);

            $error = $response->getError();
            if ($error !== null) {
                $locked = $error->getLocked();
                if ($locked !== null) {
                    $this->lockResolver->resolveLock(
                        $locked->getKey(),
                        (int) $locked->getLockVersion(),
                        $this->startTs,
                    );
                    throw new TiKvException('Lock encountered during scan, resolved - retry');
                }
            }

            $results = [];
            foreach ($response->getPairs() as $pair) {
                $results[] = [
                    'key' => $pair->getKey(),
                    'value' => $pair->getValue() !== '' ? $pair->getValue() : null,
                ];
            }

            return $results;
        });
    }

    private function getPrimaryKey(): string
    {
        foreach (array_keys($this->writeSet) as $key) {
            return $key;
        }
        throw new \LogicException('Write set is empty, no primary key');
    }

    /**
     * @return Mutation[]
     */
    private function buildMutations(): array
    {
        $mutations = [];
        foreach ($this->writeSet as $key => $value) {
            $mutation = new Mutation();
            $mutation->setKey($key);

            if ($value === null) {
                $mutation->setOp(Op::Del);
                $mutation->setValue('');
            } else {
                $mutation->setOp(Op::Put);
                $mutation->setValue($value);
            }

            $mutations[] = $mutation;
        }
        return $mutations;
    }

    /**
     * @param Mutation[] $mutations
     * @return array<int, array{region: RegionInfo, mutations: Mutation[]}>
     */
    private function groupMutationsByRegion(array $mutations): array
    {
        if ($mutations === []) {
            return [];
        }

        $keys = array_map(fn(Mutation $m) => $m->getKey(), $mutations);
        $resolved = $this->regionResolver->batchResolveRegions($keys);

        $keyToMutation = [];
        foreach ($mutations as $mutation) {
            $keyToMutation[$mutation->getKey()] = $mutation;
        }

        $grouped = [];
        foreach ($keys as $key) {
            $region = $resolved[$key] ?? null;
            if ($region === null) {
                continue;
            }
            $mutation = $keyToMutation[$key];
            $regionId = $region->regionId;
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'mutations' => []];
            }
            $grouped[$regionId]['mutations'][] = $mutation;
        }
        return $grouped;
    }

    /**
     * @param string[] $keys
     * @return array<int, array{region: RegionInfo, keys: string[]}>
     */
    private function groupStringsByRegion(array $keys): array
    {
        return RegionGrouper::groupKeysByRegionBatch($keys, $this->regionResolver);
    }

    private function handleRegionError(
        \Google\Protobuf\Internal\Message $response,
        RegionInfo $region,
    ): void {
        if (!method_exists($response, 'getRegionError')) {
            return;
        }

        $regionError = $response->getRegionError();
        if ($regionError instanceof \CrazyGoat\Proto\Errorpb\Error) {
            $this->regionCache->invalidate($region->regionId);
            throw RegionException::fromRegionError($regionError);
        }
    }

    private function ensureActive(): void
    {
        if ($this->closed) {
            throw new \RuntimeException('Transaction is not active');
        }
        if ($this->status !== TransactionStatus::Active) {
            throw new \RuntimeException('Transaction is not active');
        }
    }

    private function closeTransaction(): void
    {
        $this->closed = true;
    }

    private function retryExecutor(): RetryExecutor
    {
        $this->retryExecutor ??= new RetryExecutor(
            $this->maxBackoffMs,
            600000,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
        );

        return $this->retryExecutor;
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @return T
     */
    private function executeWithRetry(string $key, callable $operation): mixed
    {
        return $this->retryExecutor()->execute($key, $operation, $this->classifyError(...));
    }

    private function classifyError(TiKvException $e): ?BackoffType
    {
        $message = $e->getMessage();

        if (str_contains($message, 'KeyExists') || str_contains($message, 'WriteConflict')) {
            return null;
        }
        if (str_contains($message, 'locked') || str_contains($message, 'Lock')) {
            return BackoffType::TxnLock;
        }

        return null;
    }
}
