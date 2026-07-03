<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
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
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatRequest;
use CrazyGoat\Proto\Kvrpcpb\TxnHeartBeatResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Two-phase commit protocol operations for a single transaction.
 *
 * Extracted from the Transaction god object (issue #83) following the
 * same decomposition pattern as the RawKv module (RawKvCrud → RawKvBatch etc.).
 *
 * Handles prewrite, commit, rollback, pessimistic locking and transaction
 * heartbeat.  Operates on a shared TransactionState and delegates retry
 * decisions to a caller-provided RetryExecutor.
 */
final readonly class TwoPhaseCommitter
{
    private const OPTIMISTIC_LOCK_TTL_MS = 3000;
    private const PESSIMISTIC_LOCK_TTL_MS = 30000;
    private const PESSIMISTIC_LOCK_RETRY_DELAY_MS = 100;

    public function __construct(
        private int $startTs,
        private bool $pessimistic,
        private int $priority,
        private PdClientInterface $pdClient,
        private GrpcClientInterface $grpc,
        private RegionCacheInterface $regionCache,
        private RegionResolver $regionResolver,
        private LockResolver $lockResolver,
        private TimeoutConfig $timeoutConfig,
        private int $maxBackoffMs,
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function isPessimistic(): bool
    {
        return $this->pessimistic;
    }

    public function getPriority(): int
    {
        return $this->priority;
    }

    // ---------------------------------------------------------------
    //  Commit
    // ---------------------------------------------------------------

    /**
     * Execute the two-phase commit protocol.
     *
     * 1. Pessimistic lock (if pessimistic mode)
     * 2. Prewrite all mutations
     * 3. Acquire commit timestamp
     * 4. Commit primary key first, then secondary keys
     *
     * @throws TiKvException
     */
    public function commit(
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): void {
        $primary = $state->getPrimaryKey();

        if ($this->pessimistic) {
            $this->pessimisticLockBatch($primary, $state);
        }

        $mutations = $this->buildMutations($state);
        $keysByRegion = $this->groupMutationsByRegion($mutations);
        $allKeys = $state->getWriteKeys();

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionMutations = $regionData['mutations'];
            $this->prewriteForRegion($region, $regionMutations, $primary, $state);
        }

        $commitTs = $this->pdClient->getTimestamp();
        $state->setCommitTs($commitTs);

        $this->commitKeys($allKeys, $state, $retryExecutor, $classifier);
    }

    // ---------------------------------------------------------------
    //  Rollback
    // ---------------------------------------------------------------

    /**
     * Rollback the transaction.
     *
     * If pessimistic mode, first pessimistically rolls back all locked
     * keys, then performs a batch rollback of the write set.
     *
     * @throws TiKvException
     */
    public function rollback(
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): void {
        if ($this->pessimistic) {
            $this->pessimisticRollbackAll($state, $retryExecutor, $classifier);
        }

        $this->batchRollback($state->getWriteKeys(), $retryExecutor, $classifier);

        $state->clearWriteSet();
        $state->clearReadSet();
        $state->setCommitTs(null);
        $state->setStatus(TransactionStatus::RolledBack);
        $state->close();
    }

    // ---------------------------------------------------------------
    //  Heartbeat
    // ---------------------------------------------------------------

    /**
     * Send a heartbeat for the transaction's primary lock.
     *
     * @return int The actual lock TTL granted by TiKV
     *
     * @throws TiKvException
     */
    public function heartbeat(
        string $primary,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $adviseLockTtlMs = 10000,
    ): int {
        return $retryExecutor->execute($primary, function () use ($primary, $adviseLockTtlMs): int {
            $region = $this->regionResolver->getRegionInfo($primary);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new TxnHeartBeatRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setPrimaryLock($primary);
            $request->setStartVersion($this->startTs);
            $request->setAdviseLockTtl($adviseLockTtlMs);

            $this->logger->debug('TxnHeartBeat', [
                'primary' => KeyRedactor::redact($primary),
                'adviseLockTtlMs' => $adviseLockTtlMs,
            ]);

            /** @var TxnHeartBeatResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'KvTxnHeartBeat',
                $request,
                TxnHeartBeatResponse::class,
                $this->timeoutMs('write'),
            );

            RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

            $error = $response->getError();
            if ($error !== null) {
                throw new TiKvException('Heartbeat failed: key error');
            }

            return (int) $response->getLockTtl();
        }, $classifier);
    }

    // ---------------------------------------------------------------
    //  Prewrite
    // ---------------------------------------------------------------

    /**
     * @param Mutation[] $mutations
     */
    private function prewriteForRegion(
        RegionInfo $region,
        array $mutations,
        string $primary,
        TransactionState $state,
    ): void {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new PrewriteRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setMutations($mutations);
        $request->setPrimaryLock($primary);
        $request->setStartVersion($this->startTs);
        $request->setLockTtl(self::OPTIMISTIC_LOCK_TTL_MS);

        if ($this->pessimistic) {
            $forUpdateTs = $state->getMaxForUpdateTs() ?? $this->startTs;
            $request->setForUpdateTs($forUpdateTs);
            $request->setLockTtl(self::PESSIMISTIC_LOCK_TTL_MS);
            $actions = [];
            foreach ($mutations as $mutation) {
                $actions[] = PessimisticAction::DO_PESSIMISTIC_CHECK;
            }
            $request->setPessimisticActions($actions);
        }

        $this->logger->debug('Prewrite', [
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
            $this->timeoutMs('write'),
        );
        RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

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
                $rawPrimary = $locked->getPrimaryLock();
                $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
                $this->lockResolver->resolveLock($lockPrimary, $locked);
                throw new TxnRetryableException(
                    'Lock conflict during prewrite, resolved - retry',
                    BackoffType::TxnLock,
                );
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

    // ---------------------------------------------------------------
    //  Commit phase (2PC)
    // ---------------------------------------------------------------

    /**
     * Commit all keys using the two-phase commit protocol.
     *
     * TiKV requires the COMMIT of the primary key's region to precede the
     * COMMIT of secondary regions — otherwise the secondary commit may be
     * rejected with a "primary not committed" error.
     *
     * @param string[] $keys
     */
    private function commitKeys(
        array $keys,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): void {
        if ($keys === []) {
            return;
        }

        $keysByRegion = $this->groupStringsByRegion($keys);

        if ($keysByRegion === []) {
            return;
        }

        $primary = $state->getPrimaryKey();

        // Resolve the primary's region so we can commit it first.
        $primaryRegionId = null;
        foreach ($keysByRegion as $regionId => $regionData) {
            if (in_array($primary, $regionData['keys'], true)) {
                $primaryRegionId = $regionId;
                break;
            }
        }

        if ($primaryRegionId === null) {
            $primaryRegionId = array_key_first($keysByRegion);
        }

        // Commit the primary first. Failures here are fatal: do not retry.
        $primaryRegionData = $keysByRegion[$primaryRegionId];
        $this->commitForRegion($primaryRegionData['region'], $primaryRegionData['keys'], $state);

        // Remove the primary region from outstanding work; commit remaining
        // secondary regions under the retry executor.
        unset($keysByRegion[$primaryRegionId]);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $firstKey = $regionKeys[0] ?? '';

            $retryExecutor->execute($firstKey, function () use ($region, $regionKeys, $state): null {
                $this->commitForRegion($region, $regionKeys, $state);
                return null;
            }, $classifier);
        }
    }

    /**
     * @param string[] $keys
     *
     * @throws InvalidStateException if commitTs is null
     */
    private function commitForRegion(RegionInfo $region, array $keys, TransactionState $state): void
    {
        $commitTs = $state->getCommitTs();
        if ($commitTs === null) {
            throw new InvalidStateException(
                'commitTs must be set before committing; commit() must run first.',
            );
        }

        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new CommitRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setStartVersion($this->startTs);
        $request->setKeys($keys);
        $request->setCommitVersion($commitTs);

        $this->logger->debug('Commit', [
            'regionId' => $region->regionId,
            'keyCount' => count($keys),
            'commitTs' => $commitTs,
        ]);

        /** @var CommitResponse $response */
        $response = $this->grpc->call(
            $address,
            'tikvpb.Tikv',
            'KvCommit',
            $request,
            CommitResponse::class,
            $this->timeoutMs('write'),
        );
        RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

        $error = $response->getError();
        if ($error !== null) {
            $this->handleCommitError($error);
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

    // ---------------------------------------------------------------
    //  Rollback helpers
    // ---------------------------------------------------------------

    /**
     * @param string[] $keys
     */
    private function batchRollback(
        array $keys,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): void {
        $keysByRegion = $this->groupStringsByRegion($keys);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $firstKey = $regionKeys[0] ?? '';

            $retryExecutor->execute($firstKey, function () use ($region, $regionKeys): null {
                $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

                $request = new BatchRollbackRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($region));
                $request->setStartVersion($this->startTs);
                $request->setKeys($regionKeys);

                $this->logger->debug('BatchRollback', [
                    'regionId' => $region->regionId,
                    'keyCount' => count($regionKeys),
                ]);

                /** @var BatchRollbackResponse $response */
                $response = $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'KvBatchRollback',
                    $request,
                    BatchRollbackResponse::class,
                    $this->timeoutMs('write'),
                );
                RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

                $error = $response->getError();
                if ($error !== null) {
                    $this->handleRollbackError($error);
                }

                return null;
            }, $classifier);
        }
    }

    private function handleRollbackError(KeyError $error): void
    {
        $locked = $error->getLocked();
        if ($locked !== null) {
            $rawPrimary = $locked->getPrimaryLock();
            $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
            $this->lockResolver->resolveLock($lockPrimary, $locked);
            throw new TxnRetryableException(
                'Lock encountered during rollback, resolved - retry',
                BackoffType::TxnLock,
            );
        }

        $retryable = $error->getRetryable();
        if ($retryable !== '') {
            throw new TxnRetryableException(
                'Retryable error during rollback: ' . $retryable,
                BackoffType::TxnLock,
            );
        }

        $abort = $error->getAbort();
        if ($abort !== '') {
            throw new TransactionConflictException(
                'Abort during rollback: ' . $abort,
            );
        }
    }

    // ---------------------------------------------------------------
    //  Pessimistic lock
    // ---------------------------------------------------------------

    private function pessimisticLockBatch(
        string $primary,
        TransactionState $state,
    ): void {
        $keys = array_values(array_unique($state->getPendingLockKeys()));
        $state->clearPendingLockKeys();

        if ($keys === []) {
            return;
        }

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

            // Get a fresh PD timestamp for for_update_ts.
            $forUpdateTs = $this->pdClient->getTimestamp();
            $state->updateMaxForUpdateTs($forUpdateTs);

            $elapsedMs = 0;
            $attempt = 0;
            do {
                $attempt++;
                $request = new PessimisticLockRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($region));
                $request->setMutations($mutations);
                $request->setPrimaryLock($primary);
                $request->setStartVersion($this->startTs);
                $request->setLockTtl(self::PESSIMISTIC_LOCK_TTL_MS);
                $request->setForUpdateTs($forUpdateTs);
                $request->setIsFirstLock($isFirstLock);
                $request->setReturnValues(true);

                $this->logger->debug('PessimisticLock', [
                    'regionId' => $region->regionId,
                    'keyCount' => count($regionKeys),
                    'attempt' => $attempt,
                    'forUpdateTs' => $forUpdateTs,
                ]);

                /** @var PessimisticLockResponse $response */
                $response = $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'KvPessimisticLock',
                    $request,
                    PessimisticLockResponse::class,
                    $this->timeoutMs('write'),
                );

                RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

                $errors = $response->getErrors();
                $needRetry = false;

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
                            $rawPrimary = $locked->getPrimaryLock();
                            $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
                            $this->lockResolver->resolveLock($lockPrimary, $locked);
                            $needRetry = true;
                            break;
                        }

                        $conflict = $keyError->getConflict();
                        if ($conflict !== null) {
                            throw new TransactionConflictException(
                                'Write conflict during pessimistic lock',
                            );
                        }
                    }
                }

                if (!$needRetry) {
                    break;
                }

                $delayMs = min(
                    self::PESSIMISTIC_LOCK_RETRY_DELAY_MS * (1 << min($attempt, 6)),
                    10000,
                );

                $remainingMs = $this->maxBackoffMs - $elapsedMs;
                if ($remainingMs <= 0) {
                    $this->logger->warning('Pessimistic lock retry budget exhausted', [
                        'elapsedMs' => $elapsedMs,
                    ]);
                    break;
                }
                $delayMs = min($delayMs, $remainingMs);

                $this->logger->debug('Pessimistic lock conflict, retrying', [
                    'attempt' => $attempt,
                    'delayMs' => $delayMs,
                    'elapsedMs' => $elapsedMs,
                ]);
                usleep($delayMs * 1000);
                $elapsedMs += $delayMs;

                $forUpdateTs = $this->pdClient->getTimestamp();
                $state->updateMaxForUpdateTs($forUpdateTs);
            } while ($elapsedMs < $this->maxBackoffMs);

            $isFirstLock = false;
        }
    }

    private function pessimisticRollbackAll(
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): void {
        $pessimisticKeys = $state->getWriteKeys();
        if ($pessimisticKeys === []) {
            return;
        }

        $keysByRegion = $this->groupStringsByRegion($pessimisticKeys);

        foreach ($keysByRegion as $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $firstKey = $regionKeys[0] ?? '';

            $retryExecutor->execute($firstKey, function () use ($region, $regionKeys): null {
                $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

                $request = new PessimisticRollbackRequest();
                $request->setContext(RegionContextFactory::fromRegionInfo($region));
                $request->setStartVersion($this->startTs);
                $request->setForUpdateTs($this->startTs);
                $request->setKeys($regionKeys);

                $this->logger->debug('PessimisticRollback', [
                    'regionId' => $region->regionId,
                ]);

                /** @var PessimisticRollbackResponse $response */
                $response = $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'KVPessimisticRollback',
                    $request,
                    PessimisticRollbackResponse::class,
                    $this->timeoutMs('write'),
                );
                RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

                $errors = $response->getErrors();
                foreach ($errors as $keyError) {
                    $this->handleRollbackError($keyError);
                }

                return null;
            }, $classifier);
        }
    }

    // ---------------------------------------------------------------
    //  Mutation building & region grouping
    // ---------------------------------------------------------------

    /**
     * @return Mutation[]
     */
    private function buildMutations(TransactionState $state): array
    {
        $mutations = [];
        foreach ($state->getWriteSet() as $key => $value) {
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
        $grouped = RegionGrouper::groupItemsByRegion(
            $mutations,
            fn(Mutation $m) => $m->getKey(),
            $this->regionResolver,
        );

        $result = [];
        foreach ($grouped as $regionId => $data) {
            $result[$regionId] = [
                'region' => $data['region'],
                'mutations' => $data['items'],
            ];
        }

        return $result;
    }

    /**
     * @param string[] $keys
     * @return array<int, array{region: RegionInfo, keys: string[]}>
     */
    private function groupStringsByRegion(array $keys): array
    {
        return RegionGrouper::groupKeysByRegionBatch($keys, $this->regionResolver);
    }

    // ---------------------------------------------------------------
    //  Timeout helper
    // ---------------------------------------------------------------

    private function timeoutMs(string $operationType): ?int
    {
        return match ($operationType) {
            'write' => $this->timeoutConfig->writeTimeoutMs,
            'batch_write' => $this->timeoutConfig->batchWriteTimeoutMs,
            default => null,
        };
    }
}
