<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\Deadlock;
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
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\KeyErrorDescriber;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\UndeterminedCommitException;
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
    private const PESSIMISTIC_LOCK_MAX_REGROUPS = 10;
    private const ROLLBACK_MAX_REGROUPS = 10;

    /**
     * Maximum number of keys for a transaction to stay eligible for one-phase
     * commit (mirrors client-go's onePCKeysLimit). 1PC is additionally
     * restricted to a single region.
     */
    private const ONE_PC_MAX_KEYS = 128;

    /**
     * Maximum number of keys for a transaction to stay eligible for async
     * commit (mirrors client-go's asyncCommitCfg.KeysLimit default).
     */
    private const ASYNC_COMMIT_MAX_KEYS = 256;

    /**
     * TSO-tick gap used to derive max_commit_ts for async commit (mirrors
     * client-rust's max_commit_ts_gap default of 15000). TiKV declines the
     * async commit when the derived commit_ts would exceed this bound, so a
     * too-large gap only weakens the guarantee that readers can always
     * resolve the lock without waiting for the TTL.
     */
    private const ASYNC_COMMIT_MAX_COMMIT_TS_GAP = 15000;

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
        private bool $enable1Pc = false,
        private bool $enableAsyncCommit = false,
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

        // One-phase commit (issue #419): a single-region, small transaction
        // can commit in one round trip — TiKV derives the commit timestamp
        // itself and reports it in PrewriteResponse::one_pc_commit_ts.
        $onePc = $this->enable1Pc
            && count($keysByRegion) === 1
            && count($allKeys) <= self::ONE_PC_MAX_KEYS;

        // Async commit (issue #419): small transactions commit without the
        // commit phase — the primary lock carries the decision, readers
        // resolve it via CheckSecondaryLocks/CheckTxnStatus.
        $async = !$onePc
            && $this->enableAsyncCommit
            && count($allKeys) <= self::ASYNC_COMMIT_MAX_KEYS;

        $primaryMinCommitTs = 0;
        $maxMinCommitTs = 0;
        $onePcCommitTs = 0;
        $asyncAccepted = true;
        $secondaries = array_values(array_diff($allKeys, [$primary]));

        // Async commit (client-go parity): the primary region must be
        // prewritten LAST — the primary's prewrite is the commit-decision
        // barrier, and prewriting it first would let a concurrent reader
        // observe a decided-looking primary while a secondary region's
        // prewrite is still in flight.
        if ($async && count($keysByRegion) > 1) {
            $primaryRegionIndex = null;
            foreach ($keysByRegion as $index => $regionData) {
                foreach ($regionData['mutations'] as $mutation) {
                    if ($mutation->getKey() === $primary) {
                        $primaryRegionIndex = $index;
                        break 2;
                    }
                }
            }
            if ($primaryRegionIndex !== null) {
                $primaryRegionData = $keysByRegion[$primaryRegionIndex];
                unset($keysByRegion[$primaryRegionIndex]);
                $keysByRegion[] = $primaryRegionData;
            }
        }

        foreach ($keysByRegion as $regionData) {
            $regionMutations = $regionData['mutations'];
            // The group is never empty (groupMutationsByRegion() only emits
            // groups that own at least one mutation); the null-coalesce keeps
            // PHPStan happy about the list offset.
            $firstMutation = $regionMutations[0] ?? null;
            $firstKey = $firstMutation instanceof Mutation ? $firstMutation->getKey() : '';
            $isPrimaryRegion = false;
            foreach ($regionMutations as $mutation) {
                if ($mutation->getKey() === $primary) {
                    $isPrimaryRegion = true;
                    break;
                }
            }

            $useOnePc = $isPrimaryRegion && $onePc;
            $useAsyncCommit = $isPrimaryRegion && $async;
            $regionSecondaries = $useAsyncCommit ? $secondaries : [];

            // Prewrite is the last transaction RPC that used to run outside a
            // RetryExecutor (issue #213, TXN-08). Wrap each region's prewrite
            // so region errors (NotLeader, EpochNotMatch, RegionNotFound,
            // ServerIsBusy) and resolved lock conflicts retry instead of
            // aborting the whole transaction. The region is re-resolved inside
            // the closure so the leader switch / cache invalidation performed
            // by the executor takes effect on the next attempt — the same
            // stale-capture class as #267/#500/#502. Prewrite is idempotent
            // for a given start_ts, so replay is safe.
            $result = $retryExecutor->execute(
                $firstKey,
                function () use (
                    $firstKey,
                    $regionMutations,
                    $primary,
                    $state,
                    $useOnePc,
                    $useAsyncCommit,
                    $regionSecondaries,
                ): array {
                    $region = $this->regionResolver->getRegionInfo($firstKey);

                    return $this->prewriteForRegion(
                        $region,
                        $regionMutations,
                        $primary,
                        $state,
                        $useOnePc,
                        $useAsyncCommit,
                        $regionSecondaries,
                    );
                },
                $classifier,
            );

            $maxMinCommitTs = max($maxMinCommitTs, $result['minCommitTs']);
            if ($isPrimaryRegion) {
                $primaryMinCommitTs = $result['minCommitTs'];
                $onePcCommitTs = $result['onePcCommitTs'];
                // TiKV declines async commit by answering the primary
                // prewrite with min_commit_ts = 0.
                $asyncAccepted = $result['minCommitTs'] > 0;
            }
        }

        if ($onePc && $onePcCommitTs > 0) {
            $state->setCommitTs($onePcCommitTs);
            $state->setStatus(TransactionStatus::Committed);
            $this->logger->debug('One-phase commit done', ['commitTs' => $onePcCommitTs]);
            return;
        }
        if ($onePc) {
            $this->logger->info('TiKV declined one-phase commit, falling back to two-phase');
        }

        if ($async && $asyncAccepted) {
            // Derive the commit timestamp from the returned min_commit_ts
            // values: TiKV writes async-commit data at the chosen
            // min_commit_ts, so the commit ts is the maximum over the
            // primary's and every secondary's min_commit_ts — with NO
            // extra +1. A reader whose start ts equals the data's commit
            // ts must see the write: PD can hand out exactly the
            // min_commit_ts value to the next reader, so deriving
            // max(min_commit_ts) + 1 makes the write invisible to that
            // reader (the minCommitTSPrevail "+1" in client-go applies to
            // other transactions' locks, not to this transaction's own
            // prewrite responses).
            $commitTs = max($primaryMinCommitTs, $maxMinCommitTs);
            $state->setCommitTs($commitTs);
            $state->setStatus(TransactionStatus::Committed);
            $this->logger->debug('Async commit done', ['commitTs' => $commitTs]);
            return;
        }
        if ($async) {
            $this->logger->info('TiKV declined async commit, falling back to two-phase');
        }

        // Reuse an existing commit timestamp on retry: a second commit() after a failed
        // commit phase must never allocate a new ts (issue #217). With TXN-10 the
        // status is Committed at the commit point, so a second commit() is rejected
        // by ensureActive() before reaching this line.
        $commitTs = $state->getCommitTs() ?? $this->pdClient->getTimestamp();
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
        // A transaction that reached the commit phase is committed in TiKV
        // and must never be rolled back (issue #215, TXN-10). The guard is
        // on the status, not on commitTs: commitTs is already set when the
        // primary commit is attempted, and a FAILED primary commit leaves a
        // transaction that is legitimately still rollback-able.
        if ($state->getStatus() === TransactionStatus::Committed) {
            throw new InvalidStateException(
                'Cannot roll back a transaction that reached the commit phase',
            );
        }

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
                $this->handleHeartbeatError($error, $primary);
            }

            return (int) $response->getLockTtl();
        }, $classifier);
    }

    /**
     * Translates the KvTxnHeartBeat KeyError payload into typed exceptions
     * instead of discarding it (issue #492).
     *
     * A heartbeat can only reach the lock path when the server no longer
     * recognises the transaction as alive, so `locked` is deliberately NOT
     * resolved here: the only lock the server can report is (a descendant
     * of) this transaction's own primary lock, and resolving it would roll
     * the calling transaction back underneath itself. Rollback/commit
     * handlers do resolve `locked` — their calls run outside the primary's
     * liveness window.
     */
    private function handleHeartbeatError(KeyError $error, string $primary): never
    {
        $retryable = $error->getRetryable();
        if ($retryable !== '') {
            throw new TransactionConflictException('Heartbeat failed: retryable: ' . $retryable);
        }

        $abort = $error->getAbort();
        if ($abort !== '') {
            throw new TransactionConflictException('Heartbeat failed: abort: ' . $abort);
        }

        $locked = $error->getLocked();
        if ($locked !== null) {
            throw new TxnRetryableException(
                sprintf('Heartbeat failed: locked key %s', KeyRedactor::redact($primary)),
                BackoffType::TxnLock,
            );
        }

        throw new TiKvException(
            'Heartbeat failed: ' . KeyErrorDescriber::describe($error),
        );
    }

    // ---------------------------------------------------------------
    //  Prewrite
    // ---------------------------------------------------------------

    /**
     * @param Mutation[] $mutations
     * @param string[] $secondaries Secondary keys carried on the primary
     *                              region's prewrite when async commit is
     *                              used (empty otherwise)
     * @return array{minCommitTs: int, onePcCommitTs: int}
     */
    private function prewriteForRegion(
        RegionInfo $region,
        array $mutations,
        string $primary,
        TransactionState $state,
        bool $useOnePc = false,
        bool $useAsyncCommit = false,
        array $secondaries = [],
    ): array {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new PrewriteRequest();
        $request->setContext(RegionContextFactory::fromRegionInfo($region));
        $request->setMutations($mutations);
        $request->setPrimaryLock($primary);
        $request->setStartVersion($this->startTs);
        $request->setLockTtl(self::OPTIMISTIC_LOCK_TTL_MS);

        if ($useOnePc) {
            $request->setTryOnePc(true);
            $request->setMinCommitTs($this->startTs + 1);
        } elseif ($useAsyncCommit) {
            $request->setUseAsyncCommit(true);
            $request->setSecondaries($secondaries);
            // start_ts + 1, as in client-go/client-rust: the commit ts must
            // be strictly greater than the transaction's start ts.
            $request->setMinCommitTs($this->startTs + 1);
            $request->setMaxCommitTs($this->startTs + self::ASYNC_COMMIT_MAX_COMMIT_TS_GAP);
        }

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
        // commit()'s prewrite loop now runs inside a RetryExecutor
        // (issue #213, TXN-08), so its handleNotLeader() is the sole owner of
        // NotLeader drops: it switches to the hinted leader when the peer is
        // still cached and only invalidates otherwise. Self-invalidating here
        // would double-count the metric and defeat valid-hint leader
        // switching (issue #474), so NotLeader oneofs are left for the
        // executor.
        RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

        $errors = $response->getErrors();
        if (count($errors) > 0) {
            $this->handlePrewriteErrors($errors);
        }

        return [
            'minCommitTs' => (int) $response->getMinCommitTs(),
            'onePcCommitTs' => (int) $response->getOnePcCommitTs(),
        ];
    }

    /**
     * @param iterable<KeyError> $errors
     */
    private function handlePrewriteErrors(iterable $errors): void
    {
        foreach ($errors as $keyError) {
            $deadlock = $keyError->getDeadlock();
            if ($deadlock !== null) {
                $this->throwDeadlock($deadlock, 'Deadlock detected during prewrite');
            }

            $locked = $keyError->getLocked();
            if ($locked !== null) {
                $rawPrimary = $locked->getPrimaryLock();
                $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
                // Reached from commit()'s prewrite loop, which now runs
                // inside a RetryExecutor (issue #213, TXN-08): the executor's
                // handleNotLeader() owns NotLeader drops, so a NotLeader
                // surfaced by the resolve propagates and is retried.
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

            // Named but previously unhandled variants (issue #214, TXN-09).
            // Each maps to a definite, typed client outcome; the transaction
            // must not proceed to commit keys whose prewrite failed.
            if ($keyError->getAlreadyExist() !== null) {
                throw new TransactionConflictException('Prewrite failed: key already exists');
            }

            if ($keyError->getAssertionFailed() !== null) {
                throw new TransactionConflictException('Prewrite failed: assertion failed');
            }

            if ($keyError->getPrimaryMismatch() !== null) {
                throw new TransactionConflictException('Prewrite failed: primary lock mismatch');
            }

            if ($keyError->getTxnNotFound() !== null) {
                throw new TransactionConflictException('Prewrite failed: transaction not found');
            }

            if ($keyError->getCommitTsTooLarge() !== null) {
                // Fail closed (issue #214): throwing is safer than the old
                // silent success. A client-go-style max_commit_ts fallback
                // that retries the 2PC with a bounded commit ts is out of
                // scope here.
                throw new TransactionConflictException('Prewrite failed: commit timestamp too large');
            }

            // Fail closed: an unrecognised variant must never be treated as a
            // successful prewrite and fall through to the commit phase.
            throw new TiKvException(
                'Prewrite failed: ' . KeyErrorDescriber::describe($keyError),
            );
        }
    }

    /**
     * Throw the typed deadlock exception for a `KeyError.deadlock` payload.
     *
     * Shared by the prewrite and pessimistic-lock handlers so the deadlock
     * key/hash/lockTs extraction lives in exactly one place. `$context` is the
     * full exception message and preserves each call site's wording.
     */
    private function throwDeadlock(Deadlock $deadlock, string $context): never
    {
        throw new DeadlockException(
            message: $context,
            deadlockKey: $deadlock->getDeadlockKey() !== ''
                ? $deadlock->getDeadlockKey() : null,
            deadlockKeyHash: (int) $deadlock->getDeadlockKeyHash(),
            lockTs: (int) $deadlock->getLockTs(),
        );
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

        $primaryRegionId ??= array_key_first($keysByRegion);

        // Commit the primary first. Failures here are fatal: do not retry.
        // No RetryExecutor wraps this call, so commitForRegion must handle
        // NotLeader drops itself (issue #474 review round 3).
        $primaryRegionData = $keysByRegion[$primaryRegionId];
        try {
            $this->commitForRegion(
                $primaryRegionData['region'],
                $primaryRegionData['keys'],
                $state,
                notLeaderOwnedByRetryExecutor: false,
            );
        } catch (GrpcException $e) {
            // A transport-level failure on the primary commit leaves the
            // outcome unknown: the write may already be applied with only
            // the response lost. The transaction must never be rolled back
            // in this state (client-go's ErrResultUndetermined, issue #216),
            // so mark it Undetermined and close it — the destructor only
            // rolls back Active, open transactions.
            $state->setStatus(TransactionStatus::Undetermined);
            $state->close();
            $this->logger->error('Primary commit outcome undetermined (transport failure)', [
                'regionId' => $primaryRegionData['region']->regionId,
                'error' => $e->getMessage(),
            ]);
            throw new UndeterminedCommitException(
                'Primary commit failed at the transport level; the transaction'
                . ' outcome is undetermined and was not rolled back',
                $e,
            );
        }

        // Commit point: once the primary key is committed the transaction is
        // durably committed in TiKV. Mark it now (issue #215, TXN-10) so a
        // secondary commit failure can never leave the status Active and
        // trigger a rollback of a committed transaction.
        $state->setStatus(TransactionStatus::Committed);

        // Remove the primary region from outstanding work; commit remaining
        // secondary regions under the retry executor.
        unset($keysByRegion[$primaryRegionId]);

        foreach ($keysByRegion as $regionId => $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $firstKey = $regionKeys[0] ?? '';

            try {
                $retryExecutor->execute($firstKey, function () use ($region, $regionKeys, $state): null {
                    $this->commitForRegion($region, $regionKeys, $state);
                    return null;
                }, $classifier);
            } catch (\Exception $e) {
                // Secondary commit failures do NOT fail the transaction: the
                // primary is already committed, so the data is durable. The
                // leftover locks are resolved by other readers through the
                // lock resolver (same policy as client-go, issue #215).
                // Only \Exception is swallowed (retryable/fatal TiKV errors);
                // \Error subclasses (TypeError, AssertionError, ...) still
                // propagate because they indicate genuine defects.
                $this->logger->warning('Secondary commit failed; locks will be resolved by readers', [
                    'regionId' => $regionId,
                    'exception' => $e,
                ]);
            }
        }
    }

    /**
     * @param string[] $keys
     * @param bool $notLeaderOwnedByRetryExecutor Whether a RetryExecutor owns
     *                                            NotLeader handling for this call
     *                                            (see RegionErrorHandler::check()).
     *                                            The PRIMARY-region commit site
     *                                            passes false — failures there are
     *                                            fatal and never retried, so no
     *                                            handleNotLeader() would ever drop
     *                                            the stale entry.
     *
     * @throws InvalidStateException if commitTs is null
     * @throws \CrazyGoat\TiKV\Client\Exception\GrpcException on a transport
     *                                           failure — for the primary-key
     *                                           commit this leaves the outcome
     *                                           undetermined (issue #216)
     */
    private function commitForRegion(
        RegionInfo $region,
        array $keys,
        TransactionState $state,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): void {
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
        RegionErrorHandler::check(
            $response,
            $this->regionCache,
            $region->regionId,
            notLeaderOwnedByRetryExecutor: $notLeaderOwnedByRetryExecutor,
        );

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
        // Array + count() instead of an int counter: PHPStan level 9 proves
        // an int counter incremented only inside the regroup branch is
        // always 0 at the check site (single-pass narrowing) — same trap as
        // PESSIMISTIC_LOCK_MAX_REGROUPS (issue #503 review).
        /** @var list<true> $regroups */
        $regroups = [];
        $pendingKeys = $keys;

        while ($pendingKeys !== []) {
            $keysByRegion = $this->groupStringsByRegion($pendingKeys);
            $pendingKeys = [];

            foreach ($keysByRegion as $groupIndex => $regionData) {
                $regionKeys = $regionData['keys'];
                $firstKey = $regionKeys[0] ?? '';

                try {
                    $retryExecutor->execute($firstKey, function () use ($firstKey, $regionKeys): null {
                        // Resolve the region on every attempt so cache
                        // invalidation and leader switching performed by the
                        // retry executor take effect (issue #502): a stale
                        // captured region would otherwise reproduce the
                        // original error on each retry — the same
                        // stale-capture class as the scan retry fix (#267,
                        // GRPC-08) and the pessimistic lock fix (#500).
                        // groupStringsByRegion() populated the cache via
                        // batchResolveRegions(), so the first attempt is a
                        // cache hit. If the re-resolved region no longer
                        // covers the whole key group (a split happened since
                        // grouping, issue #505), signal the caller to
                        // re-group instead of retrying a request the server
                        // will keep rejecting with region errors until the
                        // budget runs out — the same recovery idea as the
                        // scan re-clipping in #267.
                        $region = $this->regionResolver->getRegionInfo($firstKey);
                        if (!$this->regionCoversAllKeys($region, $regionKeys)) {
                            throw new RollbackRegroupSignal(
                                'Re-resolved region no longer covers the rollback key group',
                            );
                        }
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
                } catch (RollbackRegroupSignal) {
                    // The re-resolved region does not cover the whole key
                    // group (a split happened since grouping, issue #505):
                    // re-group this group's keys together with all
                    // not-yet-processed groups' keys against the fresh
                    // region layout and continue. Rolling back keys of
                    // already-processed groups again would be idempotent,
                    // but they are not re-sent (mirrors the #503 pattern).
                    if (count($regroups) >= self::ROLLBACK_MAX_REGROUPS) {
                        throw new RegionException(
                            'rollback',
                            'Region split repeatedly invalidated the rollback group',
                        );
                    }
                    $regroups[] = true;
                    $regroupKeys = $regionKeys;
                    foreach ($keysByRegion as $laterIndex => $laterData) {
                        if ($laterIndex > $groupIndex) {
                            $regroupKeys = array_merge($regroupKeys, $laterData['keys']);
                        }
                    }
                    $pendingKeys = array_values(array_unique($regroupKeys));
                    $this->logger->debug('Rollback re-grouping keys after region split', [
                        'regionId' => $regionData['region']->regionId,
                        'keyCount' => count($pendingKeys),
                    ]);
                    break;
                }
            }
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

        $isFirstLock = true;
        $pendingKeys = $keys;
        // Each re-group restores a full retry budget for the re-grouped
        // keys; without a cap a pathological repeated split/region-error
        // loop could retry forever (issue #503 review). Array + count()
        // instead of an int counter: PHPStan level 9 proves an int
        // counter is always 0 at the check site (single-pass narrowing)
        // and flags the comparison as always-false.
        /** @var list<true> $regroups */
        $regroups = [];

        while ($pendingKeys !== []) {
            $keysByRegion = $this->groupStringsByRegion($pendingKeys);
            $pendingKeys = [];

            // One for_update_ts per locking pass (issue #420, GAP-06):
            // hoisting the TSO call out of the per-region loop saves one
            // PD round trip per additional region. Using the same
            // for_update_ts across regions is safe — TiKV only compares
            // it against lock timestamps, and updateMaxForUpdateTs()
            // tracks the maximum. Retries still refresh it below.
            $forUpdateTs = $this->pdClient->getTimestamp();
            $state->updateMaxForUpdateTs($forUpdateTs);

            foreach ($keysByRegion as $groupIndex => $regionData) {
                $region = $regionData['region'];
                $regionKeys = $regionData['keys'];

                $mutations = [];
                foreach ($regionKeys as $key) {
                    $mutation = new Mutation();
                    $mutation->setOp(Op::PessimisticLock);
                    $mutation->setKey($key);
                    $mutations[] = $mutation;
                }

                $elapsedMs = 0;
                $attempt = 0;
                $needRetry = false;
                $lastRegionError = null;
                $regroup = false;
                do {
                    $attempt++;
                    // First attempt uses the region already supplied by
                    // groupStringsByRegion(). On retries (issue #500) the
                    // region must be re-resolved: a region captured before
                    // the loop can be stale by the time it is retried
                    // (EpochNotMatch / NotLeader) — the same stale-capture
                    // class as the scan retry fix (#267, GRPC-08). On a
                    // region error RegionErrorHandler::check() has already
                    // invalidated the cache entry, so this re-resolve
                    // reaches PD and picks up the new epoch / leader
                    // instead of replaying the stale one until the budget
                    // runs out. If the re-resolved region no longer
                    // covers the whole key group (a split happened since
                    // grouping, issue #503), the group is re-grouped
                    // against the fresh region layout instead of
                    // retrying a request the server will keep rejecting
                    // with region errors until the budget runs out — the
                    // same recovery idea as the scan re-clipping in #267.
                    if ($attempt > 1) {
                        $region = $this->regionResolver->getRegionInfo($regionKeys[0]);
                        if (!$this->regionCoversAllKeys($region, $regionKeys)) {
                            if (count($regroups) >= self::PESSIMISTIC_LOCK_MAX_REGROUPS) {
                                throw $lastRegionError ?? new RegionException(
                                    'pessimistic lock',
                                    'Region split repeatedly invalidated the lock group',
                                );
                            }
                            $regroups[] = true;
                            $regroup = true;
                            break;
                        }
                    }
                    $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

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

                    $regionError = null;
                    try {
                        /** @var PessimisticLockResponse $response */
                        $response = $this->grpc->call(
                            $address,
                            'tikvpb.Tikv',
                            'KvPessimisticLock',
                            $request,
                            PessimisticLockResponse::class,
                            $this->timeoutMs('write'),
                        );

                        // No RetryExecutor wraps this loop, so no handleNotLeader()
                        // would drop a NotLeader-carrying region — check() must
                        // self-invalidate (issue #474 review). The thrown
                        // RegionException is caught below and retried (issue #500).
                        RegionErrorHandler::check(
                            $response,
                            $this->regionCache,
                            $region->regionId,
                            notLeaderOwnedByRetryExecutor: false,
                        );
                    } catch (RegionException $caught) {
                        $regionError = $caught;
                    }

                    $needRetry = false;
                    $lastRegionError = null;

                    if ($regionError instanceof RegionException) {
                        $this->logger->warning('Region error during pessimistic lock, retrying', [
                            'regionId' => $region->regionId,
                            'attempt' => $attempt,
                            'error' => $regionError->getMessage(),
                        ]);
                        $lastRegionError = $regionError;
                    } else {
                        $errors = $response->getErrors();

                        if (count($errors) > 0) {
                            foreach ($errors as $keyError) {
                                $deadlock = $keyError->getDeadlock();
                                if ($deadlock !== null) {
                                    $this->throwDeadlock($deadlock, 'Deadlock detected during pessimistic lock');
                                }

                                $locked = $keyError->getLocked();
                                if ($locked !== null) {
                                    $rawPrimary = $locked->getPrimaryLock();
                                    $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
                                    // Charge the whole resolve (status RPCs + TTL wait)
                                    // to this loop's budget (issue #470): pass the
                                    // remaining time so the lock wait is capped by it,
                                    // then add the wall time actually spent back into
                                    // $elapsedMs — the wait used to be invisible to
                                    // the budget and could stretch it past maxBackoffMs.
                                    $resolveStartMs = (int) (microtime(true) * 1000);
                                    // min 1 ms: 0 would select LockResolver's legacy
                                    // uncapped-by-deadline branch exactly when the
                                    // budget is most exhausted (issue #470).
                                    // No RetryExecutor wraps this loop, so the
                                    // resolve must drop NotLeader regions itself
                                    // (issue #474 review round 3).
                                    $this->lockResolver->resolveLock(
                                        $lockPrimary,
                                        $locked,
                                        max(1, $this->maxBackoffMs - $elapsedMs),
                                        notLeaderOwnedByRetryExecutor: false,
                                    );
                                    $elapsedMs += max(0, (int) (microtime(true) * 1000) - $resolveStartMs);
                                    $needRetry = true;
                                    break;
                                }

                                $conflict = $keyError->getConflict();
                                if ($conflict !== null) {
                                    throw new TransactionConflictException(
                                        'Write conflict during pessimistic lock',
                                    );
                                }

                                $retryable = $keyError->getRetryable();
                                if ($retryable !== '') {
                                    throw new TransactionConflictException(
                                        'Pessimistic lock failed: retryable: ' . $retryable,
                                    );
                                }

                                $abort = $keyError->getAbort();
                                if ($abort !== '') {
                                    throw new TransactionConflictException(
                                        'Pessimistic lock failed: abort: ' . $abort,
                                    );
                                }

                                // Fail closed (issue #454): a KeyError variant
                                // other than deadlock/locked/conflict must never
                                // leave this loop as if every lock was acquired,
                                // or the transaction would proceed to prewrite
                                // keys it does not hold a lock on. Mirrors the
                                // #214 handling of unrecognised prewrite variants.
                                throw new TiKvException(
                                    'Pessimistic lock failed: ' . KeyErrorDescriber::describe($keyError),
                                );
                            }
                        }
                    }

                    if (!$needRetry && !$lastRegionError instanceof \CrazyGoat\TiKV\Client\Exception\RegionException) {
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

                if ($regroup) {
                    // The re-resolved region does not cover the whole key
                    // group (a split happened since grouping, issue #503):
                    // re-group this group's keys together with all
                    // not-yet-processed groups' keys against the fresh
                    // region layout and continue. Keys of groups that
                    // already locked successfully are NOT re-locked — the
                    // pessimistic lock for this startTs is already held on
                    // them, and this group's keys were never confirmed
                    // locked (the attempt ended in a region error), so
                    // re-sending them is safe.
                    $regroupKeys = $regionKeys;
                    foreach ($keysByRegion as $laterIndex => $laterData) {
                        if ($laterIndex > $groupIndex) {
                            $regroupKeys = array_merge($regroupKeys, $laterData['keys']);
                        }
                    }
                    $pendingKeys = array_values(array_unique($regroupKeys));
                    $this->logger->debug('Pessimistic lock re-grouping keys after region split', [
                        'regionId' => $region->regionId,
                        'keyCount' => count($pendingKeys),
                    ]);
                    break;
                }

                // A lock that could not be acquired within the configured wait
                // budget must fail the transaction instead of silently continuing
                // to prewrite without a lock (issue #219, TXN-14).
                if ($lastRegionError instanceof \CrazyGoat\TiKV\Client\Exception\RegionException) {
                    // The budget ran out while region errors kept coming — the
                    // region failure is the reason the lock was never acquired,
                    // so surface it rather than a lock timeout (issue #500).
                    throw $lastRegionError;
                }
                if ($needRetry) {
                    throw new LockWaitTimeoutException(
                        $regionKeys[0] ?? '',
                        $this->maxBackoffMs,
                    );
                }

                $isFirstLock = false;
            }
        }
    }

    /**
     * Whether the region range covers every key (endKey === '' means the
     * last region, which extends to +inf).
     *
     * @param string[] $keys
     */
    private function regionCoversAllKeys(RegionInfo $region, array $keys): bool
    {
        foreach ($keys as $key) {
            if (
                $key < $region->startKey
                || ($region->endKey !== '' && $key >= $region->endKey)
            ) {
                return false;
            }
        }

        return true;
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

        $forUpdateTs = $state->getMaxForUpdateTs() ?? $this->startTs;

        // Array + count() instead of an int counter: PHPStan level 9 proves
        // an int counter incremented only inside the regroup branch is
        // always 0 at the check site (single-pass narrowing) — same trap as
        // PESSIMISTIC_LOCK_MAX_REGROUPS (issue #503 review).
        /** @var list<true> $regroups */
        $regroups = [];
        $pendingKeys = $pessimisticKeys;

        while ($pendingKeys !== []) {
            $keysByRegion = $this->groupStringsByRegion($pendingKeys);
            $pendingKeys = [];

            foreach ($keysByRegion as $groupIndex => $regionData) {
                $regionKeys = $regionData['keys'];
                $firstKey = $regionKeys[0] ?? '';

                try {
                    $retryExecutor->execute($firstKey, function () use ($firstKey, $regionKeys, $forUpdateTs): null {
                        // Resolve the region on every attempt so cache
                        // invalidation and leader switching performed by the
                        // retry executor take effect (issue #502) — see
                        // batchRollback() for the full stale-capture
                        // rationale (#267/#500 pattern) and the #505
                        // regroup-on-split rationale.
                        $region = $this->regionResolver->getRegionInfo($firstKey);
                        if (!$this->regionCoversAllKeys($region, $regionKeys)) {
                            throw new RollbackRegroupSignal(
                                'Re-resolved region no longer covers the rollback key group',
                            );
                        }
                        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

                        $request = new PessimisticRollbackRequest();
                        $request->setContext(RegionContextFactory::fromRegionInfo($region));
                        $request->setStartVersion($this->startTs);
                        $request->setForUpdateTs($forUpdateTs);
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
                } catch (RollbackRegroupSignal) {
                    // Same regroup-on-split recovery as batchRollback()
                    // above (issue #505).
                    if (count($regroups) >= self::ROLLBACK_MAX_REGROUPS) {
                        throw new RegionException(
                            'pessimistic rollback',
                            'Region split repeatedly invalidated the rollback group',
                        );
                    }
                    $regroups[] = true;
                    $regroupKeys = $regionKeys;
                    foreach ($keysByRegion as $laterIndex => $laterData) {
                        if ($laterIndex > $groupIndex) {
                            $regroupKeys = array_merge($regroupKeys, $laterData['keys']);
                        }
                    }
                    $pendingKeys = array_values(array_unique($regroupKeys));
                    $this->logger->debug('Pessimistic rollback re-grouping keys after region split', [
                        'regionId' => $regionData['region']->regionId,
                        'keyCount' => count($pendingKeys),
                    ]);
                    break;
                }
            }
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
            // PHP coerces integer-like string keys to int array keys, so
            // string-cast before passing to the proto setter (issue #322).
            $mutation->setKey((string) $key);

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
