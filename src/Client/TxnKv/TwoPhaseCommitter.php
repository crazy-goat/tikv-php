<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\BatchRollbackRequest;
use CrazyGoat\Proto\Kvrpcpb\BatchRollbackResponse;
use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\Context;
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
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\CheckedGrpcFuture;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
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
use CrazyGoat\TiKV\Client\Util\KeyOrder;
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
 *
 * ## Lock TTL policy (issue #218, TXN-13)
 *
 * The optimistic prewrite lock TTL scales with the transaction's write-set
 * size: `3000 ms + 10 ms per mutation`, capped at `120000 ms`. The baseline
 * alone was too short for a multi-region prewrite loop, so a concurrent
 * reader could roll the still-being-prewritten locks back and the commit
 * failed with `TxnLockNotFound` (a torn transaction). The cap mirrors
 * client-go's `maxLockTTL`.
 *
 * ## Heartbeat contract
 *
 * During the prewrite loop the committer sends a {@see heartbeat()} for the
 * primary lock once the elapsed time reaches half of the computed TTL (the
 * window resets after each heartbeat). One-phase commits are exempt: 1PC
 * leaves no lock, so a heartbeat would fail an already-committed transaction.
 * A lock can only be heartbeated between region prewrites — a single prewrite
 * RPC that by itself outlives the TTL cannot be kept alive (PHP has no timer);
 * the scaled TTL only narrows that window. Application-level long
 * transactions — anything that keeps a transaction open between operations
 * — must call {@see \CrazyGoat\TiKV\Client\TxnKv\Transaction::heartbeat()}
 * themselves before the granted TTL elapses; 10 s is a safe default.
 */
final readonly class TwoPhaseCommitter
{
    private const OPTIMISTIC_LOCK_TTL_MS = 3000;

    /**
     * Additional optimistic lock TTL granted per write-set mutation (issue
     * #218, TXN-13). client-go scales the optimistic lock TTL by byte size;
     * the mutation count is the equivalent, deterministic signal available
     * on the client side.
     */
    private const LOCK_TTL_PER_MUTATION_MS = 10;

    /**
     * Upper bound for the optimistic lock TTL (issue #218, TXN-13) — mirrors
     * client-go's `maxLockTTL`. Caps the prewrite liveness window for a
     * pathologically large write set.
     */
    private const MAX_LOCK_TTL_MS = 120000;

    /**
     * Pessimistic locks use a fixed TTL (deliberately out of scope for
     * issue #218): the pessimistic path locks every key up front and the
     * prewrite heartbeat contract applies to the optimistic write path only.
     */
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
        /**
         * Optional monotonic clock (milliseconds since an arbitrary epoch)
         * used by the prewrite heartbeat check. Injectable for deterministic
         * tests (issue #218); defaults to the wall clock. Typed `\Closure`,
         * not `callable`: PHP properties cannot be typed callable.
         */
        private ?\Closure $clock = null,
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

    /**
     * Build the RPC context for a transaction request, carrying the
     * transaction's priority (issue #441). Mirrors client-go's SetPriority:
     * the value maps directly onto the Kvrpcpb CommandPri enum
     * (Normal = 0, Low = 1, High = 2) and is applied to all transactional
     * RPCs: prewrite, pessimistic-lock, commit, heartbeat (TxnHeartBeat)
     * and rollback (BatchRollback, PessimisticRollback). The value is
     * passed through without validation.
     */
    private function buildContext(RegionInfo $region): Context
    {
        $context = RegionContextFactory::fromRegionInfo($region);
        $context->setPriority($this->priority);

        return $context;
    }

    private function nowMs(): int
    {
        if ($this->clock instanceof \Closure) {
            return ($this->clock)();
        }

        return (int) (microtime(true) * 1000);
    }

    /**
     * Lock TTL for the optimistic prewrite, scaled with the write-set size.
     * Baseline 3000 ms + LOCK_TTL_PER_MUTATION_MS per mutation, capped at
     * MAX_LOCK_TTL_MS. (client-go scales by byte size; mutation count is the
     * equivalent, deterministic signal here.)
     *
     * @param Mutation[] $mutations
     */
    private function optimisticLockTtlMs(array $mutations): int
    {
        $ttl = self::OPTIMISTIC_LOCK_TTL_MS + count($mutations) * self::LOCK_TTL_PER_MUTATION_MS;

        return min($ttl, self::MAX_LOCK_TTL_MS);
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
        $primary = $state->getPessimisticPrimaryKey() ?? $state->getPrimaryKey();

        if ($this->pessimistic) {
            $this->pessimisticLockBatch($primary, $state);
        }

        $mutations = $this->buildMutations($state);
        // The lock TTL the prewrite will actually carry: pessimistic
        // transactions use the fixed pessimistic TTL, optimistic transactions
        // scale with the write-set size (issue #218, TXN-13). The same value
        // drives the prewrite-heartbeat trigger and advise, so the pessimistic
        // path heartbeats against its own 30 s TTL rather than the smaller
        // optimistic one.
        $lockTtlMs = $this->pessimistic
            ? self::PESSIMISTIC_LOCK_TTL_MS
            : $this->optimisticLockTtlMs($mutations);
        $prewriteStartedAtMs = $this->nowMs();
        $primaryLockWritten = false;
        $keysByRegion = $this->groupMutationsByRegion($mutations);
        $groupedMutationCount = 0;
        foreach ($keysByRegion as $regionData) {
            $groupedMutationCount += count($regionData['mutations']);
        }
        if ($groupedMutationCount !== count($mutations)) {
            throw new InvalidStateException(
                'Not all transaction mutations were assigned to a region; refusing to report commit success',
            );
        }
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

        /**
         * Split the region groups into the primary region's group and the
         * secondary groups, preserving $keysByRegion order (grouping
         * follows mutation order, so in the optimistic path the primary's
         * region comes first — see the TXN-13 FAQ note).
         *
         * @var array{region: RegionInfo, mutations: Mutation[], isPrimary: bool, firstKey: string}|null $primaryGroup
         */
        $primaryGroup = null;
        /** @var list<array{region: RegionInfo, mutations: Mutation[], isPrimary: bool, firstKey: string}> $secondaryGroups */
        $secondaryGroups = [];
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

            $group = [
                'region' => $regionData['region'],
                'mutations' => $regionMutations,
                'isPrimary' => $isPrimaryRegion,
                'firstKey' => $firstKey,
            ];
            if ($isPrimaryRegion && $primaryGroup === null) {
                $primaryGroup = $group;
            } else {
                $secondaryGroups[] = $group;
            }
        }

        /**
         * Prewrite the primary region's group: the first attempt is
         * dispatched eagerly and awaited immediately, so the primary lock is
         * durably written before any secondary prewrite is issued (the
         * durability constraint client-go's twoPhaseCommitter implements).
         *
         * @return array{minCommitTs: int, onePcCommitTs: int}
         */
        $prewritePrimary = function () use (
            $primaryGroup,
            $lockTtlMs,
            $primary,
            $state,
            $onePc,
            $async,
            $secondaries,
            $retryExecutor,
            $classifier,
        ): array {
            $group = $primaryGroup;
            if ($group === null) {
                throw new InvalidStateException(
                    'No region resolved for the primary mutation; refusing to report commit success',
                );
            }

            $useOnePc = $group['isPrimary'] && $onePc;
            $useAsyncCommit = $group['isPrimary'] && $async;
            $regionSecondaries = $useAsyncCommit ? $secondaries : [];

            $future = $this->prewriteWithRetry(
                $group,
                $lockTtlMs,
                $primary,
                $state,
                $useOnePc,
                $useAsyncCommit,
                $regionSecondaries,
                $retryExecutor,
                $classifier,
            );

            /** @var array{minCommitTs: int, onePcCommitTs: int} $result */
            $result = $future->waitForExecutor();

            return $result;
        };

        /**
         * Prewrite the secondary region groups in parallel (issue #291):
         * every region's first attempt is dispatched before any wait begins,
         * so their server-side latencies overlap. The first failing region
         * is rethrown in dispatch order, preserving the sequential
         * "abort at the first failing region" exception semantics.
         */
        $prewriteSecondaries = function () use (
            $secondaryGroups,
            $lockTtlMs,
            $primary,
            $state,
            $retryExecutor,
            $classifier,
            &$maxMinCommitTs,
        ): void {
            if ($secondaryGroups === []) {
                return;
            }

            $regionCalls = [];
            foreach ($secondaryGroups as $group) {
                $regionCalls[] = fn(): CheckedGrpcFuture => $this->prewriteWithRetry(
                    $group,
                    $lockTtlMs,
                    $primary,
                    $state,
                    false,
                    false,
                    [],
                    $retryExecutor,
                    $classifier,
                );
            }

            $batchExecutor = new BatchAsyncExecutor($this->logger);
            try {
                $results = $batchExecutor->executeParallel(
                    $regionCalls,
                    $this->timeoutConfig->batchDeadlineMs,
                );
            } catch (BatchPartialFailureException $e) {
                throw $e->getFirstRegionError();
            }

            foreach ($results as $result) {
                if (
                    is_array($result)
                    && isset($result['minCommitTs'])
                    && is_int($result['minCommitTs'])
                ) {
                    $maxMinCommitTs = max($maxMinCommitTs, $result['minCommitTs']);
                }
            }
        };

        // Keep the primary lock alive when a long prewrite phase would let
        // its TTL expire before commit (issue #218, TXN-13). Only after the
        // primary region has been prewritten (heartbeating a not-yet-written
        // primary would surface TxnLockNotFound), and never for a one-phase
        // commit: 1PC writes the commit record inside the prewrite and
        // leaves no lock, so a heartbeat would come back TxnNotFound and
        // fail an already-committed transaction. Reset the window after each
        // heartbeat. With multi-region async commit the primary region is
        // deliberately prewritten LAST, so no heartbeat fires before the
        // primary exists — acceptable because async commit is capped at
        // ASYNC_COMMIT_MAX_KEYS and the commit decision is already carried
        // by the primary lock. A single prewrite RPC that by itself
        // outlives the TTL cannot be heartbeated mid-RPC (PHP has no
        // timer); the scaled TTL only reduces that window.
        $maybeHeartbeat = function (
            bool $primaryLockWritten,
        ) use (
            $primary,
            $state,
            $retryExecutor,
            $classifier,
            $lockTtlMs,
            &$prewriteStartedAtMs,
            $onePc,
        ): void {
            if (
                $primaryLockWritten
                && !$onePc
                && $this->nowMs() - $prewriteStartedAtMs >= intdiv($lockTtlMs, 2)
            ) {
                $this->heartbeat($primary, $state, $retryExecutor, $classifier, $lockTtlMs);
                $prewriteStartedAtMs = $this->nowMs();
            }
        };

        // Async commit (client-go parity): the primary region must be
        // prewritten LAST — the primary's prewrite is the commit-decision
        // barrier, and prewriting it first would let a concurrent reader
        // observe a decided-looking primary while a secondary region's
        // prewrite is still in flight. The secondary regions are fanned out
        // in parallel (issue #291) and awaited before the primary's prewrite
        // is even dispatched, preserving that barrier.
        if ($async && count($keysByRegion) > 1) {
            $prewriteSecondaries();

            $result = $prewritePrimary();
            $maxMinCommitTs = max($maxMinCommitTs, $result['minCommitTs']);
            $primaryMinCommitTs = $result['minCommitTs'];
            $onePcCommitTs = $result['onePcCommitTs'];
            // TiKV declines async commit by answering the primary
            // prewrite with min_commit_ts = 0.
            $asyncAccepted = $result['minCommitTs'] > 0;
            $primaryLockWritten = true;
            $maybeHeartbeat($primaryLockWritten);
        } else {
            // Optimistic / single-region / non-async path: the primary
            // region is prewritten (and awaited) FIRST — its lock must be
            // durably written before secondary locks exist (a secondary
            // lock without the primary is unrecoverable) — then the
            // secondary regions fan out in parallel (issue #291).
            $result = $prewritePrimary();
            $maxMinCommitTs = max($maxMinCommitTs, $result['minCommitTs']);
            if ($primaryGroup !== null) {
                $primaryMinCommitTs = $result['minCommitTs'];
                $onePcCommitTs = $result['onePcCommitTs'];
                // TiKV declines async commit by answering the primary
                // prewrite with min_commit_ts = 0.
                $asyncAccepted = $result['minCommitTs'] > 0;
                $primaryLockWritten = true;
            }
            $maybeHeartbeat($primaryLockWritten);

            $prewriteSecondaries();
            $maybeHeartbeat($primaryLockWritten);
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

        // Primary commit failures are not retried here: transport failures
        // leave the outcome undetermined, and post-commit-point errors must
        // never trigger transaction rollback.
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
            $request->setContext($this->buildContext($region));
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
     * Build one region's PrewriteRequest (shared by the dispatch and retry
     * paths).
     *
     * @param Mutation[] $mutations
     * @param string[] $secondaries Secondary keys carried on the primary
     *                              region's prewrite when async commit is
     *                              used (empty otherwise)
     */
    private function buildPrewriteRequest(
        RegionInfo $region,
        array $mutations,
        int $lockTtlMs,
        string $primary,
        TransactionState $state,
        bool $useOnePc = false,
        bool $useAsyncCommit = false,
        array $secondaries = [],
    ): PrewriteRequest {
        $request = new PrewriteRequest();
        $request->setContext($this->buildContext($region));
        $request->setMutations($mutations);
        $request->setPrimaryLock($primary);
        $request->setStartVersion($this->startTs);
        $request->setLockTtl($lockTtlMs);

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
            // Keep the deferred safety net for both eager and compatibility
            // modes: it detects an intervening commit between a read and the
            // physical lock/prewrite (issue #209).
            $actions = [];
            foreach ($mutations as $mutation) {
                $actions[] = PessimisticAction::DO_CONSTRAINT_CHECK;
            }
            $request->setPessimisticActions($actions);
        }

        return $request;
    }

    /**
     * Issue (eagerly send) one region's prewrite without waiting for the
     * response (issue #291): the returned future's wait phase resolves the
     * response, runs the region-error check and the KeyError handling.
     *
     * Prewrite is the last transaction RPC that used to run outside a
     * RetryExecutor (issue #213, TXN-08); it is now wrapped by
     * {@see self::prewriteWithRetry()} via
     * CheckedGrpcFuture::fromRetryableDispatch(), so region errors
     * (NotLeader, EpochNotMatch, RegionNotFound, ServerIsBusy) and resolved
     * lock conflicts retry instead of aborting the whole transaction. The
     * region is re-resolved inside the retry closure so the leader switch /
     * cache invalidation performed by the executor takes effect on the next
     * attempt — the same stale-capture class as #267/#500/#502. Prewrite is
     * idempotent for a given start_ts, so replay is safe. Known limitation:
     * there is no split-regroup path here. If the region split after
     * grouping, re-resolving by the first mutation's key can return a
     * region that no longer covers the whole group, and the executor
     * keeps retrying the over-wide prewrite until its budget runs out
     * (same limitation as pessimisticLockBatch() #500 and batch
     * rollback #502).
     *
     * @param Mutation[] $mutations
     * @param string[] $secondaries
     */
    private function prewriteForRegionAsync(
        RegionInfo $region,
        array $mutations,
        int $lockTtlMs,
        string $primary,
        TransactionState $state,
        bool $useOnePc = false,
        bool $useAsyncCommit = false,
        array $secondaries = [],
    ): CheckedGrpcFuture {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);
        $request = $this->buildPrewriteRequest(
            $region,
            $mutations,
            $lockTtlMs,
            $primary,
            $state,
            $useOnePc,
            $useAsyncCommit,
            $secondaries,
        );

        $this->logger->debug('Prewrite', [
            'regionId' => $region->regionId,
            'keyCount' => count($mutations),
            'pessimistic' => $this->pessimistic,
        ]);

        $future = $this->grpc->callAsync(
            $address,
            'tikvpb.Tikv',
            'KvPrewrite',
            $request,
            PrewriteResponse::class,
            $this->timeoutMs('write'),
        );

        return CheckedGrpcFuture::fromCallable(function () use ($future, $region): array {
            /** @var PrewriteResponse $response */
            $response = $future->wait();
            // The prewrite runs under RetryExecutor via
            // prewriteWithRetry() (issue #213, TXN-08), so the executor's
            // handleNotLeader() is the sole owner of NotLeader drops: it
            // switches to the hinted leader when the peer is still cached
            // and only invalidates otherwise. Self-invalidating here
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
        }, $future);
    }

    /**
     * Wrap one region group's prewrite in the retry executor, keeping the
     * first attempt eagerly dispatched (client-side fan-out — see
     * CheckedGrpcFuture::fromRetryableDispatch) while retries run in the
     * wait phase with fresh region resolution.
     *
     * @param array{region: RegionInfo, mutations: Mutation[], isPrimary: bool, firstKey: string} $group
     * @param string[] $secondaries
     */
    private function prewriteWithRetry(
        array $group,
        int $lockTtlMs,
        string $primary,
        TransactionState $state,
        bool $useOnePc,
        bool $useAsyncCommit,
        array $secondaries,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): CheckedGrpcFuture {
        $firstKey = $group['firstKey'];

        return CheckedGrpcFuture::fromRetryableDispatch(
            function () use (
                $group,
                $firstKey,
                $lockTtlMs,
                $primary,
                $state,
                $useOnePc,
                $useAsyncCommit,
                $secondaries,
            ): CheckedGrpcFuture {
                $region = $this->regionResolver->getRegionInfo($firstKey);

                return $this->prewriteForRegionAsync(
                    $region,
                    $group['mutations'],
                    $lockTtlMs,
                    $primary,
                    $state,
                    $useOnePc,
                    $useAsyncCommit,
                    $secondaries,
                );
            },
            $retryExecutor,
            $firstKey,
            $classifier,
        );
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
            throw new InvalidStateException(
                'No regions resolved for a non-empty commit key set; refusing to report success',
            );
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

        // Fail closed (issue #326): reaching this point without finding the
        // primary key in any resolved region group means the primary was
        // dropped or the caller passed a key list without it. Committing an
        // arbitrary region first "as if" it were the primary would break the
        // primary-first invariant above; throw instead of silently promoting
        // a random region to primary.
        if ($primaryRegionId === null) {
            throw new InvalidStateException(sprintf(
                'Primary key %s was not found in any resolved region group;'
                . ' refusing to commit an arbitrary region as primary',
                KeyRedactor::redact($primary),
            ));
        }

        // Commit the primary first. Failures here are fatal: do not retry.
        // No RetryExecutor wraps this call, so commitForRegion must handle
        // NotLeader drops itself (issue #474 review round 3).
        $primaryRegionData = $keysByRegion[$primaryRegionId];
        try {
            $this->commitForRegionAsync(
                $primaryRegionData['region'],
                $primaryRegionData['keys'],
                $state,
                notLeaderOwnedByRetryExecutor: false,
            )->waitForExecutor();
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
        // secondary regions in parallel (issue #291): every region's first
        // attempt is dispatched before any wait begins, so their
        // server-side latencies overlap. The primary's commit has already
        // been acknowledged above, which is the protocol's ordering
        // requirement.
        unset($keysByRegion[$primaryRegionId]);

        if ($keysByRegion === []) {
            return;
        }

        $regionCalls = [];
        foreach ($keysByRegion as $regionId => $regionData) {
            $region = $regionData['region'];
            $regionKeys = $regionData['keys'];
            $firstKey = $regionKeys[0] ?? '';

            $regionCalls[$regionId] = fn(): CheckedGrpcFuture => $this->secondaryCommitSwallowing(
                $region,
                $regionKeys,
                $firstKey,
                $state,
                $retryExecutor,
                $classifier,
                $regionId,
            );
        }

        $batchExecutor = new BatchAsyncExecutor($this->logger);
        // Note on exception semantics (issue #291 review): with
        // batchDeadlineMs > 0 a BatchDeadlineExceededException can escape
        // commit() AFTER the status was already set to Committed above —
        // the secondary commits were dispatched but not all awaited within
        // the deadline. This is deliberate: the primary is durably
        // committed (the commit point), so the data is durable either way;
        // leftover secondary locks are resolved by readers. Do NOT catch it
        // here and do NOT change the status — the deadline defaults to
        // 0 (disabled).
        $batchExecutor->executeParallel($regionCalls, $this->timeoutConfig->batchDeadlineMs);
    }

    /**
     * One secondary region's commit for the #291 fan-out: the dispatch
     * phase runs the (eagerly-sent) retryable future's constructor; the
     * WAIT phase — this method's returned future — swallows and logs the
     * region's failure.
     *
     * Secondary commit failures do NOT fail the transaction: the primary is
     * already committed, so the data is durable. The leftover locks are
     * resolved by other readers through the lock resolver (same policy as
     * client-go, issue #215). Only \Exception is swallowed
     * (retryable/fatal TiKV errors); \Error subclasses (TypeError,
     * AssertionError, ...) still propagate because they indicate genuine
     * defects. Each region's failure is swallowed individually — one
     * region's failure must neither abort the other in-flight secondary
     * commits nor change their outcome (issue #291).
     *
     * @param string[] $regionKeys
     */
    private function secondaryCommitSwallowing(
        RegionInfo $region,
        array $regionKeys,
        string $firstKey,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
        int $regionId,
    ): CheckedGrpcFuture {
        $future = $this->commitWithRetry(
            $region,
            $regionKeys,
            $firstKey,
            $state,
            $retryExecutor,
            $classifier,
        );

        return CheckedGrpcFuture::fromCallable(function () use ($future, $regionId) {
            try {
                return $future->waitForExecutor();
            } catch (\Exception $e) {
                $this->logger->warning('Secondary commit failed; locks will be resolved by readers', [
                    'regionId' => $regionId,
                    'exception' => $e,
                ]);

                return null;
            }
        }, $future->inner());
    }

    /**
     * Wrap one region's commit in the retry executor for the secondary
     * fan-out (issue #291), keeping the first attempt eagerly dispatched
     * (client-side fan-out — see CheckedGrpcFuture::fromRetryableDispatch)
     * while retries run in the wait phase.
     *
     * @param string[] $keys
     */
    private function commitWithRetry(
        RegionInfo $region,
        array $keys,
        string $firstKey,
        TransactionState $state,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): CheckedGrpcFuture {
        return CheckedGrpcFuture::fromRetryableDispatch(
            fn(): CheckedGrpcFuture => $this->commitForRegionAsync($region, $keys, $state),
            $retryExecutor,
            $firstKey,
            $classifier,
        );
    }

    /**
     * Issue (eagerly send) one region's commit without waiting for the
     * response; the returned future's wait phase runs the region-error
     * check and the KeyError handling.
     *
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
    private function commitForRegionAsync(
        RegionInfo $region,
        array $keys,
        TransactionState $state,
        bool $notLeaderOwnedByRetryExecutor = true,
    ): CheckedGrpcFuture {
        $commitTs = $state->getCommitTs();
        if ($commitTs === null) {
            throw new InvalidStateException(
                'commitTs must be set before committing; commit() must run first.',
            );
        }

        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        $request = new CommitRequest();
        $request->setContext($this->buildContext($region));
        $request->setStartVersion($this->startTs);
        $request->setKeys($keys);
        $request->setCommitVersion($commitTs);

        $this->logger->debug('Commit', [
            'regionId' => $region->regionId,
            'keyCount' => count($keys),
            'commitTs' => $commitTs,
        ]);

        $future = $this->grpc->callAsync(
            $address,
            'tikvpb.Tikv',
            'KvCommit',
            $request,
            CommitResponse::class,
            $this->timeoutMs('write'),
        );

        return CheckedGrpcFuture::fromCallable(function () use ($future, $region, $notLeaderOwnedByRetryExecutor) {
            /** @var CommitResponse $response */
            $response = $future->wait();
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

            return $response;
        }, $future);
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

        // A commit response carrying any other KeyError is not a successful
        // commit. Until a variant has an explicit, safe recovery path, fail
        // closed rather than marking the transaction committed.
        throw new TiKvException('Commit failed: ' . KeyErrorDescriber::describe($error));
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

            // Fan out all region groups in parallel (issue #291): every
            // group's first BatchRollback attempt is dispatched before any
            // wait begins, so their server-side latencies overlap. The
            // responses are then awaited in dispatch order, so the first
            // failing group is the one surfaced — the same exception the
            // sequential loop aborted with.
            $regionCalls = [];
            foreach ($keysByRegion as $regionData) {
                $regionKeys = $regionData['keys'];
                $firstKey = $regionKeys[0] ?? '';

                $regionCalls[] = fn(): CheckedGrpcFuture => $this->batchRollbackWithRetry(
                    $firstKey,
                    $regionKeys,
                    $retryExecutor,
                    $classifier,
                );
            }

            $batchExecutor = new BatchAsyncExecutor($this->logger);
            try {
                $batchExecutor->executeParallel($regionCalls, $this->timeoutConfig->batchDeadlineMs);
                continue;
            } catch (BatchPartialFailureException $e) {
                // Preserve the sequential first-abort exception semantics
                // (issue #291): rethrow the first failing group's original
                // exception instead of the aggregate.
                throw $e->getFirstRegionError();
            } catch (\RuntimeException $e) {
                // RollbackRegroupSignal deliberately extends \RuntimeException,
                // not TiKvException, so it escapes the retry closures and the
                // executor's TiKvException handling.
                if (!$e instanceof RollbackRegroupSignal) {
                    throw $e;
                }
                $signal = $e;
                // The re-resolved region does not cover the whole key
                // group (a split happened since grouping, issue #505):
                // re-group the signalling group's keys together with all
                // not-yet-processed groups' keys against the fresh
                // region layout and continue. Rolling back keys of
                // already-processed groups again would be idempotent,
                // but they are not re-sent (mirrors the #503 pattern).
                // With the #291 fan-out the signal arrives from the wait
                // phase of whichever group re-resolved to a shrinking
                // region; groups dispatched after it were cancelled by the
                // executor and their keys are re-sent below.
                $groupIndex = null;
                $regionKeys = [];
                foreach ($keysByRegion as $index => $regionData) {
                    $keys0 = $regionData['keys'][0] ?? '';
                    if ($keys0 !== '' && $keys0 === $signal->groupFirstKey) {
                        $groupIndex = $index;
                        $regionKeys = $regionData['keys'];
                        break;
                    }
                }
                if ($groupIndex === null) {
                    // The signalling group is no longer present (cannot
                    // happen: $keysByRegion was not mutated since dispatch).
                    throw new RegionException(
                        'rollback',
                        'Rollback re-group signal referenced an unknown key group',
                    );
                }
                $signalledRegion = $keysByRegion[$groupIndex]['region'];

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
                    'regionId' => $signalledRegion->regionId,
                    'keyCount' => count($pendingKeys),
                ]);
            }
        }
    }

    /**
     * Wrap one region group's BatchRollback in the retry executor for the
     * #291 fan-out, keeping the first attempt eagerly dispatched
     * (client-side fan-out — see CheckedGrpcFuture::fromRetryableDispatch)
     * while retries run in the wait phase.
     *
     * @param string[] $regionKeys
     */
    private function batchRollbackWithRetry(
        string $firstKey,
        array $regionKeys,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): CheckedGrpcFuture {
        return CheckedGrpcFuture::fromRetryableDispatch(
            function () use ($firstKey, $regionKeys): CheckedGrpcFuture {
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
                        $firstKey,
                    );
                }
                $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

                $request = new BatchRollbackRequest();
                $request->setContext($this->buildContext($region));
                $request->setStartVersion($this->startTs);
                $request->setKeys($regionKeys);

                $this->logger->debug('BatchRollback', [
                    'regionId' => $region->regionId,
                    'keyCount' => count($regionKeys),
                ]);

                $future = $this->grpc->callAsync(
                    $address,
                    'tikvpb.Tikv',
                    'KvBatchRollback',
                    $request,
                    BatchRollbackResponse::class,
                    $this->timeoutMs('write'),
                );

                return CheckedGrpcFuture::fromCallable(function () use ($future, $region) {
                    /** @var BatchRollbackResponse $response */
                    $response = $future->wait();
                    RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

                    $error = $response->getError();
                    if ($error !== null) {
                        $this->handleRollbackError($error);
                    }

                    return $response;
                }, $future);
            },
            $retryExecutor,
            $firstKey,
            $classifier,
        );
    }

    private function handleRollbackError(KeyError $error): void
    {
        $locked = $error->getLocked();
        if ($locked !== null) {
            $rawPrimary = $locked->getPrimaryLock();
            $lockPrimary = (string) ($rawPrimary !== '' ? $rawPrimary : $locked->getKey());
            // Active locks propagate a retryable exception; after they expire,
            // retry rollback so the rollback RPC can be attempted again.
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

    /**
     * Build one region's PessimisticLockRequest (shared by the #291
     * first-attempt fan-out dispatch and the in-loop retry attempts).
     *
     * @param Mutation[] $mutations
     */
    private function buildPessimisticLockRequest(
        RegionInfo $region,
        array $mutations,
        string $primary,
        int $forUpdateTs,
        bool $isFirstLock,
    ): PessimisticLockRequest {
        $request = new PessimisticLockRequest();
        $request->setContext($this->buildContext($region));
        $request->setMutations($mutations);
        $request->setPrimaryLock($primary);
        $request->setStartVersion($this->startTs);
        $request->setLockTtl(self::PESSIMISTIC_LOCK_TTL_MS);
        $request->setForUpdateTs($forUpdateTs);
        $request->setIsFirstLock($isFirstLock);
        $request->setReturnValues(true);

        return $request;
    }

    /**
     * Dispatch every region's FIRST pessimistic-lock attempt in parallel
     * (issue #291): all sends are issued before any response is processed.
     *
     * isFirstLock is true only for the first group of the pass: the flag
     * marks the transaction's very first pessimistic lock RPC, and in the
     * conflict-free sequential order exactly the first group's RPC carried
     * it (later groups saw isFirstLock=false after the first group locked
     * successfully). A bool parameter rather than an inline read of the
     * caller's loop-carried local: PHPStan L9 narrows the local to its
     * literal initializer at the dispatch site (the loop-carried mutation
     * happens in the processing loop below), the same narrowing trap as the
     * regroup counters (issue #503 review).
     *
     * @param array<int, array{region: RegionInfo, keys: string[]}> $keysByRegion
     * @return array{0: array<int, GrpcFuture>, 1: array<int, Mutation[]>}
     */
    private function dispatchPessimisticLockFirstAttempts(
        array $keysByRegion,
        string $primary,
        int $forUpdateTs,
        bool $isFirstLock,
    ): array {
        $firstGroupIndex = array_key_first($keysByRegion);
        $firstAttempts = [];
        $mutationsByGroup = [];
        foreach ($keysByRegion as $groupIndex => $regionData) {
            $groupRegion = $regionData['region'];
            $groupKeys = $regionData['keys'];

            $groupMutations = [];
            foreach ($groupKeys as $key) {
                $mutation = new Mutation();
                $mutation->setOp(Op::PessimisticLock);
                $mutation->setKey($key);
                $groupMutations[] = $mutation;
            }
            $mutationsByGroup[$groupIndex] = $groupMutations;

            $address = $this->regionResolver->resolveStoreAddress($groupRegion->leaderStoreId);
            $request = $this->buildPessimisticLockRequest(
                $groupRegion,
                $groupMutations,
                $primary,
                $forUpdateTs,
                $isFirstLock && $groupIndex === $firstGroupIndex,
            );

            $this->logger->debug('PessimisticLock', [
                'regionId' => $groupRegion->regionId,
                'keyCount' => count($groupKeys),
                'attempt' => 1,
                'forUpdateTs' => $forUpdateTs,
            ]);

            $firstAttempts[$groupIndex] = $this->grpc->callAsync(
                $address,
                'tikvpb.Tikv',
                'KvPessimisticLock',
                $request,
                PessimisticLockResponse::class,
                $this->timeoutMs('write'),
            );
        }

        return [$firstAttempts, $mutationsByGroup];
    }

    /**
     * Acquire pessimistic locks for an eager write operation.
     *
     * This is intentionally synchronous: Transaction::set()/delete() call it
     * before staging the value, so lock conflicts surface at the write call.
     * The lower-level region fan-out and wait loop remains shared with the
     * deferred commit path.
     *
     * @param string[] $keys
     */
    public function lockKeys(
        array $keys,
        TransactionState $state,
        int $forUpdateTs,
        string $primary,
    ): void {
        $keys = array_values(array_unique($keys));
        if ($keys === []) {
            return;
        }

        $primary = $state->getPessimisticPrimaryKey() ?? $primary;
        $state->setPessimisticPrimaryKey($primary);
        $state->updateMaxForUpdateTs($forUpdateTs);
        try {
            $this->lockKeysInternal($keys, $state, $primary, $forUpdateTs);
        } catch (\Throwable $failure) {
            $state->markPessimisticWriteFailed();
            throw $failure;
        }
    }

    private function pessimisticLockBatch(
        string $primary,
        TransactionState $state,
    ): void {
        $keys = array_values(array_unique($state->getPendingLockKeys()));
        $state->clearPendingLockKeys();
        $keys = array_values(array_filter(
            $keys,
            static fn (string $key): bool => !$state->hasPessimisticLock($key),
        ));

        if ($keys === []) {
            return;
        }

        foreach ($keys as $key) {
            $state->addPessimisticLockAttempt($key);
        }

        $primary = $state->getPessimisticPrimaryKey() ?? $primary;
        try {
            $forUpdateTs = $state->getMaxForUpdateTs() ?? $this->pdClient->getTimestamp();
            $state->updateMaxForUpdateTs($forUpdateTs);
            $this->lockKeysInternal($keys, $state, $primary, $forUpdateTs);
        } catch (\Throwable $failure) {
            $state->markPessimisticWriteFailed();
            throw $failure;
        }
    }

    /**
     * @param string[] $keys
     */
    private function lockKeysInternal(
        array $keys,
        TransactionState $state,
        string $primary,
        int $forUpdateTs,
    ): void {
        foreach ($keys as $key) {
            $state->addPessimisticLockAttempt($key);
        }

        // The first successful lock request in the transaction is the only
        // one that carries is_first_lock. Derive it from acknowledged locks,
        // not from the caller's local state, so eager calls share one flag.
        $isFirstLock = !$state->hasAcknowledgedPessimisticLocks();
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

            // The caller supplies the timestamp for this logical locking
            // pass. Keep it stable across region retries so a read-derived
            // timestamp cannot be silently advanced past an intervening
            // commit; the transaction-wide maximum is still used later for
            // prewrite and rollback.
            $state->updateMaxForUpdateTs($forUpdateTs);

            // Dispatch every region's FIRST pessimistic-lock attempt in
            // parallel (issue #291): all sends are issued before any region's
            // response is processed, so the lock-free common case costs one
            // round trip regardless of region count. Responses are then
            // processed in dispatch order below, so the per-group retry
            // state machine, lock-conflict handling and exception behavior
            // are identical to the sequential loop.
            [$firstAttempts, $mutationsByGroup] = $this->dispatchPessimisticLockFirstAttempts(
                $keysByRegion,
                $primary,
                $forUpdateTs,
                $isFirstLock,
            );

            try {
                foreach ($keysByRegion as $groupIndex => $regionData) {
                    $region = $regionData['region'];
                    $regionKeys = $regionData['keys'];
                    $mutations = $mutationsByGroup[$groupIndex];

                    $elapsedMs = 0;
                    $attempt = 0;
                    $needRetry = false;
                    $lastRegionError = null;
                    $regroup = false;
                    do {
                        $attempt++;
                        // First attempt uses the region already supplied by
                        // groupStringsByRegion() (and the future dispatched in
                        // the fan-out above). On retries (issue #500) the
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

                        $regionError = null;
                        try {
                            if ($attempt === 1) {
                                // Response from the future dispatched in the
                                // fan-out above (issue #291).
                                /** @var PessimisticLockResponse $response */
                                $response = $firstAttempts[$groupIndex]->wait();
                            } else {
                                $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

                                $request = $this->buildPessimisticLockRequest(
                                    $region,
                                    $mutations,
                                    $primary,
                                    $forUpdateTs,
                                    $isFirstLock,
                                );

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
                            }

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
                            $lastRegionError = $regionError;
                            $needRetry = true;
                            $this->logger->warning('Region error during pessimistic lock, retrying', [
                                'regionId' => $region->regionId,
                                'attempt' => $attempt,
                                'error' => $regionError->getMessage(),
                            ]);
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
                                        // This per-region lock loop performs bounded retries;
                                        // NotLeader handling remains local. A live lock is not
                                        // resolved; retry the request within this loop.
                                        try {
                                            $this->lockResolver->resolveLock(
                                                $lockPrimary,
                                                $locked,
                                                notLeaderOwnedByRetryExecutor: false,
                                            );
                                        } catch (TxnRetryableException $e) {
                                            if ($e->backoffType !== BackoffType::TxnLock) {
                                                throw $e;
                                            }
                                        }
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

                        $hasRegionError = $lastRegionError instanceof \CrazyGoat\TiKV\Client\Exception\RegionException;
                        if (!$needRetry && !$hasRegionError) {
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

                        // Keep the original for_update_ts for this logical
                        // statement. Advancing it after waiting would let a
                        // stale read-modify-write value pass the later
                        // prewrite constraint check; the caller must retry the
                        // statement to obtain a new read timestamp.
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
                        // The regroup abandons the not-yet-awaited
                        // first-attempt futures of the later groups; cancel
                        // them before the re-dispatch below (same rationale
                        // as the catch above).
                        $this->cancelUnawaitedFirstAttempts($firstAttempts);
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

                    $state->markPessimisticLocksAcquired($regionKeys);
                    $isFirstLock = false;
                }
            } catch (\Throwable $e) {
                // A failed lock pass leaves the transaction without a
                // complete pessimistic lock set. Mark it unwritable even in
                // deferred compatibility mode; otherwise a later commit()
                // could see an empty pending list and proceed to prewrite
                // without retrying the failed keys.
                $state->markPessimisticWriteFailed();
                // A first-attempt throw (deadlock / conflict / retryable /
                // abort / region-error-after-budget / lock timeout) abandons
                // the not-yet-awaited fan-out futures of the remaining
                // groups. Cancel them so the orphan-lock window closes
                // immediately: cancelled-but-applied locks are still covered
                // by pessimisticRollbackAll() / lock TTL, but cancelling
                // shrinks the race (issue #291 review).
                $this->cancelUnawaitedFirstAttempts($firstAttempts);
                throw $e;
            }
        }
    }

    /**
     * Best-effort cancellation of the first-attempt fan-out futures that
     * were never awaited (issue #291 review). Completed futures are skipped;
     * cancellation of a pending future prevents the response from surfacing
     * unobserved after the caller has already moved on.
     *
     * @param array<int, GrpcFuture> $firstAttempts
     */
    private function cancelUnawaitedFirstAttempts(array $firstAttempts): void
    {
        foreach ($firstAttempts as $future) {
            if (!$future->isCompleted()) {
                $future->cancel();
            }
        }
    }

    /**
     * Whether the region range covers every key (endKey === '' means the
     * last region, which extends to +inf).
     *
     * Bytewise containment via KeyOrder: a loose `<`/`>=` compares numeric
     * keys numerically, so "199" looked like it was past the region end "20"
     * (199 >= 20) even though it sorts before it, and the group was reported
     * as no longer covered (issue #186).
     *
     * @param string[] $keys
     */
    private function regionCoversAllKeys(RegionInfo $region, array $keys): bool
    {
        foreach ($keys as $key) {
            if (!KeyOrder::inRange($key, $region->startKey, $region->endKey)) {
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
        $pessimisticKeys = array_values(array_unique(array_merge(
            $state->getPessimisticLockKeys(),
            $state->getWriteKeys(),
        )));
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

            // Fan out all region groups in parallel (issue #291) — same
            // pattern as batchRollback() above.
            $regionCalls = [];
            foreach ($keysByRegion as $regionData) {
                $regionKeys = $regionData['keys'];
                $firstKey = $regionKeys[0] ?? '';

                $regionCalls[] = fn(): CheckedGrpcFuture => $this->pessimisticRollbackWithRetry(
                    $firstKey,
                    $regionKeys,
                    $forUpdateTs,
                    $retryExecutor,
                    $classifier,
                );
            }

            $batchExecutor = new BatchAsyncExecutor($this->logger);
            try {
                $batchExecutor->executeParallel($regionCalls, $this->timeoutConfig->batchDeadlineMs);
                continue;
            } catch (BatchPartialFailureException $e) {
                // Preserve the sequential first-abort exception semantics.
                throw $e->getFirstRegionError();
            } catch (\RuntimeException $e) {
                // RollbackRegroupSignal deliberately extends \RuntimeException,
                // not TiKvException, so it escapes the retry closures and the
                // executor's TiKvException handling.
                if (!$e instanceof RollbackRegroupSignal) {
                    throw $e;
                }
                $signal = $e;
                // Same regroup-on-split recovery as batchRollback()
                // above (issue #505).
                $groupIndex = null;
                $regionKeys = [];
                foreach ($keysByRegion as $index => $regionData) {
                    $keys0 = $regionData['keys'][0] ?? '';
                    if ($keys0 !== '' && $keys0 === $signal->groupFirstKey) {
                        $groupIndex = $index;
                        $regionKeys = $regionData['keys'];
                        break;
                    }
                }
                if ($groupIndex === null) {
                    throw new RegionException(
                        'pessimistic rollback',
                        'Rollback re-group signal referenced an unknown key group',
                    );
                }
                $signalledRegion = $keysByRegion[$groupIndex]['region'];

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
                    'regionId' => $signalledRegion->regionId,
                    'keyCount' => count($pendingKeys),
                ]);
            }
        }
    }

    /**
     * Wrap one region group's PessimisticRollback in the retry executor for
     * the #291 fan-out — same shape as batchRollbackWithRetry().
     *
     * @param string[] $regionKeys
     */
    private function pessimisticRollbackWithRetry(
        string $firstKey,
        array $regionKeys,
        int $forUpdateTs,
        RetryExecutor $retryExecutor,
        callable $classifier,
    ): CheckedGrpcFuture {
        return CheckedGrpcFuture::fromRetryableDispatch(
            function () use ($firstKey, $regionKeys, $forUpdateTs): CheckedGrpcFuture {
                // Resolve the region on every attempt so cache
                // invalidation and leader switching performed by the
                // retry executor take effect (issue #502) — see
                // batchRollbackWithRetry() for the full stale-capture
                // rationale (#267/#500 pattern) and the #505
                // regroup-on-split rationale.
                $region = $this->regionResolver->getRegionInfo($firstKey);
                if (!$this->regionCoversAllKeys($region, $regionKeys)) {
                    throw new RollbackRegroupSignal(
                        'Re-resolved region no longer covers the rollback key group',
                        $firstKey,
                    );
                }
                $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

                $request = new PessimisticRollbackRequest();
                $request->setContext($this->buildContext($region));
                $request->setStartVersion($this->startTs);
                $request->setForUpdateTs($forUpdateTs);
                $request->setKeys($regionKeys);

                $this->logger->debug('PessimisticRollback', [
                    'regionId' => $region->regionId,
                ]);

                $future = $this->grpc->callAsync(
                    $address,
                    'tikvpb.Tikv',
                    'KVPessimisticRollback',
                    $request,
                    PessimisticRollbackResponse::class,
                    $this->timeoutMs('write'),
                );

                return CheckedGrpcFuture::fromCallable(function () use ($future, $region) {
                    /** @var PessimisticRollbackResponse $response */
                    $response = $future->wait();
                    RegionErrorHandler::check($response, $this->regionCache, $region->regionId);

                    $errors = $response->getErrors();
                    foreach ($errors as $keyError) {
                        $this->handleRollbackError($keyError);
                    }

                    return $response;
                }, $future);
            },
            $retryExecutor,
            $firstKey,
            $classifier,
        );
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
