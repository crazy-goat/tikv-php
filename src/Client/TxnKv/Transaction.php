<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\ScanLimitExceededException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Observability\NoOpTracer;
use CrazyGoat\TiKV\Client\Observability\SafeLogging;
use CrazyGoat\TiKV\Client\Observability\TraceEventInterface;
use CrazyGoat\TiKV\Client\Region\RegionRangeClipper;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionTooLargeException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\UndeterminedCommitException;
use CrazyGoat\TiKV\Client\Util\ShutdownState;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * A single transaction against TiKV.
 *
 * This is a thin façade that delegates to focused collaborators:
 *
 * - {@see TransactionState}   — encapsulates the mutable transaction state
 * - {@see TxnReader}          — handles get, batchGet, scan operations
 * - {@see TwoPhaseCommitter}  — handles commit, rollback, pessimistic locking
 *
 * The public API is unchanged from the original 944-line monolithic
 * Transaction class (refactored in issue #83).  All behaviour is
 * preserved; no breaking changes to callers.
 */
final class Transaction
{
    private readonly TransactionState $state;
    private readonly TxnReader $reader;
    private readonly TwoPhaseCommitter $committer;
    private ?RetryExecutor $retryExecutor = null;

    /** Wall-clock deadline (ms) applied to this transaction's RetryExecutor. */
    private readonly int $retryDeadlineMs;

    /**
     * Maximum number of rows a transactional scan may return.
     *
     * It bounds the memory one scan may buffer; a range that holds more rows
     * than this does not return a truncated result but throws
     * {@see ScanLimitExceededException} (issue #440). It is also the largest
     * `limit` a caller may pass.
     */
    public const MAX_SCAN_LIMIT = 10240;

    /**
     * ServerBusy backoff budget for the transaction retry loop, kept as an
     * explicit constant because Transaction has no client options array
     * (issue #294). Matches RawKvClient::DEFAULT_SERVER_BUSY_BUDGET_MS
     * (reduced from 600000 ms — see there for rationale). The wall-clock
     * bound on the loop is retryDeadlineMs.
     */
    private const DEFAULT_SERVER_BUSY_BUDGET_MS = 60000;

    /**
     * Default wall-clock deadline for the transaction retry loop (issue #294).
     * Bounds the blocking usleep() backoff so a sustained error episode cannot
     * pin a PHP-FPM worker for minutes inside one commit/scan. Alias of the
     * canonical RetryExecutor constant; do not introduce further copies.
     */
    public const DEFAULT_RETRY_DEADLINE_MS = RetryExecutor::DEFAULT_RETRY_DEADLINE_MS;

    /**
     * Default pessimistic-lock wait timeout (issue #443, DIV-07). Alias of
     * the canonical TwoPhaseCommitter constant; do not introduce further
     * copies.
     */
    public const DEFAULT_LOCK_WAIT_TIME_MS = TwoPhaseCommitter::DEFAULT_LOCK_WAIT_TIME_MS;

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
        private readonly TimeoutConfig $timeoutConfig = new TimeoutConfig(),
        private readonly MetricsInterface $metrics = new NoOpMetrics(),
        int $retryDeadlineMs = self::DEFAULT_RETRY_DEADLINE_MS,
        /** Read preference applied to this transaction's reads (issue #421). */
        private readonly ReplicaReadPolicy $replicaReadPolicy = new ReplicaReadPolicy(),
        /** Whether this transaction may use one-phase commit (issue #419). */
        private readonly bool $enable1Pc = false,
        /** Whether this transaction may use async commit (issue #419). */
        private readonly bool $enableAsyncCommit = false,
        /**
         * TSO reads used by pessimistic read-modify-write keys. Keyed by the
         * raw TiKV key, so it inherits PHP's array-key coercion exactly like
         * the write and read sets do (issue #261): `'1000'` is stored as int
         * 1000, and every `$this->readTsByKey[$key]` lookup is a string lookup
         * that PHP casts the same way, so the map is lossless.
         *
         * @var array<array-key, int>
         */
        private array $readTsByKey = [],
        /** Whether pessimistic set/delete acquire their lock before returning. */
        private readonly bool $eagerPessimisticLocks = true,
        /**
         * How long TiKV may wait for a contended pessimistic lock, in
         * milliseconds (issue #443, DIV-07). 0 means "use TiKV's own
         * default timeout".
         */
        private readonly int $lockWaitTimeMs = self::DEFAULT_LOCK_WAIT_TIME_MS,
        /**
         * Row cap for a transactional scan (issue #440). Defaults to
         * {@see self::MAX_SCAN_LIMIT}; a smaller value is a test seam, not a
         * production knob — a transaction has no client options array to
         * configure it through.
         */
        private readonly int $maxScanLimit = self::MAX_SCAN_LIMIT,
        /** Optimistic lock TTL baseline in milliseconds (issue #429). */
        private readonly int $optimisticLockTtlMs = TwoPhaseCommitter::DEFAULT_OPTIMISTIC_LOCK_TTL_MS,
        /** Pessimistic lock TTL in milliseconds (issue #429). */
        private readonly int $pessimisticLockTtlMs = TwoPhaseCommitter::DEFAULT_PESSIMISTIC_LOCK_TTL_MS,
        /**
         * Optional clock (milliseconds) for the lock TTL bookkeeping.
         * Injectable for deterministic tests (issue #429); defaults to the
         * wall clock. Shared with the committer.
         */
        private readonly ?\Closure $clock = null,
        /**
         * Local clock reading (ms) taken before the start timestamp was
         * fetched; null reads the clock now. Internal: begin() passes it so
         * the tracked lock expiry is never later than the real one (issue #429).
         */
        ?int $startedAtMs = null,
        /**
         * Maximum number of write-set entries; 0 = unlimited (issue #430).
         */
        private readonly int $maxTxnEntryCount = 0,
        /**
         * Maximum bytes of one write-set key+value entry; 0 = unlimited
         * (issue #430).
         */
        private readonly int $maxTxnEntrySize = 0,
        /**
         * Maximum bytes of the whole write set; 0 = unlimited (issue #430).
         */
        private readonly int $maxTxnTotalSize = 0,
        /**
         * Maximum bytes of one prewrite/commit request per region (issue
         * #430); defaults to client-go's `DefTxnCommitBatchSize`.
         */
        private readonly int $maxTxnBatchSize = TwoPhaseCommitter::DEFAULT_TXN_BATCH_SIZE,
        /**
         * Maximum mutations/keys of one prewrite/commit request per region
         * (issue #430); defaults to RawKV's `MAX_BATCH_LIMIT`.
         */
        private readonly int $maxTxnBatchCount = TwoPhaseCommitter::DEFAULT_TXN_BATCH_COUNT,
        /**
         * Maximum number of read-set entries; 0 = unlimited (issue #297).
         */
        private readonly int $maxReadSetEntries = 0,
        /**
         * Maximum bytes of the whole read set; 0 = unlimited (issue #297).
         */
        private readonly int $maxReadSetTotalSize = 0,
        /**
         * Whether get()/batchGet() retain their values in the read set
         * (issue #297). Off by default: nothing in the commit path consumes
         * it, so keeping it on the hot read path would grow without bound.
         */
        private readonly bool $trackReadSet = false,
        /**
         * Optional Context control fields applied to this transaction's
         * requests (issue #426). Null (the default) applies none and leaves
         * the serialized Context unchanged.
         */
        private readonly ?RequestContextOptions $requestContext = null,
        /**
         * Resource-control metadata carried on every request of this
         * transaction (issue #427): the client-wide default with the
         * begin() overrides applied. An empty instance leaves both
         * resource-control fields unset.
         */
        private readonly ResourceControl $resourceControl = new ResourceControl(),
        /**
         * Optional structured tracing callback (issue #434, GAP-20), handed
         * to the two-phase committer.
         */
        private readonly TraceEventInterface $tracer = new NoOpTracer(),
    ) {
        if ($retryDeadlineMs < 0) {
            throw new InvalidArgumentException('retryDeadlineMs must be >= 0');
        }
        // Arm the shutdown flag before the destructor can consult it; the
        // registered callback runs before PHP destroys the remaining
        // objects (issue #287, GRPC-28).
        ShutdownState::ensureRegistered();
        if ($this->maxScanLimit < 1) {
            throw new InvalidArgumentException('maxScanLimit must be >= 1');
        }
        foreach (
            [
                'maxTxnEntryCount' => $this->maxTxnEntryCount,
                'maxTxnEntrySize' => $this->maxTxnEntrySize,
                'maxTxnTotalSize' => $this->maxTxnTotalSize,
                'maxReadSetEntries' => $this->maxReadSetEntries,
                'maxReadSetTotalSize' => $this->maxReadSetTotalSize,
            ] as $name => $limit
        ) {
            if ($limit < 0) {
                throw new InvalidArgumentException($name . ' must be >= 0');
            }
        }
        foreach (
            [
                'maxTxnBatchSize' => $this->maxTxnBatchSize,
                'maxTxnBatchCount' => $this->maxTxnBatchCount,
            ] as $name => $limit
        ) {
            if ($limit < 1) {
                throw new InvalidArgumentException($name . ' must be >= 1');
            }
        }
        $this->retryDeadlineMs = $retryDeadlineMs;
        if ($lockWaitTimeMs < 0) {
            // A negative wait_timeout means "do not wait at all" in TiKV, so
            // it would silently change the meaning of the option (issue #443).
            throw new InvalidArgumentException('lockWaitTimeMs must be >= 0');
        }
        $lockTtlsMs = [
            'optimisticLockTtlMs' => $optimisticLockTtlMs,
            'pessimisticLockTtlMs' => $pessimisticLockTtlMs,
        ];
        foreach ($lockTtlsMs as $name => $ttlMs) {
            if ($ttlMs < 1 || $ttlMs > TwoPhaseCommitter::MAX_LOCK_TTL_MS) {
                throw new InvalidArgumentException(sprintf(
                    '%s must be between 1 and %d',
                    $name,
                    TwoPhaseCommitter::MAX_LOCK_TTL_MS,
                ));
            }
        }
        $this->state = new TransactionState(
            startedAtMs: $startedAtMs ?? $this->nowMs(),
            maxTxnEntryCount: $this->maxTxnEntryCount,
            maxTxnEntrySize: $this->maxTxnEntrySize,
            maxTxnTotalSize: $this->maxTxnTotalSize,
            maxReadSetEntries: $this->maxReadSetEntries,
            maxReadSetTotalSize: $this->maxReadSetTotalSize,
            trackReadSet: $this->trackReadSet,
        );

        $this->reader = new TxnReader(
            startTs: $this->startTs,
            grpc: $this->grpc,
            pdClient: $this->pdClient,
            regionResolver: $this->regionResolver,
            timeoutConfig: $this->timeoutConfig,
            lockResolver: $this->lockResolver,
            regionCache: $this->regionCache,
            replicaReadPolicy: $this->replicaReadPolicy,
            logger: $this->logger,
            priority: $this->priority,
            metrics: $this->metrics,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
        );

        $this->committer = new TwoPhaseCommitter(
            startTs: $this->startTs,
            pessimistic: $this->pessimistic,
            priority: $this->priority,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            regionResolver: $this->regionResolver,
            lockResolver: $this->lockResolver,
            timeoutConfig: $this->timeoutConfig,
            maxBackoffMs: $this->maxBackoffMs,
            enable1Pc: $this->enable1Pc,
            enableAsyncCommit: $this->enableAsyncCommit,
            logger: $this->logger,
            clock: $this->clock,
            lockWaitTimeMs: $this->lockWaitTimeMs,
            optimisticLockTtlMs: $this->optimisticLockTtlMs,
            pessimisticLockTtlMs: $this->pessimisticLockTtlMs,
            maxTxnBatchSize: $this->maxTxnBatchSize,
            maxTxnBatchCount: $this->maxTxnBatchCount,
            requestContext: $this->requestContext,
            resourceControl: $this->resourceControl,
            metrics: $this->metrics,
            tracer: $this->tracer,
        );
    }

    // ---------------------------------------------------------------
    //  Public getters (unchanged API)
    // ---------------------------------------------------------------

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
        return $this->state->getCommitTs();
    }

    public function getStatus(): TransactionStatus
    {
        return $this->state->getStatus();
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
     * The returned map inherits PHP's array-key semantics: a canonical
     * decimal-integer key is returned under its `int` form, every other key
     * form stays a `string` key — including a canonical decimal that overflows
     * a PHP int (`'9223372036854775808'`). `$writeSet['1000']` still finds the
     * entry stored as int 1000, because PHP casts a lookup the same way. Hence
     * `array-key`, not `string` (issue #261; see
     * {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::batchGet()} for the full
     * rule).
     *
     * @return array<array-key, ?string>
     */
    public function getWriteSet(): array
    {
        return $this->state->getWriteSet();
    }

    /**
     * The read set has the same shape as the write set and the same coercion
     * (issue #261; see {@see getWriteSet()}).
     *
     * Empty unless the transaction was created with `trackReadSet: true`;
     * only `get()` and `batchGet()` populate it — `scan()` does not.
     *
     * @return array<array-key, ?string>
     */
    public function getReadSet(): array
    {
        return $this->state->getReadSet();
    }

    /**
     * Bytes currently held by the write set (issue #430).
     *
     * Every entry contributes its key length plus its value length; a delete
     * contributes the key length only. The value is maintained incrementally,
     * so it can be logged or exported as a metric on every write without
     * re-walking the write set.
     */
    public function getWriteSetSize(): int
    {
        return $this->state->getWriteSetSize();
    }

    /**
     * Number of entries currently held by the write set (issue #430).
     */
    public function getWriteSetCount(): int
    {
        return $this->state->getWriteSetCount();
    }

    /**
     * Bytes currently held by the read set (issue #297).
     *
     * Every entry contributes its key length plus its value length; a
     * not-found read contributes the key length only. The value is maintained
     * incrementally, so it can be logged or exported as a metric on every
     * read without re-walking the read set.
     */
    public function getReadSetSize(): int
    {
        return $this->state->getReadSetSize();
    }

    /**
     * Number of entries currently held by the read set (issue #297).
     */
    public function getReadSetCount(): int
    {
        return $this->state->getReadSetCount();
    }

    /**
     * Drop every tracked read and reset the read-set accounting (issue #297).
     *
     * Useful in a long-lived transaction built with `trackReadSet: true`:
     * the transaction does not need its read set at commit, so a worker can
     * release it after a batch of reads. A no-op when tracking is disabled.
     */
    public function clearReadSet(): void
    {
        $this->state->clearReadSet();
    }

    // ---------------------------------------------------------------
    //  Read operations (delegated to TxnReader)
    // ---------------------------------------------------------------

    /**
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     */
    public function get(string $key): ?string
    {
        $this->state->ensureWritable();
        $this->keepPrimaryLockAlive();

        if ($this->pessimistic && !$this->state->hasWriteSetKey($key)) {
            $this->readTsByKey[$key] = $this->pdClient->getTimestamp();
        }

        return $this->reader->get(
            $key,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
        );
    }

    /**
     * The returned map inherits PHP's array-key semantics: a canonical
     * decimal-integer key is returned under its `int` form, every other key
     * form stays a `string` key — including a canonical decimal that overflows
     * a PHP int (`'9223372036854775808'`), which no int key can denote.
     * `$results['1000']` still finds the entry stored as int 1000, because PHP
     * casts a lookup the same way. Hence `array-key`, not `string` (issue #261;
     * see {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::batchGet()} for
     * the full rule).
     *
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     * @return array<array-key, ?string>
     *
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     */
    public function batchGet(array $keys): array
    {
        $this->state->ensureWritable();
        $this->keepPrimaryLockAlive();

        if ($this->pessimistic && $keys !== []) {
            $timestamp = $this->pdClient->getTimestamp();
            foreach ($keys as $key) {
                $this->readTsByKey[(string) $key] = $timestamp;
            }
        }

        return $this->reader->batchGet(
            $keys,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
        );
    }

    /**
     * Range scan over [startKey, endKey) at the transaction's snapshot.
     *
     * `limit = 0` (the default) means **the whole range**: the scan reads
     * the range up to {@see self::MAX_SCAN_LIMIT} rows (10240). A range that
     * holds more rows than that does **not** return a truncated result — the
     * scan detects the missing rows and throws {@see ScanLimitExceededException}
     * instead (issue #440). Pass an explicit `limit` to read a known number
     * of rows: the result then holds at most that many rows, and the caller
     * detects a possibly-truncated result by comparing the returned count
     * with the limit.
     *
     * A transactional scan has no iterator of its own; the unbounded
     * alternative is {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::scanIterator()}
     * (and {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::scanPrefixIterator()}
     * for a prefix), which pages the range with constant memory — but it is a
     * raw, non-snapshot read, so it does not observe the transaction's own
     * buffered writes.
     *
     * @param int $limit Maximum rows to return; 0 = the whole range (capped
     *                   at {@see self::MAX_SCAN_LIMIT} rows, a larger range
     *                   throws instead of truncating). A negative value
     *                   throws InvalidArgumentException before the scan RPC
     *                   (a pessimistic transaction refreshes its read
     *                   timestamp first)
     * @param bool $reverse Read the range in descending key order. The bounds
     *                      then keep the {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::reverseScan()}
     *                      meaning: `$startKey` is the exclusive upper bound
     *                      and `$endKey` the lower bound, so the scan reads
     *                      `[endKey, startKey)` (issue #435, GAP-21). An empty
     *                      `$startKey` with a non-empty `$endKey` is rejected,
     *                      and `$startKey === $endKey` returns `[]`. The
     *                      convenience wrapper is {@see self::reverseScan()}.
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     * @throws ScanLimitExceededException When limit is 0 and the range holds
     *         more than {@see self::MAX_SCAN_LIMIT} rows
     */
    public function scan(string $startKey, string $endKey, int $limit = 0, bool $reverse = false): array
    {
        $this->state->ensureWritable();
        $this->keepPrimaryLockAlive();

        if ($this->pessimistic) {
            $timestamp = $this->pdClient->getTimestamp();
            foreach ($this->state->getWriteKeys() as $key) {
                $this->readTsByKey[$key] ??= $timestamp;
            }
        }

        return $this->reader->scan(
            $startKey,
            $endKey,
            $limit,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
            $this->maxScanLimit,
            $reverse,
        );
    }

    /**
     * Reverse range scan over `[endKey, startKey)`, in descending key order.
     *
     * This is the transactional counterpart of
     * {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::reverseScan()}: the first
     * argument is the **exclusive upper bound**, the second the lower bound.
     * It reads at the transaction's snapshot and observes the transaction's
     * own buffered writes, so a key set or deleted earlier in the transaction
     * appears (or is hidden) in its sorted position.
     *
     * `limit = 0` means the whole range, capped at
     * {@see self::MAX_SCAN_LIMIT} rows exactly like {@see self::scan()}; a
     * larger range throws {@see ScanLimitExceededException} instead of
     * returning a truncated result. Regions are walked from the upper bound
     * downwards, so a narrow reverse scan never enumerates the rest of the
     * range (issue #435, GAP-21).
     *
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException When the range is reversed (an empty
     *         upper bound with a non-empty lower one, or `$startKey` sorts
     *         before `$endKey`)
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     * @throws ScanLimitExceededException When limit is 0 and the range holds
     *         more than {@see self::MAX_SCAN_LIMIT} rows
     */
    public function reverseScan(string $startKey, string $endKey, int $limit = 0): array
    {
        return $this->scan($startKey, $endKey, $limit, true);
    }

    /**
     * Delete every key in the forward range `[startKey, endKey)` inside this
     * transaction (issue #435, GAP-21).
     *
     * The keys are read at the transaction's snapshot and buffered as
     * deletes, so the deletion is **atomic with the transaction**: it becomes
     * visible only at `commit()` and is undone by `rollback()`. Keys the
     * transaction already wrote are honoured (a buffered `set()` is deleted,
     * an earlier `delete()` is not sent twice).
     *
     * The range is read with {@see self::scan()} and therefore obeys the same
     * cap: a range holding more than {@see self::MAX_SCAN_LIMIT} rows throws
     * {@see ScanLimitExceededException} instead of silently deleting a part.
     * The buffered deletes also count against the transaction's write-set
     * limits (`maxTxnEntryCount`, `maxTxnTotalSize`, …). The whole batch is
     * validated against the projected write set before any pessimistic lock
     * is acquired, so an oversized range fails without locking or buffering
     * anything. The whole range is one transaction, so this method is meant
     * for bounded ranges. For a range of unbounded size use
     * `RawKvClient::deleteRange()` (a raw, non-transactional range delete) or
     * delete in explicit batches across several transactions.
     *
     * @param string $startKey Inclusive lower bound
     * @param string $endKey   Exclusive upper bound; `''` means the end of the
     *                         keyspace. `startKey === endKey` deletes nothing
     * @return int Number of keys marked deleted
     *
     * @throws InvalidArgumentException When the range is reversed
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     * @throws ScanLimitExceededException When the range holds more than
     *         {@see self::MAX_SCAN_LIMIT} rows
     * @throws TransactionTooLargeException When the deletes exceed a
     *         configured transaction size limit
     */
    public function deleteRange(string $startKey, string $endKey): int
    {
        $this->state->ensureWritable();
        RegionRangeClipper::assertForwardRangeOrder($startKey, $endKey, 'deleteRange');

        if ($startKey === $endKey) {
            return 0;
        }

        $rows = $this->scan($startKey, $endKey, 0);
        $keys = [];
        foreach ($rows as $row) {
            $keys[] = $row['key'];
        }
        if ($keys === []) {
            return 0;
        }

        // Validate the whole batch against the *projected* write set before
        // any lock or buffer mutation: a per-key assertWriteAllowed() would
        // each see the unchanged set and miss a cumulative count/size
        // overflow (issue #435).
        $this->state->assertDeletesFit($keys);

        if ($this->pessimistic && $this->eagerPessimisticLocks) {
            // Take every physical lock in one batched lockKeys() call (it
            // groups the keys by region) instead of one RPC per key, then
            // buffer the tombstones without re-locking.
            $this->lockKeys(...$keys);
            foreach ($keys as $key) {
                $this->state->setWrite($key, null);
            }

            return count($keys);
        }

        foreach ($keys as $key) {
            $this->delete($key);
        }

        return count($keys);
    }

    // ---------------------------------------------------------------
    //  Write operations
    // ---------------------------------------------------------------

    /**
     * Prepare a pessimistic write. The value remains buffered, but in the
     * default eager mode the physical lock is acquired before this method
     * returns. Deferred mode is retained as an explicit compatibility path.
     */
    private function preparePessimisticWrite(string $key): void
    {
        if (!$this->pessimistic) {
            return;
        }

        if ($this->eagerPessimisticLocks && $this->state->hasPessimisticLock($key)) {
            unset($this->readTsByKey[$key]);
            return;
        }

        $forUpdateTs = $this->readTsByKey[$key] ?? $this->pdClient->getTimestamp();
        if ($this->eagerPessimisticLocks) {
            // The TTL the lock request carries, measured before the RPC. The
            // start time was read before the start timestamp was fetched, and
            // TiKV counts the TTL from the start timestamp, so the tracked
            // expiry below is never later than the real one.
            $lockTtlMs = $this->committer->ttlWithUptimeMs($this->state, $this->pessimisticLockTtlMs);
            $this->state->setPessimisticPrimaryKey($key);
            try {
                $this->committer->lockKeys(
                    [$key],
                    $this->state,
                    $forUpdateTs,
                    $key,
                    retryExecutor: $this->retryExecutor()->beginOperation(),
                );
            } catch (\Throwable $failure) {
                $this->failPessimisticStatement($failure);
            }
            if ($this->state->getPrimaryLockExpiresAtMs() === null) {
                $this->state->setPrimaryLockExpiresAtMs($this->state->getStartedAtMs() + $lockTtlMs);
            }
        } else {
            $this->state->addPendingLockKey($key);
            $this->state->updateMaxForUpdateTs($forUpdateTs);
        }

        unset($this->readTsByKey[$key]);
    }

    /**
     * A failed eager pessimistic statement makes the transaction unusable;
     * otherwise a later write could inherit a primary key that was never
     * locked. Best-effort rollback also releases a lock whose response may
     * have been lost in transport (issue #436).
     */
    private function failPessimisticStatement(\Throwable $failure): never
    {
        $this->state->markPessimisticWriteFailed();
        try {
            $this->committer->rollback(
                $this->state,
                $this->retryExecutor(),
                $this->classifyError(...),
            );
        } catch (\Throwable $cleanupFailure) {
            // The rollback attempt made the transaction terminal even though
            // its RPC failed (issue #231): the remaining locks expire with
            // their TTL, and neither the caller nor the destructor retries.
            $this->logger->error('Pessimistic lock failure cleanup failed', [
                'txnId' => $this->txnId,
                'exception' => $cleanupFailure,
            ]);
        }

        throw $failure;
    }

    private function assertPessimistic(string $method): void
    {
        if (!$this->pessimistic) {
            throw new InvalidStateException(sprintf(
                '%s() is only available in a pessimistic transaction',
                $method,
            ));
        }
    }

    /**
     * @throws InvalidStateException
     * @throws TransactionTooLargeException when the write would exceed a
     *         configured transaction size limit (issue #430); thrown before
     *         the eager pessimistic lock RPC, so nothing reaches TiKV
     * @throws TiKvException if the pessimistic transaction cannot obtain a
     *         write timestamp or acquire its lock
     */
    public function set(string $key, string $value): void
    {
        $this->state->ensureWritable();
        // Reject an oversized write before the eager pessimistic lock below
        // sends any RPC (issue #430).
        $this->state->assertWriteAllowed($key, $value);
        $this->keepPrimaryLockAlive();
        $this->preparePessimisticWrite($key);
        $this->state->setWrite($key, $value);
    }

    /**
     * @throws InvalidStateException
     * @throws TransactionTooLargeException when the write would exceed a
     *         configured transaction size limit (issue #430); thrown before
     *         the eager pessimistic lock RPC, so nothing reaches TiKV
     * @throws TiKvException if the pessimistic transaction cannot obtain a
     *         write timestamp or acquire its lock
     */
    public function delete(string $key): void
    {
        $this->state->ensureWritable();
        // Reject an oversized write before the eager pessimistic lock below
        // sends any RPC (issue #430).
        $this->state->assertWriteAllowed($key, null);
        $this->keepPrimaryLockAlive();
        $this->preparePessimisticWrite($key);
        $this->state->setWrite($key, null);
    }

    /**
     * Acquire pessimistic locks on the given keys without writing values
     * (issue #436, GAP-22).
     *
     * This is the explicit read-for-update primitive: a key locked here stays
     * locked until the transaction commits or rolls back, so a concurrent
     * writer blocks (or fails) at its own lock attempt instead of at commit.
     * The call sends one `KvPessimisticLock` request per region batch and
     * returns once every lock is acknowledged.
     *
     * A key already locked by this transaction is skipped, so calling this
     * method twice is safe. The lock is acquired immediately even when the
     * transaction was begun with `eagerPessimisticLocks: false`: that option
     * defers the implicit locks of `set()` / `delete()`, not an explicit
     * request.
     *
     * A locked key that is never written is still committed: the committer
     * emits it as an `Op::Lock` mutation, so `commit()` releases the lock and
     * a transaction that only locks keys ends `Committed`, not `RolledBack`.
     *
     * @throws InvalidStateException when the transaction is optimistic, not
     *         active, or already unusable after a failed lock statement
     * @throws TransactionConflictException
     * @throws DeadlockException
     * @throws LockWaitTimeoutException
     * @throws TiKvException
     * @throws RegionException
     * @throws GrpcException
     */
    public function lockKeys(string ...$keys): void
    {
        $this->state->ensureWritable();
        $this->assertPessimistic('lockKeys');
        $this->keepPrimaryLockAlive();

        $keys = array_values(array_filter(
            array_values(array_unique($keys)),
            fn (string $key): bool => !$this->state->hasPessimisticLock($key),
        ));
        if ($keys === []) {
            return;
        }

        $primary = $this->state->getPessimisticPrimaryKey() ?? $keys[0];
        $this->state->setPessimisticPrimaryKey($primary);
        // Measure the TTL before the RPC, exactly like the implicit eager
        // lock: the tracked expiry must never be later than the real one.
        $lockTtlMs = $this->committer->ttlWithUptimeMs($this->state, $this->pessimisticLockTtlMs);
        try {
            $this->committer->lockKeys(
                $keys,
                $this->state,
                $this->pdClient->getTimestamp(),
                $primary,
                retryExecutor: $this->retryExecutor()->beginOperation(),
            );
        } catch (\Throwable $failure) {
            $this->failPessimisticStatement($failure);
        }
        if ($this->state->getPrimaryLockExpiresAtMs() === null) {
            $this->state->setPrimaryLockExpiresAtMs($this->state->getStartedAtMs() + $lockTtlMs);
        }
    }

    /**
     * Acquire a pessimistic lock on one key and return its current value
     * (issue #436, GAP-22).
     *
     * This is the `SELECT ... FOR UPDATE` primitive: the lock and the read
     * happen in one `KvPessimisticLock` round trip, and the value is the
     * latest committed one at the request's `for_update_ts`, not the
     * transaction snapshot's. A missing key returns `null`; a key with a
     * buffered `set()` / `delete()` returns the buffered value
     * (read-your-writes).
     *
     * The value comes from the response's legacy `values` / `not_founds`
     * fields or, on a server that uses the newer wake-up mode, its per-key
     * `results` field; only this method asks for values (`return_values`).
     * A server that populates neither makes this method fall back to a
     * snapshot `get()`, which costs a second round trip and returns the
     * `startTs` value.
     *
     * @throws InvalidStateException when the transaction is optimistic, not
     *         active, or already unusable after a failed lock statement
     * @throws TransactionConflictException
     * @throws DeadlockException
     * @throws LockWaitTimeoutException
     * @throws TiKvException
     * @throws RegionException
     * @throws GrpcException
     */
    public function getForUpdate(string $key): ?string
    {
        $this->state->ensureWritable();
        $this->assertPessimistic('getForUpdate');
        $this->keepPrimaryLockAlive();

        // The key already holds a pessimistic lock and its value is buffered:
        // the RPC would only re-lock a key this transaction owns and the
        // buffered value wins anyway (read-your-writes), so skip it.
        if ($this->state->hasPessimisticLock($key) && $this->state->hasWriteSetKey($key)) {
            return $this->state->getWriteSetValue($key);
        }

        $primary = $this->state->getPessimisticPrimaryKey() ?? $key;
        $this->state->setPessimisticPrimaryKey($primary);
        $lockTtlMs = $this->committer->ttlWithUptimeMs($this->state, $this->pessimisticLockTtlMs);
        try {
            $values = $this->committer->lockKeys(
                [$key],
                $this->state,
                $this->pdClient->getTimestamp(),
                $primary,
                true,
                $this->retryExecutor()->beginOperation(),
            );
        } catch (\Throwable $failure) {
            $this->failPessimisticStatement($failure);
        }
        if ($this->state->getPrimaryLockExpiresAtMs() === null) {
            $this->state->setPrimaryLockExpiresAtMs($this->state->getStartedAtMs() + $lockTtlMs);
        }

        if ($this->state->hasWriteSetKey($key)) {
            return $this->state->getWriteSetValue($key);
        }
        if (array_key_exists($key, $values)) {
            return $values[$key];
        }

        return $this->reader->get(
            $key,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
        );
    }

    // ---------------------------------------------------------------
    //  Commit / Rollback (delegated to TwoPhaseCommitter)
    // ---------------------------------------------------------------

    /**
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws DeadlockException
     * @throws LockWaitTimeoutException
     * @throws UndeterminedCommitException
     * @throws RegionException
     * @throws GrpcException
     */
    public function commit(): void
    {
        $this->state->ensureCommitAllowed();
        $this->keepPrimaryLockAlive();
        $this->state->markCommitStarted();

        if ($this->state->isEmptyWriteSet() && !$this->state->hasAcknowledgedPessimisticLocks()) {
            if ($this->state->hasPessimisticLockActivity()) {
                // A failed eager set/delete may have acquired a server-side
                // lock before its response was lost. Do not report an empty
                // commit as success while such a lock is outstanding.
                $this->committer->rollback(
                    $this->state,
                    $this->retryExecutor(),
                    $this->classifyError(...),
                );
                return;
            }

            $this->state->setStatus(TransactionStatus::Committed);
            $this->state->close();
            return;
        }

        // A lock-only transaction commits too: the committer turns every
        // acknowledged pessimistic lock that was never written into an
        // `Op::Lock` mutation, so commit releases the locks instead of
        // rolling the transaction back (issue #436).
        $this->committer->commit(
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
        );
        $this->state->setStatus(TransactionStatus::Committed);
        $this->state->close();
    }

    /**
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws RegionException
     * @throws GrpcException
     */
    public function rollback(): void
    {
        $this->state->ensureActive();
        $this->state->markRollbackStarted();

        if ($this->state->isEmptyWriteSet() && !$this->state->hasPessimisticLockActivity()) {
            $this->state->setStatus(TransactionStatus::RolledBack);
            $this->state->close();
            return;
        }

        $this->committer->rollback(
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
        );
    }

    // ---------------------------------------------------------------
    //  Heartbeat (delegated to TwoPhaseCommitter)
    // ---------------------------------------------------------------

    /**
     * Send a heartbeat for this transaction's primary lock to extend its TTL.
     *
     * A transaction left open between operations keeps its locks only for the
     * granted TTL (the optimistic prewrite baseline is 3000 ms plus 10 ms per
     * write-set mutation, capped at 120000 ms; the pessimistic TTL is 30000
     * ms), so a caller that waits (for example on an external call) without
     * touching TiKV for longer than half the TTL must call this method,
     * otherwise concurrent readers may roll the locks back.
     *
     * A pessimistic transaction also heartbeats on its own inside get(),
     * batchGet(), scan(), set(), delete() and commit() once half of the lock
     * TTL has elapsed (issue #429), and the committer heartbeats between
     * region prewrites (issue #218, TXN-13).
     *
     * TiKV counts the lock TTL from the transaction's start timestamp, so an
     * explicit `$adviseLockTtlMs` is a total TTL, not an extension: a value at
     * or below the current TTL changes nothing. With the default (null) the
     * client asks for `min(uptime + TTL, 1 hour)`, where TTL is the TTL of the
     * transaction mode, which always extends the lock by one full TTL.
     *
     * @param ?int $adviseLockTtlMs Requested total lock TTL in milliseconds;
     *                              null = uptime plus the mode's TTL, capped
     *                              at 3600000
     * @return int The actual lock TTL granted by TiKV
     *
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws RegionException
     * @throws GrpcException
     */
    public function heartbeat(?int $adviseLockTtlMs = null): int
    {
        $this->state->ensureWritable();

        $primary = $this->state->getPessimisticPrimaryKey() ?? $this->state->getPrimaryKey();
        $adviseLockTtlMs ??= $this->committer->ttlWithUptimeMs(
            $this->state,
            $this->pessimistic ? $this->pessimisticLockTtlMs : $this->optimisticLockTtlMs,
        );

        $grantedMs = $this->committer->heartbeat(
            $primary,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
            $adviseLockTtlMs,
        );
        if ($this->state->getPrimaryLockExpiresAtMs() !== null && $grantedMs > 0) {
            $this->state->setPrimaryLockExpiresAtMs($this->state->getStartedAtMs() + $grantedMs);
        }

        return $grantedMs;
    }

    /**
     * Opportunistic keepalive (issue #429): called by every operation that
     * talks to TiKV. It sends a heartbeat for the primary pessimistic lock
     * when fewer than half of its TTL remain, and does nothing for a
     * transaction without a primary lock (optimistic, or no lock taken yet).
     * See {@see TwoPhaseCommitter::heartbeatIfDue()}.
     */
    private function keepPrimaryLockAlive(): void
    {
        $this->committer->heartbeatIfDue(
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
            $this->nowMs(),
        );
    }

    private function nowMs(): int
    {
        if ($this->clock instanceof \Closure) {
            return ($this->clock)();
        }

        return (int) (microtime(true) * 1000);
    }

    // ---------------------------------------------------------------
    //  Internal
    // ---------------------------------------------------------------

    /**
     * Roll back a transaction the caller abandoned.
     *
     * Best-effort by design: a rollback is a network round trip and this
     * method runs from a destructor, where an exception must never escape.
     * During PHP's shutdown sequence the rollback is skipped entirely —
     * ext-grpc may already be in or past module shutdown, where the
     * behaviour of a call is undefined rather than merely erroneous (issue
     * #287, GRPC-28). The transaction then keeps its locks until the lock
     * TTL expires; callers that need an immediate release must call
     * {@see self::commit()} or {@see self::rollback()} explicitly.
     */
    public function __destruct()
    {
        if ($this->state->getStatus() !== TransactionStatus::Active || $this->state->isClosed()) {
            return;
        }

        if (ShutdownState::isShuttingDown()) {
            SafeLogging::log($this->logger, 'warning', 'Transaction rollback skipped during shutdown', [
                'txnId' => $this->txnId,
            ]);

            return;
        }

        try {
            $this->rollback();
        } catch (\Throwable $e) {
            // The logger may itself be unusable during shutdown: the guard
            // is what keeps the destructor from throwing (issue #287,
            // GRPC-28).
            SafeLogging::log($this->logger, 'error', 'Transaction destruct rollback failed', [
                'txnId' => $this->txnId,
                'exception' => $e,
            ]);
        }
    }

    private function retryExecutor(): RetryExecutor
    {
        $this->retryExecutor ??= new RetryExecutor(
            $this->maxBackoffMs,
            self::DEFAULT_SERVER_BUSY_BUDGET_MS,
            $this->regionCache,
            $this->grpc,
            $this->regionResolver,
            $this->logger,
            deadlineMs: $this->retryDeadlineMs,
            metrics: $this->metrics,
        );

        return $this->retryExecutor;
    }

    private function classifyError(TiKvException $e): RetryVerdict
    {
        if ($e instanceof TxnRetryableException) {
            return RetryVerdict::retry($e->backoffType);
        }

        // No opinion: let the built-in classifier decide (issue #534).
        return RetryVerdict::pass();
    }
}
