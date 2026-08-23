<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\Exception\DeadlockException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\LockWaitTimeoutException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
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

    /** Maximum scan limit to prevent unbounded memory usage */
    private const MAX_SCAN_LIMIT = 10240;

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
     * pin a PHP-FPM worker for minutes inside one commit/scan; matches the
     * RawKvClient default (RawKvClient::DEFAULT_RETRY_DEADLINE_MS).
     */
    public const DEFAULT_RETRY_DEADLINE_MS = 30000;

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
    ) {
        if ($retryDeadlineMs < 0) {
            throw new \InvalidArgumentException('retryDeadlineMs must be >= 0');
        }
        $this->retryDeadlineMs = $retryDeadlineMs;
        $this->state = new TransactionState();

        $this->reader = new TxnReader(
            startTs: $this->startTs,
            grpc: $this->grpc,
            pdClient: $this->pdClient,
            regionResolver: $this->regionResolver,
            timeoutConfig: $this->timeoutConfig,
            lockResolver: $this->lockResolver,
            regionCache: $this->regionCache,
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
            logger: $this->logger,
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
     * @return array<string, ?string>
     */
    public function getWriteSet(): array
    {
        return $this->state->getWriteSet();
    }

    /**
     * @return array<string, ?string>
     */
    public function getReadSet(): array
    {
        return $this->state->getReadSet();
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
        $this->state->ensureActive();

        return $this->reader->get(
            $key,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
        );
    }

    /**
     * @param array<array-key, string|int> $keys Keys may be ints when built via
     *                                           array_keys() on a map with
     *                                           numeric-string keys (issue #322)
     * @return array<string, ?string>
     *
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     */
    public function batchGet(array $keys): array
    {
        $this->state->ensureActive();

        return $this->reader->batchGet(
            $keys,
            $this->state,
        );
    }

    /**
     * @return array<array{key: string, value: ?string}>
     *
     * @throws InvalidArgumentException
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws TransactionConflictException
     * @throws RegionException
     * @throws GrpcException
     */
    public function scan(string $startKey, string $endKey, int $limit = 0): array
    {
        $this->state->ensureActive();

        return $this->reader->scan(
            $startKey,
            $endKey,
            $limit,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
            self::MAX_SCAN_LIMIT,
        );
    }

    // ---------------------------------------------------------------
    //  Write operations
    // ---------------------------------------------------------------

    /**
     * @throws InvalidStateException
     */
    public function set(string $key, string $value): void
    {
        $this->state->ensureActive();

        if ($this->pessimistic) {
            $this->state->addPendingLockKey($key);
        }

        $this->state->setWrite($key, $value);
    }

    /**
     * @throws InvalidStateException
     */
    public function delete(string $key): void
    {
        $this->state->ensureActive();

        if ($this->pessimistic) {
            $this->state->addPendingLockKey($key);
        }

        $this->state->setWrite($key, null);
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
     * @throws RegionException
     * @throws GrpcException
     */
    public function commit(): void
    {
        $this->state->ensureActive();

        if ($this->state->isEmptyWriteSet()) {
            $this->state->setStatus(TransactionStatus::Committed);
            $this->state->close();
            return;
        }

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

        if ($this->state->isEmptyWriteSet()) {
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
     * @throws InvalidStateException
     * @throws TiKvException
     * @throws RegionException
     * @throws GrpcException
     */
    public function heartbeat(int $adviseLockTtlMs = 10000): int
    {
        $this->state->ensureActive();

        $primary = $this->state->getPrimaryKey();

        return $this->committer->heartbeat(
            $primary,
            $this->state,
            $this->retryExecutor(),
            $this->classifyError(...),
            $adviseLockTtlMs,
        );
    }

    // ---------------------------------------------------------------
    //  Internal
    // ---------------------------------------------------------------

    public function __destruct()
    {
        if ($this->state->getStatus() === TransactionStatus::Active && !$this->state->isClosed()) {
            try {
                $this->rollback();
            } catch (\Throwable $e) {
                $this->logger->error('Transaction destruct rollback failed', [
                    'txnId' => $this->txnId,
                    'exception' => $e,
                ]);
            }
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

    private function classifyError(TiKvException $e): ?BackoffType
    {
        if ($e instanceof TxnRetryableException) {
            return $e->backoffType;
        }

        return null;
    }
}
