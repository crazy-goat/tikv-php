<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\TiKV\Client\Exception\InvalidStateException;

/**
 * Mutable state holder for a single transaction.
 *
 * Encapsulates the read set, write set, status flags and metadata that
 * {@see Transaction} and its collaborators need to share.  Extracted from
 * the 944-line Transaction god object as part of the SRP decomposition
 * (issue #83).
 */
final class TransactionState
{
    /** @var array<string, ?string> key => value. null value means delete */
    private array $writeSet = [];

    /** @var array<string, ?string> key => value read (for read-set tracking) */
    private array $readSet = [];

    /** @var string[] keys pending a deferred pessimistic lock pass */
    private array $pendingLockKeys = [];

    /** @var string[] keys for which a pessimistic lock request was attempted */
    private array $pessimisticLockAttempts = [];

    /** @var string[] keys whose pessimistic lock was acknowledged by TiKV */
    private array $pessimisticLocks = [];

    private ?string $pessimisticPrimaryKey = null;
    private bool $pessimisticWriteFailed = false;
    private bool $commitStarted = false;
    private bool $rollbackStarted = false;

    private ?int $commitTs = null;
    private TransactionStatus $status = TransactionStatus::Active;
    private bool $closed = false;
    private ?int $maxForUpdateTs = null;

    public function isActive(): bool
    {
        return $this->status === TransactionStatus::Active && !$this->closed;
    }

    /**
     * @throws InvalidStateException if the transaction is not active
     */
    public function ensureActive(): void
    {
        if ($this->closed) {
            throw new InvalidStateException('Transaction is not active');
        }
        if ($this->status !== TransactionStatus::Active) {
            throw new InvalidStateException('Transaction is not active');
        }
    }

    public function ensureWritable(): void
    {
        $this->ensureActive();
        if ($this->pessimisticWriteFailed) {
            throw new InvalidStateException(
                'Pessimistic write failed; rollback the transaction before continuing',
            );
        }
        if ($this->commitStarted) {
            throw new InvalidStateException(
                'Transaction commit has started; rollback before changing writes',
            );
        }
        if ($this->rollbackStarted) {
            throw new InvalidStateException(
                'Transaction rollback has started; no further writes are allowed',
            );
        }
    }

    public function ensureCommitAllowed(): void
    {
        $this->ensureActive();
        if ($this->pessimisticWriteFailed) {
            throw new InvalidStateException(
                'Pessimistic write failed; rollback the transaction before committing',
            );
        }
        if ($this->rollbackStarted) {
            throw new InvalidStateException(
                'Transaction rollback has started; commit is no longer allowed',
            );
        }
    }

    public function markPessimisticWriteFailed(): void
    {
        $this->pessimisticWriteFailed = true;
    }

    public function markCommitStarted(): void
    {
        $this->commitStarted = true;
    }

    public function markRollbackStarted(): void
    {
        $this->rollbackStarted = true;
    }

    public function close(): void
    {
        $this->closed = true;
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    // -- Status -------------------------------------------------------------------

    public function getStatus(): TransactionStatus
    {
        return $this->status;
    }

    public function setStatus(TransactionStatus $status): void
    {
        $this->status = $status;
    }

    // -- Commit TS ----------------------------------------------------------------

    public function getCommitTs(): ?int
    {
        return $this->commitTs;
    }

    public function setCommitTs(?int $commitTs): void
    {
        $this->commitTs = $commitTs;
    }

    // -- Write Set ----------------------------------------------------------------

    /**
     * @return array<string, ?string>
     */
    public function getWriteSet(): array
    {
        return $this->writeSet;
    }

    /**
     * @param array<string, ?string> $writeSet
     */
    public function setWriteSet(array $writeSet): void
    {
        $this->writeSet = $writeSet;
    }

    public function hasWriteSetKey(string $key): bool
    {
        return array_key_exists($key, $this->writeSet);
    }

    public function getWriteSetValue(string $key): ?string
    {
        return $this->writeSet[$key] ?? null;
    }

    public function setWrite(string $key, ?string $value): void
    {
        $this->writeSet[$key] = $value;
    }

    public function clearWriteSet(): void
    {
        $this->writeSet = [];
    }

    /**
     * Returns the first key of the write set.
     *
     * @throws InvalidStateException if the write set is empty
     */
    public function getPrimaryKey(): string
    {
        $key = array_key_first($this->writeSet);
        if ($key === null) {
            throw new InvalidStateException('Write set is empty, no primary key');
        }

        // PHP coerces integer-like string keys ("12345", "0") to int array
        // keys, so the key must be string-cast before returning it (issue #322).
        return (string) $key;
    }

    /**
     * @return string[]
     */
    public function getWriteKeys(): array
    {
        return array_map(strval(...), array_keys($this->writeSet));
    }

    public function isEmptyWriteSet(): bool
    {
        return $this->writeSet === [];
    }

    // -- Read Set -----------------------------------------------------------------

    /**
     * @return array<string, ?string>
     */
    public function getReadSet(): array
    {
        return $this->readSet;
    }

    public function setReadValue(string $key, ?string $value): void
    {
        $this->readSet[$key] = $value;
    }

    public function clearReadSet(): void
    {
        $this->readSet = [];
    }

    // -- Pending Lock Keys --------------------------------------------------------

    /**
     * @return string[]
     */
    public function getPendingLockKeys(): array
    {
        return $this->pendingLockKeys;
    }

    public function addPendingLockKey(string $key): void
    {
        if (!in_array($key, $this->pendingLockKeys, true)) {
            $this->pendingLockKeys[] = $key;
        }
    }

    public function hasPendingLockKey(string $key): bool
    {
        return in_array($key, $this->pendingLockKeys, true);
    }

    public function clearPendingLockKeys(): void
    {
        $this->pendingLockKeys = [];
    }

    // -- Pessimistic lock tracking -------------------------------------------------

    public function addPessimisticLockAttempt(string $key): void
    {
        if (!in_array($key, $this->pessimisticLockAttempts, true)) {
            $this->pessimisticLockAttempts[] = $key;
        }
    }

    /**
     * @param string[] $keys
     */
    public function markPessimisticLocksAcquired(array $keys): void
    {
        foreach ($keys as $key) {
            $this->addPessimisticLockAttempt($key);
            if (!in_array($key, $this->pessimisticLocks, true)) {
                $this->pessimisticLocks[] = $key;
            }
        }
    }

    public function hasPessimisticLock(string $key): bool
    {
        return in_array($key, $this->pessimisticLocks, true);
    }

    public function hasAcknowledgedPessimisticLocks(): bool
    {
        return $this->pessimisticLocks !== [];
    }

    public function hasPessimisticLockActivity(): bool
    {
        return $this->pendingLockKeys !== []
            || $this->pessimisticLockAttempts !== []
            || $this->pessimisticLocks !== [];
    }

    /**
     * Return every key that may have acquired a server-side lock and therefore
     * must be considered by rollback. This includes attempted keys because a
     * transport failure or a cancelled fan-out future can hide a successful
     * server-side lock.
     *
     * @return string[]
     */
    public function getPessimisticLockKeys(): array
    {
        return array_values(array_unique(array_merge(
            $this->pessimisticLockAttempts,
            $this->pessimisticLocks,
            $this->pendingLockKeys,
        )));
    }

    public function setPessimisticPrimaryKey(string $key): void
    {
        $this->pessimisticPrimaryKey ??= $key;
    }

    public function getPessimisticPrimaryKey(): ?string
    {
        return $this->pessimisticPrimaryKey;
    }

    // -- For Update TS ------------------------------------------------------------

    public function getMaxForUpdateTs(): ?int
    {
        return $this->maxForUpdateTs;
    }

    public function setMaxForUpdateTs(?int $ts): void
    {
        $this->maxForUpdateTs = $ts;
    }

    public function updateMaxForUpdateTs(int $ts): void
    {
        if ($this->maxForUpdateTs === null || $ts > $this->maxForUpdateTs) {
            $this->maxForUpdateTs = $ts;
        }
    }
}
