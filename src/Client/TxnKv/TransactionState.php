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

    /** @var string[] keys pending pessimistic lock (batched at commit time) */
    private array $pendingLockKeys = [];

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
        return $key;
    }

    /**
     * @return string[]
     */
    public function getWriteKeys(): array
    {
        return array_keys($this->writeSet);
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
        $this->pendingLockKeys[] = $key;
    }

    public function clearPendingLockKeys(): void
    {
        $this->pendingLockKeys = [];
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
