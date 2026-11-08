<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\Context;
use InvalidArgumentException;

/**
 * Bounded per-transaction record of the locks this reader has already
 * resolved (issue #432, GAP-18).
 *
 * TiKV accepts the start timestamps of transactions a request has already
 * resolved in `Context.resolved_locks`, and the committed ones the request
 * must read through in `Context.committed_locks`. The two sets are disjoint:
 * a transaction is classified against the reader's start timestamp, because a
 * commit at or before that timestamp is visible to the snapshot while a later
 * commit is not. Sending them on later requests lets the server skip locks it
 * can ignore and read through locks it must see, instead of making the client
 * resolve the same transaction again after a retry or a region cache
 * invalidation.
 *
 * Both sets are bounded FIFO maps keyed by the lock's start timestamp. The
 * bound matters because a long-lived snapshot under sustained contention
 * would otherwise accumulate one entry per resolved transaction for the
 * lifetime of the transaction; client-go's `TSSet` is unbounded, so this is
 * deliberately stricter. The oldest timestamp is evicted first, mirroring
 * {@see ResolvedTxnStatusCache}.
 */
final class ResolvedLockTracker
{
    /**
     * Default number of timestamps kept per set. Matches the capacity of the
     * resolved-status cache (client-go keeps 2048 resolved transaction
     * statuses as well).
     */
    public const DEFAULT_CAPACITY = 2048;

    /** @var array<int, true> lock start ts => true, oldest first */
    private array $resolvedLocks = [];

    /** @var array<int, true> lock start ts => true, oldest first */
    private array $committedLocks = [];

    public function __construct(private readonly int $capacity = self::DEFAULT_CAPACITY)
    {
        if ($capacity < 1) {
            throw new InvalidArgumentException('Resolved lock tracker capacity must be at least 1');
        }
    }

    /**
     * Remember a transaction the reader can ignore: rolled back, or committed
     * after the reader's start timestamp.
     */
    public function recordResolved(int $lockTs): void
    {
        $this->insert($this->resolvedLocks, $lockTs);
    }

    /**
     * Remember a transaction the reader must read through: committed at or
     * before the reader's start timestamp.
     */
    public function recordCommitted(int $lockTs): void
    {
        $this->insert($this->committedLocks, $lockTs);
    }

    /**
     * @return list<int> lock start timestamps, oldest first
     */
    public function getResolvedLocks(): array
    {
        return array_keys($this->resolvedLocks);
    }

    /**
     * @return list<int> lock start timestamps resolved as committed, oldest first
     */
    public function getCommittedLocks(): array
    {
        return array_keys($this->committedLocks);
    }

    public function resolvedCount(): int
    {
        return count($this->resolvedLocks);
    }

    public function committedCount(): int
    {
        return count($this->committedLocks);
    }

    public function isEmpty(): bool
    {
        return $this->resolvedLocks === [] && $this->committedLocks === [];
    }

    /**
     * Copy both sets onto a request Context.
     *
     * An empty set leaves the field unset, so a request sent before any lock
     * was resolved keeps its original wire bytes.
     */
    public function applyTo(Context $context): void
    {
        $resolved = $this->getResolvedLocks();
        if ($resolved !== []) {
            $context->setResolvedLocks($resolved);
        }

        $committed = $this->getCommittedLocks();
        if ($committed !== []) {
            $context->setCommittedLocks($committed);
        }
    }

    /**
     * @param array<int, true> $set
     */
    private function insert(array &$set, int $lockTs): void
    {
        if (isset($set[$lockTs])) {
            return;
        }

        if (count($set) >= $this->capacity) {
            unset($set[array_key_first($set)]);
        }

        $set[$lockTs] = true;
    }
}
