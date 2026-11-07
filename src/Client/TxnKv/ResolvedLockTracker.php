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
 * resolved in `Context.resolved_locks`, and the subset that was resolved as
 * committed in `Context.committed_locks`. Sending them on later requests lets
 * the server skip locks the client has already dealt with instead of making
 * the client resolve the same transaction again after a retry or a region
 * cache invalidation.
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
     * Remember that the transaction starting at `$lockTs` was resolved.
     *
     * A positive commit timestamp also records the transaction as committed;
     * zero means it was rolled back and only belongs to the resolved set.
     */
    public function record(int $lockTs, int $commitTs): void
    {
        $evicted = $this->insert($this->resolvedLocks, $lockTs);
        if ($evicted !== null) {
            // Committed locks are a subset of the resolved ones: never report
            // a committed transaction the resolved set has already forgotten.
            unset($this->committedLocks[$evicted]);
        }

        if ($commitTs > 0) {
            $this->insert($this->committedLocks, $lockTs);
        }
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
     * @return int|null the evicted timestamp, or null when nothing was evicted
     */
    private function insert(array &$set, int $lockTs): ?int
    {
        if (isset($set[$lockTs])) {
            return null;
        }

        $evicted = null;
        if (count($set) >= $this->capacity) {
            $evicted = array_key_first($set);
            unset($set[$evicted]);
        }

        $set[$lockTs] = true;

        return $evicted;
    }
}
