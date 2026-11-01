<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use InvalidArgumentException;

/**
 * Bounded FIFO cache of final transaction outcomes, keyed by lock start ts
 * (issue #428). Mirrors client-go's `resolved` cache (2048 entries).
 *
 * Only a final outcome may be stored: a commit ts above zero (committed) or
 * zero (rolled back). A transaction that is still alive, or whose state is
 * undetermined, can change on the next check and must never be stored.
 * {@see LockResolver} enforces that; this class only stores what it is given.
 */
final class ResolvedTxnStatusCache
{
    public const DEFAULT_CAPACITY = 2048;

    /** @var array<int, int> start ts => commit ts (0 = rolled back), oldest first */
    private array $entries = [];

    public function __construct(private readonly int $capacity = self::DEFAULT_CAPACITY)
    {
        if ($capacity < 1) {
            throw new InvalidArgumentException('Resolved status cache capacity must be at least 1');
        }
    }

    /**
     * @return int|null the commit ts (0 = rolled back), or null when unknown
     */
    public function get(int $startTs): ?int
    {
        return $this->entries[$startTs] ?? null;
    }

    public function put(int $startTs, int $commitTs): void
    {
        if (!isset($this->entries[$startTs]) && count($this->entries) >= $this->capacity) {
            unset($this->entries[array_key_first($this->entries)]);
        }
        $this->entries[$startTs] = $commitTs;
    }

    public function size(): int
    {
        return count($this->entries);
    }
}
