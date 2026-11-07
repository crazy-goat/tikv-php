<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

/**
 * Retry budget shared by every attempt of one logical operation (issue #223).
 *
 * A {@see RetryExecutor} is stateless with respect to a budget: `execute()`
 * without an operation-scoped executor starts a fresh budget, while
 * {@see RetryExecutor::beginOperation()} returns an executor whose
 * `execute()` calls all charge the same instance. That is what keeps the
 * multi-region / multi-page fan-out of one operation inside a single
 * `maxBackoffMs`, `serverBusyBudgetMs` and wall-clock deadline bound, without
 * leaking the spend into the next operation.
 *
 * The attempt cap deliberately stays per `execute()` call: it is a
 * termination backstop for one retry loop, not a time budget, and sharing it
 * across a fan-out would let a scan over many regions exhaust the cap on
 * ordinary one-retry-per-region traffic.
 */
final class RetryBudget
{
    private int $totalBackoffMs = 0;
    private int $serverBusyBackoffMs = 0;

    /**
     * @param int $startTimeMs Wall-clock start of the operation, or 0 when
     *                         the wall-clock deadline is disabled.
     * @param int $deadlineMs  Wall-clock deadline for the operation, or 0
     *                         when it is disabled.
     */
    public function __construct(
        private readonly int $startTimeMs,
        private readonly int $deadlineMs = 0,
    ) {
    }

    public function startTimeMs(): int
    {
        return $this->startTimeMs;
    }

    /**
     * Remaining wall-clock budget in milliseconds, or 0 when the deadline is
     * disabled.
     *
     * The value is computed at call time, so it accounts for the time the
     * current attempt has already spent in an RPC; it is not a snapshot
     * taken before the attempt. A configured but already-elapsed deadline
     * returns 1, never 0, so a caller cannot mistake an exhausted budget for
     * "no deadline" (issue #569).
     */
    public function remainingDeadlineMs(): int
    {
        if ($this->deadlineMs <= 0 || $this->startTimeMs <= 0) {
            return 0;
        }

        $elapsedMs = (int) (microtime(true) * 1000) - $this->startTimeMs;

        return max(1, $this->deadlineMs - $elapsedMs);
    }

    public function totalBackoffMs(): int
    {
        return $this->totalBackoffMs;
    }

    /**
     * Charge one non-ServerBusy backoff sleep and return the new total.
     */
    public function addBackoff(int $sleepMs): int
    {
        return $this->totalBackoffMs += $sleepMs;
    }

    /**
     * Charge one ServerBusy backoff sleep and return the new total.
     */
    public function addServerBusy(int $sleepMs): int
    {
        return $this->serverBusyBackoffMs += $sleepMs;
    }
}
