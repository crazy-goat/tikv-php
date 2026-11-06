<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

/**
 * Per-invocation view of a {@see RetryExecutor}'s wall-clock budget.
 *
 * The executor configures it with its deadline and operation start time, and
 * a nested operation that must respect the same deadline — lock resolution is
 * the first user — asks {@see self::remainingDeadlineMs()} when it needs the
 * value. The remaining budget is computed at call time, so it also accounts
 * for the time the current attempt has already spent in an RPC; it is not a
 * snapshot taken before the attempt (issue #569).
 *
 * `0` means the executor has no wall-clock deadline configured
 * (`retryDeadlineMs = 0`), which keeps the historical behaviour of the nested
 * operation. A configured but exhausted budget is reported as `1`, never `0`,
 * so it cannot be mistaken for "no deadline".
 *
 * A context belongs to exactly one `execute()` call: the caller creates it,
 * passes it to {@see RetryExecutor::execute()} and shares it with the
 * operation's nested calls. It is never reused across operations.
 */
final class RetryContext
{
    private int $deadlineMs = 0;
    private int $startTimeMs = 0;

    /**
     * Remaining wall-clock budget in milliseconds, or 0 when the executor has
     * no deadline configured. A configured deadline that has already elapsed
     * returns 1 (the minimum representable budget) so the caller fails closed
     * instead of treating it as "no deadline".
     */
    public function remainingDeadlineMs(): int
    {
        if ($this->deadlineMs <= 0) {
            return 0;
        }

        $elapsedMs = (int) (microtime(true) * 1000) - $this->startTimeMs;

        return max(1, $this->deadlineMs - $elapsedMs);
    }

    /**
     * @internal Configured by {@see RetryExecutor} before each attempt.
     *
     * @param int $deadlineMs 0 disables the deadline.
     * @param int $startTimeMs Wall-clock start of the operation; ignored when
     *                         the deadline is disabled.
     */
    public function configure(int $deadlineMs, int $startTimeMs): void
    {
        $this->deadlineMs = $deadlineMs;
        $this->startTimeMs = $startTimeMs;
    }
}
