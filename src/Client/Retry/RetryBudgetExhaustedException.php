<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

/**
 * Thrown when RetryExecutor's attempt-cap or wall-clock deadline is reached
 * before the operation succeeds. The original TiKV exception is not lost —
 * callers that want to inspect or rethrow it should catch this and check
 * {@see RetryBudgetExhaustedException::getPrevious()}.
 */
final class RetryBudgetExhaustedException extends TiKvException
{
    public function __construct(
        string $message,
        private readonly int $attempts,
        private readonly int $elapsedOrBackoffMs,
        ?\Throwable $previous = null,
    ) {
        parent::__construct($message, 0, $previous);
    }

    public function attempts(): int
    {
        return $this->attempts;
    }

    public function elapsedOrBackoffMs(): int
    {
        return $this->elapsedOrBackoffMs;
    }
}
