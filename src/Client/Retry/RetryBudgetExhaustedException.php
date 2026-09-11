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
        /**
         * Raw user key, exposed for programmatic handling only. It is
         * deliberately NOT part of $message (and therefore never reaches a
         * log formatter / error tracker) — use KeyRedactor::redact() for the
         * human-readable representation (issue #269, GRPC-10).
         */
        private readonly ?string $rawKey = null,
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

    /**
     * The raw user key this exception was raised for, if one was supplied.
     *
     * Kept out of getMessage() so no raw key material can leak through the
     * logging/error-tracking channel; callers that genuinely need the key
     * should use this typed accessor instead of parsing the message.
     */
    public function getRawKey(): ?string
    {
        return $this->rawKey;
    }
}
