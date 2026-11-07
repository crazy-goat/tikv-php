<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Retry;

/**
 * Outcome of one error-classifier decision (issue #534).
 *
 * A classifier used to return `?BackoffType`, which conflated two different
 * answers: "I have no opinion, ask the built-in classifier" and "this error is
 * fatal, do not retry". A classifier that needed the fatal verdict had to
 * throw from inside the executor's catch block, which skipped the executor's
 * own fatal path (region invalidation and the broken-channel cleanup).
 *
 * This result type keeps the three answers apart:
 *
 * - {@see self::retry()} — retry with the carried {@see BackoffType};
 * - {@see self::fatal()} — do not retry, and run the executor's fatal path;
 * - {@see self::pass()} — no opinion, fall through to the next classifier.
 */
final readonly class RetryVerdict
{
    private function __construct(
        private ?BackoffType $backoffType,
        private bool $fatal,
    ) {
    }

    public static function retry(BackoffType $backoffType): self
    {
        return new self($backoffType, false);
    }

    public static function fatal(): self
    {
        return new self(null, true);
    }

    public static function pass(): self
    {
        return new self(null, false);
    }

    public function isRetry(): bool
    {
        return $this->backoffType instanceof BackoffType;
    }

    public function isFatal(): bool
    {
        return $this->fatal;
    }

    public function isPass(): bool
    {
        return !$this->fatal && !$this->backoffType instanceof BackoffType;
    }

    /**
     * The backoff to use for a retry verdict.
     *
     * @throws \LogicException when this verdict is not a retry verdict; asking
     *         a fatal or pass verdict for a backoff is a programming error.
     */
    public function backoffType(): BackoffType
    {
        if (!$this->backoffType instanceof BackoffType) {
            throw new \LogicException('A non-retry RetryVerdict carries no BackoffType');
        }

        return $this->backoffType;
    }
}
