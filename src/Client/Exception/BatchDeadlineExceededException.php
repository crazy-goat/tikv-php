<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Exception;

final class BatchDeadlineExceededException extends TiKvException
{
    /**
     * @param int                  $deadlineMs  Wall-clock budget (ms) the batch was given
     * @param int                  $elapsedMs   Wall-clock time (ms) actually elapsed
     * @param array<string, mixed> $context     Diagnostic context (e.g. pending regions)
     */
    public function __construct(
        private readonly int $deadlineMs,
        private readonly int $elapsedMs,
        private readonly array $context = [],
    ) {
        parent::__construct(sprintf(
            'Batch operation exceeded its %d ms deadline (elapsed %d ms)',
            $deadlineMs,
            $elapsedMs,
        ));
    }

    public function getDeadlineMs(): int
    {
        return $this->deadlineMs;
    }

    public function getElapsedMs(): int
    {
        return $this->elapsedMs;
    }

    /**
     * @return array<string, mixed>
     */
    public function getContext(): array
    {
        return $this->context;
    }
}
