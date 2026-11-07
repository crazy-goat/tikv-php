<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Support;

use Psr\Log\AbstractLogger;

/**
 * A logger that keeps every record's message and context.
 *
 * Unlike {@see RecordingLogger}, which keeps only the message, this one
 * preserves the context array so a test can assert on the values the client
 * reports alongside a line (a retry delay, a budget, a region id).
 */
final class ContextRecordingLogger extends AbstractLogger
{
    /** @var list<array{level: mixed, message: string, context: array<string, mixed>}> */
    public array $records = [];

    /**
     * @param mixed                $level
     * @param array<string, mixed> $context
     */
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = [
            'level' => $level,
            'message' => (string) $message,
            'context' => $context,
        ];
    }

    /**
     * Contexts of every record whose message is exactly $message.
     *
     * @return list<array<string, mixed>>
     */
    public function contextsFor(string $message): array
    {
        $contexts = [];
        foreach ($this->records as $record) {
            if ($record['message'] === $message) {
                $contexts[] = $record['context'];
            }
        }

        return $contexts;
    }
}
