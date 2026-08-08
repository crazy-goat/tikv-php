<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class TestLogger implements LoggerInterface
{
    /** @var array<string, list<array{message: string, context: array<string, mixed>}>> */
    private array $records = [];

    public function emergency(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::EMERGENCY, $message, $context);
    }

    public function alert(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::ALERT, $message, $context);
    }

    public function critical(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::CRITICAL, $message, $context);
    }

    public function error(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::ERROR, $message, $context);
    }

    public function warning(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::WARNING, $message, $context);
    }

    public function notice(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::NOTICE, $message, $context);
    }

    public function info(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::INFO, $message, $context);
    }

    public function debug(string|\Stringable $message, array $context = []): void
    {
        $this->log(LogLevel::DEBUG, $message, $context);
    }

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        if (!is_string($level)) {
            throw new \InvalidArgumentException('Log level must be a string');
        }

        $this->records[$level][] = [
            'message' => (string) $message,
            'context' => $context,
        ];
    }

    /**
     * @return list<array{message: string, context: array<string, mixed>}>
     */
    public function records(string $level): array
    {
        return $this->records[$level] ?? [];
    }
}
