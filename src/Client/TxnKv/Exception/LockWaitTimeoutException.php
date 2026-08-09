<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

final class LockWaitTimeoutException extends TiKvException
{
    public function __construct(
        private readonly string $key,
        private readonly int $timeoutMs,
    ) {
        $redacted = KeyRedactor::redact($key);
        parent::__construct("Lock wait timeout for key: {$redacted} ({$timeoutMs}ms)");
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getTimeoutMs(): int
    {
        return $this->timeoutMs;
    }
}
