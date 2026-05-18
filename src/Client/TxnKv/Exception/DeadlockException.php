<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

final class DeadlockException extends TiKvException
{
    public function __construct(
        string $message = 'Deadlock detected',
        private readonly ?string $deadlockKey = null,
        private readonly ?int $deadlockKeyHash = null,
        private readonly ?int $lockTs = null,
    ) {
        parent::__construct($message);
    }

    public function getDeadlockKey(): ?string
    {
        return $this->deadlockKey;
    }

    public function getDeadlockKeyHash(): ?int
    {
        return $this->deadlockKeyHash;
    }

    public function getLockTs(): ?int
    {
        return $this->lockTs;
    }
}
