<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Util\ServerText;

final class TransactionConflictException extends TiKvException
{
    /**
     * @param string[] $conflictingKeys
     */
    public function __construct(
        string $message = 'Transaction conflict detected',
        private readonly ?array $conflictingKeys = null,
    ) {
        parent::__construct(ServerText::sanitise($message));
    }

    /**
     * @return string[]|null
     */
    public function getConflictingKeys(): ?array
    {
        return $this->conflictingKeys;
    }
}
