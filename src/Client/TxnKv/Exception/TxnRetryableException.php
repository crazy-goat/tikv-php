<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Retry\BackoffType;

/**
 * Exception thrown for transactional operations that should be retried
 * with a specific backoff strategy, determined by the carried BackoffType.
 *
 * Carrying the BackoffType directly eliminates the need for the classifier
 * to re-parse exception message text.
 */
final class TxnRetryableException extends TiKvException
{
    public function __construct(
        string $message,
        public readonly BackoffType $backoffType,
    ) {
        parent::__construct($message);
    }
}
