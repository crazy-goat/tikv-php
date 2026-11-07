<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv\Exception;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;

/**
 * A pessimistic lock wait exhausted the client-side lock-wait budget.
 *
 * Two different timeouts are involved (issue #229, TXN-24):
 * - the budget (`maxBackoffMs`) that the client spent retrying, which is
 *   the timeout that actually expired, and
 * - the per-request `lockWaitTimeMs` handed to TiKV as
 *   `PessimisticLockRequest.wait_timeout`.
 *
 * Both are exposed so a caller can tell a short per-request wait from a
 * short retry budget.
 */
final class LockWaitTimeoutException extends TiKvException
{
    public function __construct(
        private readonly string $key,
        private readonly int $budgetMs,
        private readonly int $waitTimeoutMs = 0,
    ) {
        parent::__construct(sprintf(
            'Lock wait timeout for key: %s (budget %dms, per-request wait %dms)',
            KeyRedactor::redact($key),
            $budgetMs,
            $waitTimeoutMs,
        ));
    }

    public function getKey(): string
    {
        return $this->key;
    }

    /**
     * The client-side lock-wait budget that expired (`maxBackoffMs`).
     *
     * The historical name is kept for backwards compatibility; use
     * {@see self::getBudgetMs()} for the unambiguous spelling.
     */
    public function getTimeoutMs(): int
    {
        return $this->budgetMs;
    }

    /** The client-side lock-wait budget that expired (`maxBackoffMs`). */
    public function getBudgetMs(): int
    {
        return $this->budgetMs;
    }

    /**
     * The configured per-request server-side wait (`lockWaitTimeMs`), the
     * value sent as `PessimisticLockRequest.wait_timeout`. `0` means TiKV's
     * own default timeout.
     */
    public function getWaitTimeoutMs(): int
    {
        return $this->waitTimeoutMs;
    }
}
