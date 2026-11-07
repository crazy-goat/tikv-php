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
 * - the configured per-request `lockWaitTimeMs`, which the committer caps to
 *   `min(configured wait, remaining budget, RPC deadline - 100 ms)` before
 *   sending it as `PessimisticLockRequest.wait_timeout`.
 *
 * Both are exposed so a caller can tell a short per-request wait from a
 * short retry budget.
 */
final class LockWaitTimeoutException extends TiKvException
{
    public function __construct(
        private readonly string $key,
        private readonly int $timeoutMs,
        private readonly int $configuredWaitTimeoutMs = 0,
    ) {
        parent::__construct(sprintf(
            'Lock wait timeout for key: %s (budget %dms, %s)',
            KeyRedactor::redact($key),
            $timeoutMs,
            $this->describeConfiguredWaitTimeout($configuredWaitTimeoutMs),
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
        return $this->timeoutMs;
    }

    /** The client-side lock-wait budget that expired (`maxBackoffMs`). */
    public function getBudgetMs(): int
    {
        return $this->timeoutMs;
    }

    /**
     * The **configured** per-request server-side wait (`lockWaitTimeMs`),
     * before the remaining-budget and gRPC-deadline caps. It is not
     * necessarily the value the last request carried: the effective value is
     * `min(configured wait, remaining budget, RPC deadline - 100 ms)`. `0`
     * means "let TiKV use its own default timeout".
     */
    public function getConfiguredWaitTimeoutMs(): int
    {
        return $this->configuredWaitTimeoutMs;
    }

    private function describeConfiguredWaitTimeout(int $configuredWaitTimeoutMs): string
    {
        return $configuredWaitTimeoutMs === 0
            ? 'per-request wait: TiKV default'
            : sprintf('per-request wait %dms', $configuredWaitTimeoutMs);
    }
}
