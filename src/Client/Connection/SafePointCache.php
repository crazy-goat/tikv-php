<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Pdpb\GetGCSafePointRequest;
use CrazyGoat\Proto\Pdpb\GetGCSafePointResponse;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Bounded TTL cache of the cluster's current GC safe point, fetched from PD.
 *
 * Issue #422: transactions read at a start timestamp obtained at begin()
 * with no knowledge of GC. A long-running read whose startTs falls below
 * the cluster's GC safe point fails partway through with an untyped
 * "GC life time is shorter than transaction duration" error. Validating
 * the startTs against a (cheap, cached) safe point turns that into an
 * immediate, typed TxnAbortedByGcException before any data is read.
 *
 * The cache is refreshed at most once per $refreshIntervalMs and served
 * from memory otherwise, so begin() does not add a PD round trip per
 * transaction. Construction is lazy: the first read() issues the RPC, and
 * a failed fetch throws — a safe point is never guessed locally, mirroring
 * the fail-closed TSO behaviour in TimestampOracle.
 */
final class SafePointCache
{
    /** Default refresh interval (30s) — well under PD's default gc_life_time of 10m. */
    public const DEFAULT_REFRESH_INTERVAL_MS = 30000;

    private ?int $cachedSafePoint = null;
    private int $cachedAtMs = 0;

    public function __construct(
        /** @var \Closure(): int fetches the current GC safe point from PD (injected so the cache does not depend on PdClient itself) */
        private readonly \Closure $fetchSafePoint,
        private readonly int $refreshIntervalMs = self::DEFAULT_REFRESH_INTERVAL_MS,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
        if ($refreshIntervalMs < 1) {
            throw new \InvalidArgumentException('refreshIntervalMs must be >= 1');
        }
    }

    /**
     * Current GC safe point, served from cache when fresh.
     *
     * @throws TiKvException when the PD fetch fails (fail closed)
     */
    public function get(): int
    {
        $nowMs = (int) (microtime(true) * 1000);

        if (
            $this->cachedSafePoint !== null
            && ($nowMs - $this->cachedAtMs) < $this->refreshIntervalMs
        ) {
            return $this->cachedSafePoint;
        }

        $safePoint = ($this->fetchSafePoint)();
        if ($safePoint < 0) {
            // Defensive: PD safe points are uint64. Reject instead of
            // validating startTs against a nonsense bound.
            throw new TiKvException(sprintf('PD returned invalid GC safe point: %d', $safePoint));
        }

        $this->logger->debug('Refreshed GC safe point', ['safePoint' => $safePoint]);
        $this->cachedSafePoint = $safePoint;
        $this->cachedAtMs = $nowMs;

        return $safePoint;
    }

    /** Drop the cached value; the next read() re-fetches from PD. */
    public function invalidate(): void
    {
        $this->cachedSafePoint = null;
        $this->cachedAtMs = 0;
    }
}
