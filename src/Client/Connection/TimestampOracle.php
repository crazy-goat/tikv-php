<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Pdpb\RequestHeader;
use CrazyGoat\Proto\Pdpb\TsoRequest;
use CrazyGoat\Proto\Pdpb\TsoResponse;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * PD TSO oracle: pooling, batching and low-resolution caching of PD
 * timestamps.
 *
 * `getTimestamp()` serves consecutive values from a small per-client pool
 * (issue #292); `getTimestampBatch()` issues one explicitly-sized grant
 * (issue #420) and `getLowResolutionTimestamp()` adds a staleness-bounded
 * cache for staleness-tolerant consumers.
 *
 * TSO timestamps carry an 18-bit logical counter inside one physical
 * millisecond; a single TSO grant of $count timestamps covers the
 * consecutive values base - $count + 1 … base (PD's response timestamp
 * is the highest of the grant — see client-rust's `allocate_timestamps`).
 * Plain integer addition/subtraction composes/wraps the logical part
 * correctly.
 */
final class TimestampOracle
{
    /** Number of logical bits inside one physical millisecond (issue #420). */
    private const LOGICAL_SHIFT = 18;

    /**
     * Default number of timestamps requested per pooled TSO grant
     * (issue #292). Kept inside the 32–128 range suggested by the audit:
     * large enough to amortise the RPC over many `getTimestamp()` calls,
     * small enough that the pooled physical window stays short.
     */
    public const DEFAULT_TIMESTAMP_POOL_SIZE = 64;

    /**
     * Upper bound on `tsoPoolSize` (issue #292), and therefore on
     * `TsoRequest.count` (a uint32) and the size of the in-memory pool.
     * A larger grant lengthens the pooled physical window — and with it
     * the real-time-ordering risk of serving an old timestamp — while the
     * RPC-amortisation benefit flattens out.
     */
    public const MAX_TIMESTAMP_POOL_SIZE = 1000;

    /**
     * Default maximum age (ms) of a pooled timestamp before the pool is
     * discarded and refilled (issue #292). This bounds the
     * real-time-ordering window: a pooled timestamp fetched up to this
     * many ms ago may be served after a concurrent commit, handing out a
     * `start_ts` below that `commit_ts` (stale read / spurious
     * `TxnAbortedByGcException`). A 64-timestamp grant spans well under
     * 1 ms of physical time, so 5 ms keeps reuse a small, documented
     * multiple of the grant's own window while still amortising the TSO
     * RPC over many `getTimestamp()` calls — the safety/performance
     * tradeoff.
     */
    public const DEFAULT_TIMESTAMP_POOL_MAX_AGE_MS = 5;

    /** Cached low-resolution timestamp, or null when not populated. */
    private ?int $lowResCachedTs = null;
    /** Wall-clock milliseconds (per {@see $clock}) at which the cache was filled. */
    private ?int $lowResCachedAtMs = null;
    /** Wall-clock milliseconds source for the low-resolution cache. */
    private readonly \Closure $clock;

    /** Number of timestamps requested per pooled TSO grant (issue #292). */
    private readonly int $poolSize;
    /** Maximum age (ms) of a pooled timestamp before refilling (issue #292). */
    private readonly int $poolMaxAgeMs;
    /** Process-id source for the fork guard (issue #292). */
    private readonly \Closure $pid;

    /** @var list<int> remaining timestamps from the last pooled grant (ascending) */
    private array $timestampPool = [];
    /** Index of the next unserved timestamp in {@see $timestampPool}. */
    private int $poolOffset = 0;
    /** Wall-clock milliseconds at which the current pool was fetched. */
    private ?int $poolFetchedAtMs = null;
    /** Cluster ID observed when the pool was fetched (null = not yet learned). */
    private ?int $poolClusterId = null;
    /** PID that fetched the pool; a fork invalidates the pool. */
    private ?int $poolPid = null;

    /**
     * @param \Closure(): ?int $getClusterId
     * @param \Closure(int): void $setClusterId
     * @param int|null $lowResMaxStalenessMs maximum allowed staleness (ms) of the
     *                                       low-resolution timestamp cache;
     *                                       null = no caching (default), 0 = a
     *                                       cached value may be reused only
     *                                       within the same wall-clock
     *                                       millisecond (never stale data)
     * @param \Closure(): int $clock wall-clock milliseconds; injectable for tests
     * @param int|null $poolSize number of timestamps requested per pooled TSO
     *                           grant (issue #292). null = the default
     *                           ({@see self::DEFAULT_TIMESTAMP_POOL_SIZE});
     *                           1 disables pooling; must be >= 1 and
     *                           <= {@see self::MAX_TIMESTAMP_POOL_SIZE}
     * @param int|null $poolMaxAgeMs maximum age (ms) a pooled timestamp may
     *                               reach before the pool is discarded and
     *                               refilled; null = the default
     *                               ({@see self::DEFAULT_TIMESTAMP_POOL_MAX_AGE_MS});
     *                               must be >= 0
     * @param (\Closure(): int)|null $pid process-id source for the fork guard;
     *                                 null = getmypid(); injectable for tests
     */
    public function __construct(
        private readonly GrpcClientInterface $grpc,
        private readonly string $pdAddress,
        private readonly \Closure $getClusterId,
        private readonly \Closure $setClusterId,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly ?int $lowResMaxStalenessMs = null,
        ?\Closure $clock = null,
        ?int $poolSize = null,
        ?int $poolMaxAgeMs = null,
        ?\Closure $pid = null,
    ) {
        $this->clock = $clock ?? static fn (): int => (int) (microtime(true) * 1000);

        $resolvedPoolSize = $poolSize ?? self::DEFAULT_TIMESTAMP_POOL_SIZE;
        if ($resolvedPoolSize < 1) {
            throw new InvalidArgumentException('Timestamp pool size must be >= 1');
        }
        if ($resolvedPoolSize > self::MAX_TIMESTAMP_POOL_SIZE) {
            throw new InvalidArgumentException(sprintf(
                'Timestamp pool size must be <= %d',
                self::MAX_TIMESTAMP_POOL_SIZE,
            ));
        }
        $this->poolSize = $resolvedPoolSize;

        $resolvedPoolMaxAgeMs = $poolMaxAgeMs ?? self::DEFAULT_TIMESTAMP_POOL_MAX_AGE_MS;
        if ($resolvedPoolMaxAgeMs < 0) {
            throw new InvalidArgumentException('Timestamp pool max age must be >= 0');
        }
        $this->poolMaxAgeMs = $resolvedPoolMaxAgeMs;

        $this->pid = $pid ?? static fn (): int => (int) getmypid();
    }

    /**
     * Request a monotonically increasing timestamp from PD's TSO service.
     *
     * Since issue #292 the oracle keeps a small pool of consecutive
     * timestamps obtained from a single `Tso` RPC (`count = poolSize`,
     * default {@see self::DEFAULT_TIMESTAMP_POOL_SIZE}). Each call hands
     * out the next pooled value, so N calls cost roughly N / poolSize
     * round trips. The pool is discarded — and a fresh grant requested —
     * whenever it is exhausted, outlives its real-time-ordering window
     * (`poolMaxAgeMs`, default
     * {@see self::DEFAULT_TIMESTAMP_POOL_MAX_AGE_MS}), the process forks,
     * or the cluster ID changes.
     *
     * Fails closed on TSO unavailability: a locally fabricated timestamp
     * would violate TiKV MVCC ordering (snapshot isolation / global ordering),
     * so callers must observe the failure and decide whether to retry or
     * abort the transaction.
     *
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds (null = no timeout)
     *
     * @throws TiKvException when the TSO RPC fails or returns an invalid response
     */
    public function getTimestamp(?int $timeoutMs = null): int
    {
        if ($this->poolSize <= 1) {
            return $this->getTimestampBatch(1, $timeoutMs)[0];
        }

        $this->discardPoolIfInvalid();

        if ($this->poolOffset < count($this->timestampPool)) {
            /** @var int $pooled */
            $pooled = $this->timestampPool[$this->poolOffset];
            $this->poolOffset++;

            return $pooled;
        }

        return $this->refillPool($timeoutMs);
    }

    /**
     * Request a batch of monotonically increasing timestamps from PD's
     * TSO service in a single RPC (issue #420, GAP-06).
     *
     * A single `Tso` request with `count = $count` costs one round trip
     * and yields a consecutive range of timestamps; PD's response
     * timestamp is the highest of the granted range, so the range is
     * handed out in ascending order as
     * `base - (n - 1) … base` (plain integer arithmetic composes and
     * wraps the 18-bit logical counter inside the physical millisecond
     * correctly).
     *
     * An explicit batch advances PD beyond any pooled range, so the
     * internal pool is discarded first: serving a pooled timestamp
     * afterwards could return a value below one this batch already
     * returned. Use {@see getTimestamp()} for the pooled path.
     *
     * @param int $count number of timestamps to request (>= 1)
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds (null = no timeout)
     *
     * @return list<int> at most $count monotonically increasing timestamps
     *                   (PD may grant fewer than requested; never fewer than 1)
     *
     * @throws InvalidArgumentException when $count is < 1
     * @throws TiKvException when the TSO RPC fails or returns an invalid response
     */
    public function getTimestampBatch(int $count, ?int $timeoutMs = null): array
    {
        if ($count < 1) {
            throw new InvalidArgumentException('Timestamp batch count must be >= 1');
        }

        $this->resetPool();

        return $this->requestTimestampRange($count, $timeoutMs);
    }

    /**
     * Issue one `Tso` RPC and return up to $count consecutive timestamps.
     *
     * Deliberately pool-unaware: callers that must not disturb the pooled
     * stream (the pool refill itself, and the low-resolution cache) use
     * this method; {@see getTimestampBatch()} wraps it and discards the
     * pool first.
     *
     * @return list<int>
     *
     * @throws TiKvException when the TSO RPC fails or returns an invalid response
     */
    private function requestTimestampRange(int $count, ?int $timeoutMs): array
    {
        $request = new TsoRequest();
        $request->setHeader($this->createHeader());
        $request->setCount($count);

        try {
            $response = $this->callTso($request, $timeoutMs);

            return $this->extractTimestampRange($response, $count);
        } catch (GrpcException $e) {
            $this->logger->error('TSO request failed; refusing to fabricate a local timestamp', [
                'error' => $e->getMessage(),
                'grpcStatusCode' => $e->grpcStatusCode,
            ]);

            throw new TiKvException(
                sprintf('TSO request failed: %s', $e->getMessage()),
                $e->grpcStatusCode,
                $e,
            );
        }
    }

    /**
     * Refill the pool from a fresh TSO grant and return its first
     * timestamp.
     *
     * Any previous pool is discarded first so a PD error leaves nothing
     * behind for a later call to serve.
     *
     * @throws TiKvException when the TSO RPC fails or returns an invalid response
     */
    private function refillPool(?int $timeoutMs): int
    {
        $this->resetPool();

        $fetchedAtMs = ($this->clock)();
        $range = $this->requestTimestampRange($this->poolSize, $timeoutMs);

        /** @var int $first */
        $first = array_shift($range);

        if ($range !== []) {
            $this->timestampPool = $range;
            $this->poolFetchedAtMs = $fetchedAtMs;
            $this->poolClusterId = ($this->getClusterId)();
            $this->poolPid = ($this->pid)();
        }

        return $first;
    }

    /**
     * Discard the pool when it can no longer be served safely.
     *
     * The pool is invalidated when:
     *  - it outlived its physical window (`age > poolMaxAgeMs`, or the
     *    clock jumped backwards — uncertain, so refill rather than risk a
     *    stale timestamp);
     *  - the process changed (fork guard: parent and child would otherwise
     *    hand out the same timestamps);
     *  - the cluster ID changed (a pool granted by another cluster must
     *    never be served).
     */
    private function discardPoolIfInvalid(): void
    {
        if ($this->poolOffset >= count($this->timestampPool)) {
            return;
        }

        $fetchedAtMs = $this->poolFetchedAtMs;
        if ($fetchedAtMs === null) {
            $this->resetPool();
            return;
        }

        $ageMs = ($this->clock)() - $fetchedAtMs;
        if ($ageMs < 0 || $ageMs > $this->poolMaxAgeMs) {
            $this->logger->debug('Discarding TSO pool outside its physical window', ['ageMs' => $ageMs]);
            $this->resetPool();
            return;
        }

        if ($this->poolPid !== ($this->pid)()) {
            $this->logger->debug('Discarding TSO pool after process change');
            $this->resetPool();
            return;
        }

        if ($this->poolClusterId !== ($this->getClusterId)()) {
            $this->logger->debug('Discarding TSO pool after cluster ID change');
            $this->resetPool();
        }
    }

    private function resetPool(): void
    {
        $this->timestampPool = [];
        $this->poolOffset = 0;
        $this->poolFetchedAtMs = null;
        $this->poolClusterId = null;
        $this->poolPid = null;
    }

    /**
     * Return a timestamp that is at most $lowResMaxStalenessMs old
     * (issue #420, GAP-06 low-resolution cache).
     *
     * With no staleness bound configured (default) this is equivalent to
     * a fresh {@see getTimestampBatch()} call: every call performs a fresh
     * TSO RPC. With a bound set, repeated calls within the bound reuse the
     * cached timestamp and save the PD round trip — suitable for
     * staleness-tolerant consumers such as lock resolution
     * (`CheckTxnStatus.current_ts`), never for start/commit timestamps.
     *
     * This path deliberately bypasses the pooled `getTimestamp()` (it uses
     * the raw `requestTimestampRange()`): the low-resolution contract is a
     * fresh fetch, and serving from — or discarding — the pool here would
     * perturb the start/commit timestamp stream.
     *
     * @param int|null $timeoutMs Optional gRPC call timeout in milliseconds (null = no timeout)
     *
     * @throws TiKvException when the TSO RPC fails or returns an invalid response
     */
    public function getLowResolutionTimestamp(?int $timeoutMs = null): int
    {
        if ($this->lowResMaxStalenessMs === null) {
            return $this->requestTimestampRange(1, $timeoutMs)[0];
        }

        $cachedTs = $this->lowResCachedTs;
        $cachedAtMs = $this->lowResCachedAtMs;
        if ($cachedTs !== null && $cachedAtMs !== null) {
            $ageMs = ($this->clock)() - $cachedAtMs;
            if ($ageMs >= 0 && $ageMs <= $this->lowResMaxStalenessMs) {
                return $cachedTs;
            }
        }

        $ts = $this->requestTimestampRange(1, $timeoutMs)[0];
        $this->lowResCachedTs = $ts;
        $this->lowResCachedAtMs = ($this->clock)();

        return $ts;
    }

    private function createHeader(): RequestHeader
    {
        $header = new RequestHeader();
        $clusterId = ($this->getClusterId)();
        if ($clusterId !== null) {
            $header->setClusterId($clusterId);
        }

        return $header;
    }

    /**
     * Issue the TSO RPC, retrying once on cluster-id mismatch.
     *
     * Mirrors `PdClient::callWithClusterIdRetry()` so the oracle benefits
     * from the same first-connect cluster-id discovery as the other PD
     * RPCs. The retry only fires for the "mismatch cluster id" error;
     * any other gRPC failure propagates immediately so the caller can
     * fail closed.
     */
    private function callTso(TsoRequest $request, ?int $timeoutMs): TsoResponse
    {
        try {
            $response = $this->grpc->call(
                $this->pdAddress,
                'pdpb.PD',
                'Tso',
                $request,
                TsoResponse::class,
                $timeoutMs,
            );

            $this->learnClusterId($response);

            return $response;
        } catch (GrpcException $e) {
            $extractedId = $this->extractClusterIdFromError($e->getMessage());
            if ($extractedId === null) {
                throw $e;
            }

            $this->logger->warning(
                'Cluster ID mismatch on TSO, retrying',
                ['clusterId' => $extractedId],
            );
            ($this->setClusterId)($extractedId);
            $request->setHeader($this->createHeader());

            $response = $this->grpc->call(
                $this->pdAddress,
                'pdpb.PD',
                'Tso',
                $request,
                TsoResponse::class,
                $timeoutMs,
            );

            $this->learnClusterId($response);

            return $response;
        }
    }

    private function learnClusterId(TsoResponse $response): void
    {
        if (($this->getClusterId)() !== null) {
            return;
        }

        $header = $response->getHeader();
        if ($header !== null) {
            ($this->setClusterId)((int) $header->getClusterId());
            $this->logger->info('Learned cluster ID', ['clusterId' => $header->getClusterId()]);
        }
    }

    private function extractClusterIdFromError(string $message): ?int
    {
        if (!str_contains($message, 'mismatch cluster id')) {
            return null;
        }
        if (preg_match('/need (\d+) but got/', $message, $matches)) {
            return (int) $matches[1];
        }

        return null;
    }

    /**
     * Turn a TSO grant into exactly $count consecutive timestamps.
     *
     * PD's response timestamp is the highest of the granted range
     * (client-rust `allocate_timestamps`); the handout is therefore
     * `base - (n - 1) … base` in ascending order, where
     * $n = min(granted, requested). The handout never exceeds the
     * granted count so we never fabricate timestamps outside the grant.
     *
     * @return list<int>
     */
    private function extractTimestampRange(TsoResponse $response, int $requested): array
    {
        $ts = $response->getTimestamp();
        if ($ts === null) {
            throw new TiKvException('TSO response missing timestamp');
        }

        $physical = (int) $ts->getPhysical();
        $logical = (int) $ts->getLogical();
        $base = ($physical << self::LOGICAL_SHIFT) + $logical;

        $granted = (int) $response->getCount();
        if ($granted < 1) {
            $this->logger->warning('TSO response missing count, assuming single timestamp', [
                'count' => $granted,
            ]);
            $granted = 1;
        }
        if ($granted < $requested) {
            $this->logger->warning('TSO grant smaller than requested', [
                'requested' => $requested,
                'granted' => $granted,
            ]);
        }

        $n = min($granted, $requested);
        $start = $base - ($n - 1);

        $range = [];
        for ($i = 0; $i < $n; $i++) {
            $range[] = $start + $i;
        }

        return $range;
    }
}
