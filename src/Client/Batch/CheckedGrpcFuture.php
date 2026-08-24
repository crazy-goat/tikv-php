<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch;

use Closure;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use Google\Protobuf\Internal\Message;

/**
 * Lazy region-error-checking wrapper for the dispatch phase of a batch
 * operation.
 *
 * Two construction modes are supported:
 *
 *   1. {@see self::fromGrpcFuture()}: wrap an un-waited {@see GrpcFuture}.
 *      `waitForExecutor()` performs the underlying wait + a region-error
 *      check on the resolved protobuf message. The wrapper itself does not
 *      touch the gRPC channel during dispatch, which is the property that
 *      makes true client-side fan-out possible: the per-region callables
 *      can issue every send before any wait begins.
 *
 *   2. {@see self::fromCallable()}: wrap a callable that already has run
 *      (or will run) arbitrary work and returns a resolved protobuf
 *      message. This is used by the multi-region split/merge path inside
 *      {@see \CrazyGoat\TiKV\Client\RawKv\RawKvBatch}, which issues sends
 *      for every sub-region up-front and then merges their responses
 *      during the wait phase.
 *
 * The executor never sees the raw protobuf response without first
 * running {@see RegionErrorHandler::check()} on it.
 */
final readonly class CheckedGrpcFuture
{
    /**
     * @param callable(): mixed $waiter         Resolves the future to its
     *                                          result (a protobuf message,
     *                                          or an aggregated value for
     *                                          the batchScan fan-out)
     * @param bool               $hasInnerFuture True if there is a single
     *                                          underlying {@see GrpcFuture}
     *                                          that exposes cancel/isCompleted
     *                                          semantics; false for synthetic
     *                                          or no-op transporters.
     */
    private function __construct(
        private mixed $inner,
        private bool $hasInnerFuture,
        private mixed $waiter,
    ) {
    }

    /**
     * Wrap a real, un-waited gRPC future. The wrapper waits on it and runs
     * {@see RegionErrorHandler::check()} on the resolved response.
     */
    public static function fromGrpcFuture(GrpcFuture $future): self
    {
        return new self(
            inner: $future,
            hasInnerFuture: true,
            waiter: static function () use ($future): Message {
                $response = $future->wait();
                RegionErrorHandler::check($response);
                return $response;
            },
        );
    }

    /**
     * Wrap a callable that, on first invocation, returns the resolved
     * result. Most often this is a protobuf Message; the batchScan fan-out
     * (issue #295) instead aggregates several sub-futures into the final
     * plain-PHP row array during the wait, so any fully-resolved value is
     * accepted — the executor passes it through untouched.
     *
     * @param callable(): mixed $waiter
     */
    public static function fromCallable(callable $waiter): self
    {
        return new self(
            inner: null,
            hasInnerFuture: false,
            waiter: Closure::fromCallable($waiter),
        );
    }

    /**
     * Underlying gRPC future, when present. Exposed so the executor can
     * route cancellation correctly without bypassing the region-error check.
     */
    public function inner(): ?GrpcFuture
    {
        if ($this->hasInnerFuture && $this->inner instanceof GrpcFuture) {
            return $this->inner;
        }
        return null;
    }

    /**
     * Resolve the future. Returns the underlying protobuf message for real
     * RPCs, or whatever aggregated result the waiter produced for synthetic
     * futures (the batchScan fan-out aggregates its segments into a plain
     * row array, see RawKvScanner::scanRangeAsync()).
     *
     * @throws TiKvException If either the underlying RPC fails or the
     *                       resolved response carries a region/key error.
     */
    public function waitForExecutor(): mixed
    {
        return ($this->waiter)();
    }

    public function isCompleted(): bool
    {
        // Synthetic callables (fromCallable without an inner GrpcFuture)
        // never perform I/O, so they are vacuously "completed" for the
        // purpose of duplicate-cancellation guards.
        if (!$this->hasInnerFuture) {
            return true;
        }
        return $this->inner instanceof GrpcFuture && $this->inner->isCompleted();
    }

    /**
     * Cancel the underlying gRPC future, if any. Best-effort: never throws.
     */
    public function cancel(): void
    {
        if ($this->hasInnerFuture && $this->inner instanceof GrpcFuture && !$this->inner->isCompleted()) {
            $this->inner->cancel();
        }
    }
}
