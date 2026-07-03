<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch;

use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class BatchAsyncExecutor
{
    /**
     * @param LoggerInterface $logger PSR-3 logger
     */
    public function __construct(
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    /**
     * Execute multiple callables concurrently and return results.
     *
     * Concurrency model:
     *   - Dispatch phase: every callable is invoked once to obtain a value.
     *     When the callable returns an un-waited {@see GrpcFuture} or
     *     {@see CheckedGrpcFuture}, the gRPC send completes at this point;
     *     multiple regions' RPCs are therefore in-flight simultaneously on
     *     their respective channels.
     *   - Wait phase: each value is awaited in declaration order and results
     *     collected. Because every send has already been issued, server-side
     *     latencies for distinct regions overlap with each other.
     *   - Cancel phase: on the first wait-phase failure, the loop short-
     *     circuits and every still-unfinished future is cancelled to avoid
     *     leaking completion-queue/channel resources.
     *
     * Errors raised during the dispatch phase are accumulated; on the first
     * wait-phase failure the executor stops awaiting further callables;
     * either kind of failure is reported via
     * {@see BatchPartialFailureException}.
     *
     * @param array<int, callable(): mixed> $regionCalls regionId => callable
     *                                                   returning GrpcFuture,
     *                                                   CheckedGrpcFuture, or
     *                                                   a direct value
     * @param int                            $deadlineMs Wall-clock deadline in
     *                                                   milliseconds for
     *                                                   dispatch+wait; 0
     *                                                   disables the deadline.
     *                                                   When exceeded a
     *                                                   {@see BatchDeadlineExceededException}
     *                                                   is thrown after
     *                                                   cancelling any
     *                                                   in-flight futures.
     * @return array<int, mixed>
     * @throws BatchPartialFailureException   If any region fails
     * @throws BatchDeadlineExceededException If $deadlineMs > 0 and dispatch+wait exceeds it
     */
    public function executeParallel(array $regionCalls, int $deadlineMs = 0): array
    {
        $totalRegions = count($regionCalls);

        $this->logger->debug('Starting parallel batch execution', [
            'totalRegions' => $totalRegions,
            'deadlineMs' => $deadlineMs,
        ]);

        $startTimeMs = $deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;

        // Dispatch phase: invoke every callable to obtain its value. Stops
        // issuing further sends once the deadline is exhausted so the wait
        // phase doesn't extend the wall-clock budget indefinitely.
        $futures = [];
        $errors = [];
        foreach ($regionCalls as $regionId => $callable) {
            if ($deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $deadlineMs) {
                    $this->logger->error('Batch deadline exhausted during dispatch', [
                        'regionId' => $regionId,
                        'elapsedMs' => $elapsedMs,
                        'deadlineMs' => $deadlineMs,
                    ]);
                    $this->cancelAll($futures);
                    throw new BatchDeadlineExceededException($deadlineMs, $elapsedMs, [
                        'dispatchedRegions' => array_keys($futures),
                        'pendingRegions' => array_keys(array_diff_key($regionCalls, $futures)),
                    ]);
                }
            }

            try {
                $futures[$regionId] = $callable();
            } catch (TiKvException $e) {
                $errors[$regionId] = $e;
                $this->logger->warning('Region failed during dispatch', [
                    'regionId' => $regionId,
                    'error' => $e->getMessage(),
                ]);
            }
        }

        // Wait phase: await every value, in declaration order. On the first
        // failure the loop short-circuits and cancels remaining in-flight
        // futures to prevent gRPC channel/completion-queue leaks.
        $results = [];
        foreach ($futures as $regionId => $future) {
            if ($deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $deadlineMs) {
                    $this->logger->error('Batch deadline exhausted during wait', [
                        'regionId' => $regionId,
                        'elapsedMs' => $elapsedMs,
                        'deadlineMs' => $deadlineMs,
                    ]);
                    $this->cancelAll($futures);
                    throw new BatchDeadlineExceededException($deadlineMs, $elapsedMs, [
                        'pendingRegions' => array_keys(array_diff_key($futures, $results, $errors)),
                    ]);
                }
            }

            try {
                $results[$regionId] = $this->awaitValue($future);
                $this->logger->debug('Region completed successfully', ['regionId' => $regionId]);
            } catch (TiKvException $e) {
                $errors[$regionId] = $e;
                $this->logger->warning('Region failed', [
                    'regionId' => $regionId,
                    'error' => $e->getMessage(),
                ]);
                // Cancel any remaining un-waited futures so their pending
                // gRPC calls do not leak completion-queue/channel resources.
                $this->cancelAll($futures);
                break;
            }
        }

        if ($errors !== []) {
            throw new BatchPartialFailureException($errors, $totalRegions);
        }

        $this->logger->debug('Parallel batch execution completed', [
            'totalRegions' => $totalRegions,
            'deadlineMs' => $deadlineMs,
        ]);

        return $results;
    }

    /**
     * Await a single value coming out of a per-region callable. Handles the
     * three accepted return shapes uniformly (GrpcFuture, CheckedGrpcFuture,
     * or raw response/message).
     *
     * @throws TiKvException Propagated from either the raw RPC or the
     *                       region-error check inside CheckedGrpcFuture.
     */
    private function awaitValue(mixed $value): mixed
    {
        if ($value instanceof CheckedGrpcFuture) {
            return $value->waitForExecutor();
        }

        return $value instanceof GrpcFuture ? $value->wait() : $value;
    }

    /**
     * Cancel every still-unfinished future in the given map.
     *
     * Cancellation is best-effort: both `GrpcFuture::cancel()` and
     * `CheckedGrpcFuture::cancel()` swallow any throwable from the
     * underlying gRPC layer because cancellation may be invoked during
     * shutdown and must never propagate.
     *
     * @param array<int, mixed> $futures regionId => value (GrpcFuture, CheckedGrpcFuture, or
     *                                              direct)
     */
    private function cancelAll(array $futures): void
    {
        foreach ($futures as $remaining) {
            $inner = $remaining instanceof CheckedGrpcFuture ? $remaining->inner() : $remaining;
            if ($inner instanceof GrpcFuture && !$inner->isCompleted()) {
                $inner->cancel();
            }
        }
    }
}
