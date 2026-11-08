<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch;

use CrazyGoat\TiKV\Client\Exception\BatchDeadlineExceededException;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\DebugLogging;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class BatchAsyncExecutor
{
    /**
     * Default upper bound on simultaneously in-flight requests for the
     * windowed executor ({@see self::executeParallelCapped()}): large enough
     * to saturate a cluster on a LAN, small enough to avoid a thundering
     * herd against a single TiKV store (issue #295).
     */
    public const DEFAULT_MAX_CONCURRENCY = 16;

    /**
     * Whether this executor emits its debug records at all.
     *
     * A batch logs its start, every completed region and its end; the
     * context of a completed region is built by `logContext()` for every
     * slot. A logger that discards the record makes that work pointless, so
     * {@see DebugLogging} asks the logger once, at construction (issue
     * #715). Reconfigure the logger before the client is built — see
     * `docs/configuration.md`.
     */
    private bool $debugEnabled;

    /**
     * @param LoggerInterface $logger PSR-3 logger
     */
    public function __construct(
        private LoggerInterface $logger = new NullLogger(),
    ) {
        $this->debugEnabled = DebugLogging::handlesDebug($logger);
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
     *   - Partial failures: TiKvException failures are accumulated while the
     *     executor continues awaiting the other dispatched calls. The final
     *     {@see BatchPartialFailureException} carries both the failures and
     *     results from successful calls. Deadline and unexpected throwable
     *     exits still cancel unfinished futures.
     *
     * The deadline is checked at dispatch and wait **boundaries** (before each
     * dispatch and before each wait), never during one blocking wait. One wait
     * on a `CheckedGrpcFuture::fromRetryableDispatch()` future runs the
     * whole retry loop of one region (up to
     * `RetryExecutor::DEFAULT_RETRY_DEADLINE_MS`, plus one per-call timeout),
     * so the worst case is `$deadlineMs` plus one region's full retry loop
     * (plus the time of a callable that blocks while it dispatches, e.g. a PD
     * lookup on a cold region cache). `TimeoutConfig` rejects a batch deadline
     * that is shorter than a per-call timeout (issue #275).
     *
     * Errors raised during dispatch and wait phases are accumulated and
     * reported via {@see BatchPartialFailureException} after all calls have
     * been observed.
     *
     * @param array<int, callable(): mixed> $regionCalls fan-out slot => callable
     *                                                   returning GrpcFuture,
     *                                                   CheckedGrpcFuture, or
     *                                                   a direct value. The key
     *                                                   is an internal slot index
     *                                                   (a sub-batch/segment
     *                                                   ordinal), not a TiKV
     *                                                   region ID; it is what
     *                                                   the exception context
     *                                                   reports.
     * @param int                            $deadlineMs Wall-clock deadline in
     *                                                   milliseconds for
     *                                                   dispatch+wait; 0
     *                                                   disables the deadline
     *                                                   (explicit opt-out; the
     *                                                   default is finite).
     *                                                   When exceeded a
     *                                                   {@see BatchDeadlineExceededException}
     *                                                   is thrown after
     *                                                   cancelling any
     *                                                   in-flight futures.
     * @param array<int, BatchCallId>         $labels     fan-out slot => call
     *                                                   descriptor (issue #285,
     *                                                   GRPC-26). When supplied,
     *                                                   log context names the
     *                                                   real TiKV region ID and
     *                                                   the sub-batch index
     *                                                   instead of the slot
     *                                                   index; the exception
     *                                                   exposes them through
     *                                                   {@see BatchPartialFailureException::getFailedRegionIds()}.
     *                                                   Omitted entries fall
     *                                                   back to a `callKey`
     *                                                   log field.
     * @return array<int, mixed>
     * @throws BatchPartialFailureException   If any region fails
     * @throws BatchDeadlineExceededException If $deadlineMs > 0 and dispatch+wait exceeds it
     * @throws \RuntimeException              If a callable throws a non-TiKvException
     *                                        (e.g. a regroup signal); in-flight futures
     *                                        are cancelled first
     */
    public function executeParallel(
        array $regionCalls,
        int $deadlineMs = TimeoutConfig::DEFAULT_BATCH_DEADLINE_MS,
        array $labels = [],
    ): array {
        $totalRegions = count($regionCalls);

        if ($this->debugEnabled) {
            $this->logger->debug('Starting parallel batch execution', [
                'totalRegions' => $totalRegions,
                'deadlineMs' => $deadlineMs,
            ]);
        }

        $startTimeMs = $deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;

        // Dispatch phase: invoke every callable to obtain its value. Stops
        // issuing further sends once the deadline is exhausted so the wait
        // phase doesn't extend the wall-clock budget indefinitely.
        $futures = [];
        $errors = [];
        foreach ($regionCalls as $slot => $callable) {
            if ($deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $deadlineMs) {
                    $this->logger->error(
                        'Batch deadline exhausted during dispatch',
                        $this->logContext($slot, $labels) + [
                            'elapsedMs' => $elapsedMs,
                            'deadlineMs' => $deadlineMs,
                        ],
                    );
                    $this->cancelAll($futures);
                    throw new BatchDeadlineExceededException(
                        $deadlineMs,
                        $elapsedMs,
                        $this->deadlineContext(
                            $labels,
                            array_keys($futures),
                            array_keys(array_diff_key($regionCalls, $futures)),
                        ),
                    );
                }
            }

            try {
                $futures[$slot] = $callable();
            } catch (TiKvException $e) {
                $errors[$slot] = $e;
                $this->logger->warning('Region failed during dispatch', $this->logContext($slot, $labels) + [
                    'error' => $e->getMessage(),
                ]);
            }
        }

        // Wait phase: await every value, in declaration order. On the first
        // failure the loop short-circuits and cancels remaining in-flight
        // futures to prevent gRPC channel/completion-queue leaks.
        $results = [];
        try {
            foreach ($futures as $slot => $future) {
                if ($deadlineMs > 0) {
                    $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                    if ($elapsedMs >= $deadlineMs) {
                        $this->logger->error(
                            'Batch deadline exhausted during wait',
                            $this->logContext($slot, $labels) + [
                                'elapsedMs' => $elapsedMs,
                                'deadlineMs' => $deadlineMs,
                            ],
                        );
                        $this->cancelAll($futures);
                        throw new BatchDeadlineExceededException(
                            $deadlineMs,
                            $elapsedMs,
                            $this->deadlineContext(
                                $labels,
                                array_keys($futures),
                                array_keys(array_diff_key($futures, $results, $errors)),
                            ),
                        );
                    }
                }

                try {
                    $results[$slot] = $this->awaitValue($future);
                    if ($this->debugEnabled) {
                        $this->logger->debug('Region completed successfully', $this->logContext($slot, $labels));
                    }
                } catch (TiKvException $e) {
                    $errors[$slot] = $e;
                    $this->logger->warning('Region failed', $this->logContext($slot, $labels) + [
                        'error' => $e->getMessage(),
                    ]);
                    // Continue awaiting other in-flight calls so the partial
                    // failure can report every success and every failed
                    // sub-batch instead of discarding completed work.
                }
            }
        } catch (\Throwable $e) {
            // A non-TiKvException escape (e.g. RollbackRegroupSignal, which
            // deliberately extends \RuntimeException so it bypasses the
            // TiKvException handling above) must still cancel the in-flight
            // futures before propagating. cancelAll() is best-effort and
            // safe to run twice.
            $this->cancelAll($futures);
            throw $e;
        }

        if ($errors !== []) {
            throw new BatchPartialFailureException($errors, $totalRegions, $results, $labels);
        }

        if ($this->debugEnabled) {
            $this->logger->debug('Parallel batch execution completed', [
                'totalRegions' => $totalRegions,
                'deadlineMs' => $deadlineMs,
            ]);
        }

        return $results;
    }

    /**
     * Execute multiple callables with a bounded number of simultaneously
     * in-flight requests.
     *
     * The callables are chunked into windows of at most $maxConcurrency
     * entries and each window is executed sequentially through
     * {@see self::executeParallel()}: within a window every request is sent
     * before any wait begins, so up to $maxConcurrency RPCs are in flight
     * simultaneously; the next window is dispatched only after the current
     * one has been fully awaited. This bounds the fan-out of a single
     * operation (e.g. a deleteRange spanning hundreds of regions) instead
     * of opening an unbounded thundering herd against the cluster.
     *
     * On the first failure inside a window the underlying executor cancels
     * that window's in-flight futures and reports
     * {@see BatchPartialFailureException}; later windows are never
     * dispatched.
     *
     * @param array<int, callable(): mixed> $regionCalls fan-out slot => callable
     *                                                   returning GrpcFuture,
     *                                                   CheckedGrpcFuture, or
     *                                                   a direct value. The key
     *                                                   is an internal slot index
     *                                                   (a sub-batch/segment
     *                                                   ordinal), not a TiKV
     *                                                   region ID; it is what
     *                                                   the exception context
     *                                                   reports.
     * @param int                           $maxConcurrency maximum number of
     *                                                      in-flight requests;
     *                                                      must be >= 1
     * @param int                           $deadlineMs     wall-clock deadline
     *                                                      for dispatch+wait
     *                                                      across ALL windows;
     *                                                      0 disables it
     * @param array<int, BatchCallId>       $labels         fan-out slot => call
     *                                                      descriptor, forwarded
     *                                                      to
     *                                                      {@see self::executeParallel()}
     *                                                      (issue #285,
     *                                                      GRPC-26)
     * @return array<int, mixed> results keyed by the original fan-out slot
     * @throws BatchPartialFailureException   If any region fails
     * @throws BatchDeadlineExceededException If $deadlineMs > 0 and dispatch+wait exceeds it
     * @throws \InvalidArgumentException      If $maxConcurrency < 1
     */
    public function executeParallelCapped(
        array $regionCalls,
        int $maxConcurrency = self::DEFAULT_MAX_CONCURRENCY,
        int $deadlineMs = TimeoutConfig::DEFAULT_BATCH_DEADLINE_MS,
        array $labels = [],
    ): array {
        if ($maxConcurrency < 1) {
            throw new \InvalidArgumentException(sprintf(
                'maxConcurrency must be 1 or greater, %d given',
                $maxConcurrency,
            ));
        }

        if ($regionCalls === []) {
            return [];
        }

        $startTimeMs = $deadlineMs > 0 ? (int) (microtime(true) * 1000) : 0;
        $results = [];
        foreach (array_chunk($regionCalls, $maxConcurrency, true) as $window) {
            // One budget for ALL windows: each window gets what is left, so
            // N windows cannot stretch the batch to N times the deadline.
            $windowDeadlineMs = $deadlineMs;
            if ($deadlineMs > 0) {
                $elapsedMs = (int) (microtime(true) * 1000) - $startTimeMs;
                if ($elapsedMs >= $deadlineMs) {
                    $this->logger->error('Batch deadline exhausted between windows', [
                        'elapsedMs' => $elapsedMs,
                        'deadlineMs' => $deadlineMs,
                    ]);
                    throw $this->capDeadlineException(
                        $regionCalls,
                        $results,
                        $deadlineMs,
                        $elapsedMs,
                        array_keys($results),
                        $labels,
                    );
                }
                $windowDeadlineMs = $deadlineMs - $elapsedMs;
            }

            try {
                $windowResults = $this->executeParallel($window, $windowDeadlineMs, $labels);
            } catch (BatchDeadlineExceededException $e) {
                // The inner exception speaks for one window and for the
                // remaining budget. Report the configured deadline, the total
                // elapsed time and every region that has no result, including
                // the windows that never started.
                /** @var array<int, int> $dispatched */
                $dispatched = $e->getContext()['dispatchedRegions'] ?? array_keys($window);
                throw $this->capDeadlineException(
                    $regionCalls,
                    $results,
                    $deadlineMs,
                    (int) (microtime(true) * 1000) - $startTimeMs,
                    array_merge(array_keys($results), $dispatched),
                    $labels,
                );
            }

            foreach ($windowResults as $regionId => $result) {
                $results[$regionId] = $result;
            }
        }

        return $results;
    }

    /**
     * Build the exception for a deadline that ran out inside
     * executeParallelCapped(): the configured deadline, the total elapsed
     * time, and every region without a result as pending.
     *
     * @param array<int, callable(): mixed> $regionCalls
     * @param array<int, mixed>             $results
     * @param array<int, int>               $dispatched
     * @param array<int, BatchCallId>       $labels
     */
    private function capDeadlineException(
        array $regionCalls,
        array $results,
        int $deadlineMs,
        int $elapsedMs,
        array $dispatched,
        array $labels = [],
    ): BatchDeadlineExceededException {
        return new BatchDeadlineExceededException(
            $deadlineMs,
            $elapsedMs,
            $this->deadlineContext(
                $labels,
                $dispatched,
                array_keys(array_diff_key($regionCalls, $results)),
            ),
        );
    }

    /**
     * Log fields identifying a fan-out slot: the real region ID and
     * sub-batch index when the caller supplied a descriptor, otherwise the
     * slot under an honest `callKey` name (never a fabricated `regionId`).
     *
     * @param array<int, BatchCallId> $labels
     * @return array<string, int|string>
     */
    private function logContext(int $slot, array $labels): array
    {
        $callId = $labels[$slot] ?? null;
        if (!$callId instanceof BatchCallId) {
            return ['callKey' => $slot];
        }

        return [
            'regionId' => $callId->regionId,
            'subBatchIndex' => $callId->subBatchIndex,
        ];
    }

    /**
     * Build the diagnostic context of a deadline exception. The historical
     * `dispatchedRegions`/`pendingRegions` keys stay fan-out slots (their
     * consumers are internal and several tests pin them); when descriptors
     * are available the same context also carries the real TiKV region IDs
     * and the per-call identities, so a failure can be correlated with the
     * cluster (issue #285, GRPC-26).
     *
     * @param array<int, BatchCallId> $labels
     * @param array<int, int>         $dispatched
     * @param array<int, int>         $pending
     * @return array<string, mixed>
     */
    private function deadlineContext(array $labels, array $dispatched, array $pending): array
    {
        $context = [
            'dispatchedRegions' => array_values(array_unique($dispatched)),
            'pendingRegions' => array_values(array_unique($pending)),
        ];

        if ($labels === []) {
            return $context;
        }

        $context['dispatchedRegionIds'] = $this->regionIdsFor($labels, $dispatched);
        $context['pendingRegionIds'] = $this->regionIdsFor($labels, $pending);
        $context['dispatchedCalls'] = $this->describeCalls($labels, $dispatched);
        $context['pendingCalls'] = $this->describeCalls($labels, $pending);

        return $context;
    }

    /**
     * @param array<int, BatchCallId> $labels
     * @param array<int, int>         $slots
     * @return list<int>
     */
    private function regionIdsFor(array $labels, array $slots): array
    {
        $regionIds = [];
        foreach ($slots as $slot) {
            $callId = $labels[$slot] ?? null;
            if ($callId instanceof BatchCallId) {
                $regionIds[$callId->regionId] = true;
            }
        }

        $regionIds = array_keys($regionIds);
        sort($regionIds);

        return $regionIds;
    }

    /**
     * @param array<int, BatchCallId> $labels
     * @param array<int, int>         $slots
     * @return list<string>
     */
    private function describeCalls(array $labels, array $slots): array
    {
        $calls = [];
        foreach ($slots as $slot) {
            $callId = $labels[$slot] ?? null;
            if ($callId instanceof BatchCallId) {
                $calls[] = (string) $callId;
            }
        }

        return $calls;
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
     * @param array<int, mixed> $futures fan-out slot => value (GrpcFuture, CheckedGrpcFuture, or
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
