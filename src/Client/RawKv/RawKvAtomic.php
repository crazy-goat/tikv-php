<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawCASRequest;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\TiKV\Client\Exception\CasIndeterminateException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcStatusCode;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\Retry\ErrorClassifier;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class RawKvAtomic
{
    public function __construct(
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
        private LoggerInterface $logger = new NullLogger(),
        private ?SlowLogConfig $slowLogConfig = null,
        /** Optional Context control fields applied to every request (issue #426). */
        private ?RequestContextOptions $requestContext = null,
    ) {
    }

    public function compareAndSwap(
        string $key,
        ?string $expectedValue,
        string $newValue,
        int $ttl,
        RetryExecutor $retryExecutor,
        string $columnFamily = '',
    ): CasResult {
        // CAS (and therefore putIfAbsent) is not idempotent: if the server
        // applied the write and the response was lost, retrying on a
        // transport error would re-send the CAS against the new state and
        // report a false failure (issue #239). Region errors (NotLeader,
        // EpochNotMatch, …) are rejected *before* the write is proposed and
        // remain safe to retry. A transport error on the RawCompareAndSwap
        // RPC itself leaves the outcome indeterminate; it is wrapped in
        // CasIndeterminateException at the call site, with the GrpcException
        // preserved as `previous`.
        //
        // A GrpcException from region or store resolution (PD unreachable)
        // is definitive — the request was never sent — so it is rethrown
        // unchanged instead of being reported as an indeterminate swap.
        $classifier = static function (TiKvException $e): ?BackoffType {
            if ($e instanceof GrpcException) {
                throw $e;
            }

            return ErrorClassifier::classify($e);
        };

        return $retryExecutor->execute($key, function () use (
            $key,
            $expectedValue,
            $newValue,
            $ttl,
            $columnFamily,
        ): CasResult {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawCASRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region, requestContext: $this->requestContext));
            $request->setKey($key);
            $request->setValue($newValue);

            if ($expectedValue === null) {
                $request->setPreviousNotExist(true);
            } else {
                $request->setPreviousNotExist(false);
                $request->setPreviousValue($expectedValue);
            }

            if ($ttl > 0) {
                $request->setTtl($ttl);
            }
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            try {
                $response = $this->measure('write', $key, fn(): RawCASResponse => $this->grpc->call(
                    $address,
                    'tikvpb.Tikv',
                    'RawCompareAndSwap',
                    $request,
                    RawCASResponse::class,
                    $this->timeoutConfig->writeTimeoutMs,
                ));
            } catch (GrpcException $e) {
                if (!$this->isIndeterminateTransportFailure($e)) {
                    // The server rejected the request before applying it (bad
                    // credentials, invalid argument, resource exhaustion, …):
                    // the swap definitively did not happen.
                    throw $e;
                }

                // The request may have been applied and its response lost, so
                // the outcome is indeterminate. Do not retry, and do not let
                // a bare transport exception look like a definitive failure.
                throw new CasIndeterminateException(
                    sprintf(
                        'RawCompareAndSwap failed at the transport level for key %s;'
                        . ' the swap outcome is indeterminate. Re-read the key to'
                        . ' determine whether the write was applied',
                        KeyRedactor::redact($key),
                    ),
                    $e,
                );
            }
            /** @var RawCASResponse $response */
            RegionErrorHandler::check($response);

            $error = $response->getError();
            if ($error !== '') {
                throw new RegionException('RawCompareAndSwap', $error);
            }

            return new CasResult(
                swapped: $response->getSucceed(),
                previousValue: $response->getPreviousNotExist() ? null : $response->getPreviousValue(),
            );
        }, $classifier);
    }

    /**
     * Whether a gRPC failure on the CAS request leaves the outcome unknown.
     *
     * A status that rejects the request before it is applied is definitive:
     * the swap did not happen, and reporting it as indeterminate would push
     * the caller into an unnecessary re-read (or, worse, a lock owner to
     * believe it may hold a lock it never acquired). The remaining statuses —
     * a lost response, a deadline, a reset connection, an internal or unknown
     * error, an unrecognised code — can arrive after the server applied the
     * write, so they are indeterminate.
     */
    private function isIndeterminateTransportFailure(GrpcException $e): bool
    {
        $status = GrpcStatusCode::tryFrom($e->grpcStatusCode);

        return !in_array(
            $status,
            [
                GrpcStatusCode::InvalidArgument,
                GrpcStatusCode::NotFound,
                GrpcStatusCode::AlreadyExists,
                GrpcStatusCode::PermissionDenied,
                GrpcStatusCode::ResourceExhausted,
                GrpcStatusCode::FailedPrecondition,
                GrpcStatusCode::OutOfRange,
                GrpcStatusCode::Unimplemented,
                GrpcStatusCode::Unauthenticated,
            ],
            true,
        );
    }

    /**
     * Measure the execution time of a callable and log a warning if it
     * exceeds the configured threshold for the given operation type.
     *
     * @template T
     * @param callable(): T $fn
     * @return T
     */
    private function measure(string $operation, string $key, callable $fn): mixed
    {
        if (!$this->slowLogConfig instanceof SlowLogConfig) {
            return $fn();
        }

        $threshold = $this->slowLogConfig->getThreshold($operation);
        if ($threshold <= 0) {
            return $fn();
        }

        $start = hrtime(true);
        try {
            return $fn();
        } finally {
            $durationMs = (hrtime(true) - $start) / 1_000_000;
            if ($durationMs > $threshold) {
                $this->logger->warning('Slow TiKV operation', [
                    'operation' => $operation,
                    'key' => KeyRedactor::redact($key),
                    'duration_ms' => round($durationMs, 2),
                    'threshold_ms' => $threshold,
                ]);
            }
        }
    }
}
