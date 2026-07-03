<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawCASRequest;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
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
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
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

            $response = $this->measure('write', $key, fn(): RawCASResponse => $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawCompareAndSwap',
                $request,
                RawCASResponse::class,
                $this->timeoutConfig->writeTimeoutMs,
            ));
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
        });
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
                    'key' => \CrazyGoat\TiKV\Client\Util\KeyRedactor::redact($key),
                    'duration_ms' => round($durationMs, 2),
                    'threshold_ms' => $threshold,
                ]);
            }
        }
    }
}
