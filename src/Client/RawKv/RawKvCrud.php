<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawDeleteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetKeyTTLResponse;
use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use CrazyGoat\TiKV\Client\Retry\ErrorKind;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final readonly class RawKvCrud
{
    public function __construct(
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
        private LoggerInterface $logger = new NullLogger(),
        private ?SlowLogConfig $slowLogConfig = null,
        /** Read preference for reads (issue #421); writes always target the leader. */
        private ReplicaReadPolicy $replicaReadPolicy = new ReplicaReadPolicy(),
        /** Optional Context control fields applied to every request (issue #426). */
        private ?RequestContextOptions $requestContext = null,
        /** Resource-control metadata carried on every request (issue #427). */
        private ResourceControl $resourceControl = new ResourceControl(),
    ) {
    }

    public function get(string $key, RetryExecutor $retryExecutor, string $columnFamily = ''): ?string
    {
        $excludedStore = null;
        $targetStoreId = null;

        return $retryExecutor->execute(
            $key,
            function () use ($key, $columnFamily, &$excludedStore, &$targetStoreId): ?string {
                $region = $this->regionResolver->getRegionInfo($key);
                $target = RegionContextFactory::resolveTarget(
                    $region,
                    $this->replicaReadPolicy,
                    $this->regionResolver->getStore(...),
                    excludedStoreId: $excludedStore,
                    requestContext: $this->requestContext,
                    resourceControl: $this->resourceControl,
                );
                $excludedStore = null;
                $targetStoreId = $target->storeId;
                $address = $this->regionResolver->resolveStoreAddress($target->storeId);

                $request = new RawGetRequest();
                $request->setContext($target->context);
                $request->setKey($key);
                if ($columnFamily !== '') {
                    $request->setCf($columnFamily);
                }

                try {
                    $response = $this->measure('read', $key, fn(): RawGetResponse => $this->grpc->call(
                        $address,
                        'tikvpb.Tikv',
                        'RawGet',
                        $request,
                        RawGetResponse::class,
                        $this->timeoutMs('read'),
                    ));
                    /** @var RawGetResponse $response */
                    RegionErrorHandler::check($response);
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        // The selected replica's applied index is behind: exclude
                        // it so the next attempt falls back to another replica or
                        // the leader (issue #421).
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                if ($response->getNotFound()) {
                    return null;
                }

                return $response->getValue();
            },
            targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            },
        );
    }

    public function put(
        string $key,
        string $value,
        int $ttl,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): void {
        $retryExecutor->execute($key, function () use ($key, $value, $ttl, $forCas, $columnFamily): null {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawPutRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo(
                $region,
                requestContext: $this->requestContext,
                resourceControl: $this->resourceControl,
            ));
            $request->setKey($key);
            $request->setValue($value);
            if ($ttl > 0) {
                $request->setTtl($ttl);
            }
            if ($forCas) {
                $request->setForCas(true);
            }
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            $response = $this->measure('write', $key, fn(): RawPutResponse => $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawPut',
                $request,
                RawPutResponse::class,
                $this->timeoutMs('write'),
            ));
            /** @var RawPutResponse $response */
            RegionErrorHandler::check($response);

            return null;
        });
    }

    public function delete(
        string $key,
        RetryExecutor $retryExecutor,
        bool $forCas = false,
        string $columnFamily = '',
    ): void {
        $retryExecutor->execute($key, function () use ($key, $forCas, $columnFamily): null {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawDeleteRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo(
                $region,
                requestContext: $this->requestContext,
                resourceControl: $this->resourceControl,
            ));
            $request->setKey($key);
            if ($forCas) {
                $request->setForCas(true);
            }
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            $response = $this->measure('write', $key, fn(): RawDeleteResponse => $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawDelete',
                $request,
                RawDeleteResponse::class,
                $this->timeoutMs('write'),
            ));
            /** @var RawDeleteResponse $response */
            RegionErrorHandler::check($response);

            return null;
        });
    }

    public function getKeyTTL(string $key, RetryExecutor $retryExecutor, string $columnFamily = ''): ?int
    {
        $excludedStore = null;
        $targetStoreId = null;

        return $retryExecutor->execute(
            $key,
            function () use ($key, $columnFamily, &$excludedStore, &$targetStoreId): ?int {
                $region = $this->regionResolver->getRegionInfo($key);
                $target = RegionContextFactory::resolveTarget(
                    $region,
                    $this->replicaReadPolicy,
                    $this->regionResolver->getStore(...),
                    excludedStoreId: $excludedStore,
                    requestContext: $this->requestContext,
                    resourceControl: $this->resourceControl,
                );
                $excludedStore = null;
                $targetStoreId = $target->storeId;
                $address = $this->regionResolver->resolveStoreAddress($target->storeId);

                $request = new RawGetKeyTTLRequest();
                $request->setContext($target->context);
                $request->setKey($key);
                if ($columnFamily !== '') {
                    $request->setCf($columnFamily);
                }

                try {
                    $response = $this->measure('read', $key, fn(): RawGetKeyTTLResponse => $this->grpc->call(
                        $address,
                        'tikvpb.Tikv',
                        'RawGetKeyTTL',
                        $request,
                        RawGetKeyTTLResponse::class,
                        $this->timeoutMs('read'),
                    ));
                    /** @var RawGetKeyTTLResponse $response */
                    RegionErrorHandler::check($response);
                } catch (RegionException $e) {
                    if ($e->errorKind === ErrorKind::DataIsNotReady) {
                        $excludedStore = $target->storeId;
                    }
                    throw $e;
                }

                $error = $response->getError();
                if ($error !== '') {
                    throw new RegionException('RawGetKeyTTL', $error);
                }

                if ($response->getNotFound()) {
                    return null;
                }

                $ttl = (int) $response->getTtl();
                return $ttl > 0 ? $ttl : null;
            },
            targetStoreId: static function () use (&$targetStoreId): ?int {
                return $targetStoreId;
            },
        );
    }

    private function timeoutMs(string $operationType): ?int
    {
        return match ($operationType) {
            'read' => $this->timeoutConfig->readTimeoutMs,
            'write' => $this->timeoutConfig->writeTimeoutMs,
            default => null,
        };
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
