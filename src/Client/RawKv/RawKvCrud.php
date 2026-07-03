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
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;

final readonly class RawKvCrud
{
    public function __construct(
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
    ) {
    }

    public function get(string $key, RetryExecutor $retryExecutor, string $columnFamily = ''): ?string
    {
        return $retryExecutor->execute($key, function () use ($key, $columnFamily): ?string {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawGetRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKey($key);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            /** @var RawGetResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawGet',
                $request,
                RawGetResponse::class,
                $this->timeoutMs('read'),
            );
            RegionErrorHandler::check($response);

            if ($response->getNotFound()) {
                return null;
            }

            return $response->getValue();
        });
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
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
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

            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawPut',
                $request,
                RawPutResponse::class,
                $this->timeoutMs('write'),
            );
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
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKey($key);
            if ($forCas) {
                $request->setForCas(true);
            }
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawDelete',
                $request,
                RawDeleteResponse::class,
                $this->timeoutMs('write'),
            );
            RegionErrorHandler::check($response);
            return null;
        });
    }

    public function getKeyTTL(string $key, RetryExecutor $retryExecutor, string $columnFamily = ''): ?int
    {
        return $retryExecutor->execute($key, function () use ($key, $columnFamily): ?int {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawGetKeyTTLRequest();
            $request->setContext(RegionContextFactory::fromRegionInfo($region));
            $request->setKey($key);
            if ($columnFamily !== '') {
                $request->setCf($columnFamily);
            }

            /** @var RawGetKeyTTLResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawGetKeyTTL',
                $request,
                RawGetKeyTTLResponse::class,
                $this->timeoutMs('read'),
            );
            RegionErrorHandler::check($response);

            $error = $response->getError();
            if ($error !== '') {
                throw new RegionException('RawGetKeyTTL', $error);
            }

            if ($response->getNotFound()) {
                return null;
            }

            $ttl = (int) $response->getTtl();
            return $ttl > 0 ? $ttl : null;
        });
    }

    private function timeoutMs(string $operationType): ?int
    {
        return match ($operationType) {
            'read' => $this->timeoutConfig->readTimeoutMs,
            'write' => $this->timeoutConfig->writeTimeoutMs,
            default => null,
        };
    }
}
