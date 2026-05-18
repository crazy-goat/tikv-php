<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawCASRequest;
use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;

final readonly class RawKvAtomic
{
    public function __construct(
        private GrpcClientInterface $grpc,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
    ) {
    }

    public function compareAndSwap(
        string $key,
        ?string $expectedValue,
        string $newValue,
        int $ttl,
        RetryExecutor $retryExecutor,
    ): CasResult {
        return $retryExecutor->execute($key, function () use ($key, $expectedValue, $newValue, $ttl): CasResult {
            $region = $this->regionResolver->getRegionInfo($key);
            $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

            $request = new RawCASRequest();
            $request->setContext(RegionContext::fromRegionInfo($region));
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

            /** @var RawCASResponse $response */
            $response = $this->grpc->call(
                $address,
                'tikvpb.Tikv',
                'RawCompareAndSwap',
                $request,
                RawCASResponse::class,
                $this->timeoutConfig->writeTimeoutMs,
            );
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
}
