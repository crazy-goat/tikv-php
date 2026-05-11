<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\Proto\Pdpb\RequestHeader;
use CrazyGoat\Proto\Pdpb\TsoRequest;
use CrazyGoat\Proto\Pdpb\TsoResponse;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class TimestampOracle
{
    private int $lastPhysical = 0;
    private int $lastLogical = 0;

    public function __construct(
        private readonly GrpcClientInterface $grpc,
        private readonly string $pdAddress,
        private readonly PdClientInterface $pdClient,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }

    public function getTimestamp(): int
    {
        $header = new RequestHeader();
        if ($this->pdClient instanceof PdClient) {
            $clusterId = $this->pdClient->getClusterId();
            if ($clusterId !== null) {
                $header->setClusterId($clusterId);
            }
        }

        $request = new TsoRequest();
        $request->setHeader($header);
        $request->setCount(1);

        try {
            $response = $this->grpc->call(
                $this->pdAddress,
                'pdpb.PD',
                'Tso',
                $request,
                TsoResponse::class,
            );

            return $this->extractTimestamp($response);
        } catch (GrpcException $e) {
            $this->logger->warning('TSO request failed, using local fallback', [
                'error' => $e->getMessage(),
            ]);

            return $this->getTimestampFallback();
        }
    }

    private function extractTimestamp(TsoResponse $response): int
    {
        $ts = $response->getTimestamp();
        if ($ts === null) {
            throw new TiKvException('TSO response missing timestamp');
        }

        $physical = (int) $ts->getPhysical();
        $logical = (int) $ts->getLogical();

        $this->lastPhysical = $physical;
        $this->lastLogical = $logical;

        return $this->composeTs($physical, $logical);
    }

    private function composeTs(int $physical, int $logical): int
    {
        return ($physical << 18) | $logical;
    }

    private function getTimestampFallback(): int
    {
        $physicalMs = (int) (hrtime(true) / 1_000_000);
        $ts = ($physicalMs << 18);

        if ($ts <= $this->lastPhysical << 18) {
            $this->lastLogical++;
            $ts = $this->composeTs($this->lastPhysical, $this->lastLogical);
        }

        return $ts;
    }
}
