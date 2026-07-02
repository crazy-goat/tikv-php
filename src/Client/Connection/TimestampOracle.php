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

final readonly class TimestampOracle
{
    public function __construct(
        private GrpcClientInterface $grpc,
        private string $pdAddress,
        private PdClientInterface $pdClient,
        private LoggerInterface $logger = new NullLogger(),
    ) {
    }

    /**
     * Request a monotonically increasing timestamp from PD's TSO service.
     *
     * Fails closed on TSO unavailability: a locally fabricated timestamp
     * would violate TiKV MVCC ordering (snapshot isolation / global ordering),
     * so callers must observe the failure and decide whether to retry or
     * abort the transaction.
     *
     * @throws TiKvException when the TSO RPC fails or returns an invalid response
     */
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

    private function extractTimestamp(TsoResponse $response): int
    {
        $ts = $response->getTimestamp();
        if ($ts === null) {
            throw new TiKvException('TSO response missing timestamp');
        }

        $physical = (int) $ts->getPhysical();
        $logical = (int) $ts->getLogical();

        return $this->composeTs($physical, $logical);
    }

    private function composeTs(int $physical, int $logical): int
    {
        return ($physical << 18) | $logical;
    }
}
