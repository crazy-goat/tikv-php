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
        $request = new TsoRequest();
        $request->setHeader($this->createHeader());
        $request->setCount(1);

        try {
            $response = $this->callTso($request);

            return $this->extractTimestamp($response);
        } catch (GrpcException $e) {
            $this->logger->warning('TSO request failed, using local fallback', [
                'error' => $e->getMessage(),
            ]);

            return $this->getTimestampFallback();
        }
    }

    private function createHeader(): RequestHeader
    {
        $header = new RequestHeader();
        if ($this->pdClient instanceof PdClient) {
            $clusterId = $this->pdClient->getClusterId();
            if ($clusterId !== null) {
                $header->setClusterId($clusterId);
            }
        }

        return $header;
    }

    /**
     * Issue the TSO RPC, retrying once on cluster-id mismatch.
     *
     * Mirrors `PdClient::callWithClusterIdRetry()` so the oracle benefits
     * from the same first-connect cluster-id discovery as the other PD
     * RPCs. The retry only fires for the "mismatch cluster id" error;
     * any other gRPC failure propagates immediately.
     */
    private function callTso(TsoRequest $request): TsoResponse
    {
        try {
            $response = $this->grpc->call(
                $this->pdAddress,
                'pdpb.PD',
                'Tso',
                $request,
                TsoResponse::class,
            );

            $this->learnClusterId($response);

            return $response;
        } catch (GrpcException $e) {
            $extractedId = $this->extractClusterIdFromError($e->getMessage());
            if ($extractedId === null) {
                throw $e;
            }

            if (!$this->pdClient instanceof PdClient) {
                throw $e;
            }

            $this->logger->warning(
                'Cluster ID mismatch on TSO, retrying',
                ['clusterId' => $extractedId],
            );
            $this->pdClient->setClusterId($extractedId);
            $request->setHeader($this->createHeader());

            $response = $this->grpc->call(
                $this->pdAddress,
                'pdpb.PD',
                'Tso',
                $request,
                TsoResponse::class,
            );

            $this->learnClusterId($response);

            return $response;
        }
    }

    private function learnClusterId(TsoResponse $response): void
    {
        if (!$this->pdClient instanceof PdClient) {
            return;
        }

        if ($this->pdClient->getClusterId() !== null) {
            return;
        }

        $header = $response->getHeader();
        if ($header !== null) {
            $this->pdClient->setClusterId((int) $header->getClusterId());
            $this->logger->info('Learned cluster ID', ['clusterId' => $header->getClusterId()]);
        }
    }

    private function extractClusterIdFromError(string $message): ?int
    {
        if (!str_contains($message, 'mismatch cluster id')) {
            return null;
        }
        if (preg_match('/need (\d+) but got/', $message, $matches)) {
            return (int) $matches[1];
        }

        return null;
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
