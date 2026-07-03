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
    /**
     * @param ClusterIdHolder $clusterIdHolder Shared mutable cluster ID holder,
     *        used instead of a PdClientInterface reference to avoid the
     *        PdClient → TimestampOracle → PdClient reference cycle.
     */
    public function __construct(
        private GrpcClientInterface $grpc,
        private string $pdAddress,
        private ClusterIdHolder $clusterIdHolder,
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
        $request = new TsoRequest();
        $request->setHeader($this->createHeader());
        $request->setCount(1);

        try {
            $response = $this->callTso($request);

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

    private function createHeader(): RequestHeader
    {
        $header = new RequestHeader();
        $clusterId = $this->clusterIdHolder->get();
        if ($clusterId !== null) {
            $header->setClusterId($clusterId);
        }

        return $header;
    }

    /**
     * Issue the TSO RPC, retrying once on cluster-id mismatch.
     *
     * Mirrors `PdClient::callWithClusterIdRetry()` so the oracle benefits
     * from the same first-connect cluster-id discovery as the other PD
     * RPCs. The retry only fires for the "mismatch cluster id" error;
     * any other gRPC failure propagates immediately so the caller can
     * fail closed.
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

            $this->logger->warning(
                'Cluster ID mismatch on TSO, retrying',
                ['clusterId' => $extractedId],
            );
            $this->clusterIdHolder->set($extractedId);
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
        if ($this->clusterIdHolder->get() !== null) {
            return;
        }

        $header = $response->getHeader();
        if ($header !== null) {
            $this->clusterIdHolder->set((int) $header->getClusterId());
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

        return $this->composeTs($physical, $logical);
    }

    private function composeTs(int $physical, int $logical): int
    {
        return ($physical << 18) | $logical;
    }
}
