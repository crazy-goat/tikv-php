<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\Proto\ImportSstpb\IngestRequest;
use CrazyGoat\Proto\ImportSstpb\IngestResponse;
use CrazyGoat\Proto\ImportSstpb\Pair;
use CrazyGoat\Proto\ImportSstpb\Range;
use CrazyGoat\Proto\ImportSstpb\RawWriteBatch;
use CrazyGoat\Proto\ImportSstpb\RawWriteRequest;
use CrazyGoat\Proto\ImportSstpb\RawWriteResponse;
use CrazyGoat\Proto\ImportSstpb\SSTMeta;
use CrazyGoat\Proto\ImportSstpb\SwitchMode;
use CrazyGoat\Proto\ImportSstpb\SwitchModeRequest;
use CrazyGoat\Proto\ImportSstpb\SwitchModeResponse;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use Psr\Log\LoggerInterface;

/**
 * Handles bulk data import via TiKV SST (Sorted String Table) Import API.
 *
 * This bypasses the normal Raft write path and directly ingests pre-sorted
 * data into TiKV regions, achieving much higher throughput for large data loads.
 */
final readonly class SstIngestor
{
    private const IMPORT_SST_SERVICE = 'import_sstpb.ImportSST';
    private const INGEST_CHUNK_SIZE = 1024;

    public function __construct(
        private GrpcClientInterface $grpc,
        private PdClientInterface $pdClient,
        private RegionResolver $regionResolver,
        private TimeoutConfig $timeoutConfig,
        private LoggerInterface $logger,
    ) {
    }

    /**
     * Bulk-import key-value pairs into TiKV via SST ingestion.
     *
     * @param array<string, string> $keyValuePairs Key-value pairs (sorted or unsorted)
     * @param int|null $ttl Time-to-live in seconds (null = no TTL)
     *
     * @throws GrpcException On gRPC transport error
     * @throws RegionException On region error
     */
    public function ingest(array $keyValuePairs, ?int $ttl = null): void
    {
        if ($keyValuePairs === []) {
            return;
        }

        // 1. Sort input key-value pairs by key (SST requires sorted input).
        ksort($keyValuePairs, SORT_STRING);

        // 2. Get all TiKV stores from PD.
        $stores = $this->pdClient->getAllStores();

        // 3. Switch all stores to import mode, then do the work.
        //    The try/finally ensures stores are always switched back to normal
        //    mode, even if switching to import mode partially fails.
        try {
            $this->switchStoresMode($stores, SwitchMode::Import);

            // 4. Group sorted KV pairs by region.
            $pairs = $this->buildPairs($keyValuePairs);
            $grouped = $this->groupPairsByRegion($pairs);

            // 5. For each region: build SST, write, and ingest.
            foreach ($grouped as $group) {
                /** @var \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo $region */
                $region = $group['region'];
                /** @var Pair[] $regionPairs */
                $regionPairs = $group['pairs'];

                $this->ingestRegionPairs($region, $regionPairs, $ttl);
            }
        } finally {
            // 6. Always switch stores back to normal mode, even on failure.
            $this->switchStoresMode($stores, SwitchMode::Normal);
        }
    }

    /**
     * Switch all stores to the given mode.
     *
     * @param \CrazyGoat\Proto\Metapb\Store[] $stores
     */
    private function switchStoresMode(array $stores, int $mode): void
    {
        $modeName = $mode === SwitchMode::Import ? 'import' : 'normal';

        foreach ($stores as $store) {
            $address = $store->getAddress();
            if ($address === '') {
                $this->logger->warning('Skipping store with empty address for switch mode', [
                    'storeId' => $store->getId(),
                    'mode' => $modeName,
                ]);
                continue;
            }

            try {
                $request = new SwitchModeRequest();
                $request->setMode($mode);

                $this->grpc->call(
                    $address,
                    self::IMPORT_SST_SERVICE,
                    'SwitchMode',
                    $request,
                    SwitchModeResponse::class,
                    $this->timeoutConfig->ingestTimeoutMs,
                );

                $this->logger->debug('Switched store to ' . $modeName . ' mode', [
                    'storeId' => $store->getId(),
                    'address' => $address,
                ]);
            } catch (\Throwable $e) {
                $this->logger->error('Failed to switch store to ' . $modeName . ' mode', [
                    'storeId' => $store->getId(),
                    'address' => $address,
                    'exception' => $e,
                ]);

                // If we fail to switch back to normal, we should still continue
                // trying other stores. The failure to switch to import mode is
                // more critical.
                if ($mode === SwitchMode::Import) {
                    throw $e;
                }
            }
        }
    }

    /**
     * Convert key-value pairs to proto Pair messages.
     *
     * @param array<string, string> $keyValuePairs
     * @return Pair[]
     */
    private function buildPairs(array $keyValuePairs): array
    {
        $pairs = [];
        foreach ($keyValuePairs as $key => $value) {
            $pair = new Pair();
            $pair->setKey($key);
            $pair->setValue($value);
            $pair->setOp(Pair\OP::Put);
            $pairs[] = $pair;
        }

        return $pairs;
    }

    /**
     * Group pairs by region using batch resolution.
     *
     * @param Pair[] $pairs
     * @return array<int, array{region: \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo, pairs: Pair[]}>
     */
    private function groupPairsByRegion(array $pairs): array
    {
        // Extract keys from pairs for region resolution.
        $keys = [];
        foreach ($pairs as $pair) {
            $keys[] = $pair->getKey();
        }

        $resolvedRegions = $this->regionResolver->batchResolveRegions($keys);

        $grouped = [];
        foreach ($pairs as $pair) {
            $key = $pair->getKey();
            $region = $resolvedRegions[$key] ?? null;
            if ($region === null) {
                continue;
            }

            $regionId = $region->regionId;
            if (!isset($grouped[$regionId])) {
                $grouped[$regionId] = ['region' => $region, 'pairs' => []];
            }
            $grouped[$regionId]['pairs'][] = $pair;
        }

        return $grouped;
    }

    /**
     * Write pairs for a single region via SST Write RPC and ingest.
     *
     * @param Pair[] $pairs
     *
     * @throws GrpcException
     * @throws RegionException
     */
    private function ingestRegionPairs(
        \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo $region,
        array $pairs,
        ?int $ttl,
    ): void {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        // Build the SST metadata.
        $uuid = $this->generateUuid();
        $range = new Range();
        $firstKey = $pairs[0]->getKey();
        $lastPair = end($pairs);
        /** @var Pair $lastPair */
        $lastKey = $lastPair->getKey();
        $range->setStart($firstKey);
        $range->setEnd($lastKey);

        $regionEpoch = new \CrazyGoat\Proto\Metapb\RegionEpoch();
        $regionEpoch->setConfVer($region->epochConfVer);
        $regionEpoch->setVersion($region->epochVersion);

        $totalBytes = 0;
        foreach ($pairs as $pair) {
            $totalBytes += strlen($pair->getKey()) + strlen($pair->getValue());
        }

        $sstMeta = new SSTMeta();
        $sstMeta->setUuid($uuid);
        $sstMeta->setRange($range);
        $sstMeta->setCfName('default');
        $sstMeta->setRegionId($region->regionId);
        $sstMeta->setRegionEpoch($regionEpoch);
        $sstMeta->setEndKeyExclusive(false);
        $sstMeta->setTotalKvs(count($pairs));
        $sstMeta->setTotalBytes($totalBytes);
        $sstMeta->setCrc32($this->computePairsCrc32($pairs));

        // Build the streaming requests: first a meta request, then batched data.
        $requests = [];

        // First chunk: SST metadata.
        $metaRequest = new RawWriteRequest();
        $metaRequest->setMeta($sstMeta);
        $requests[] = $metaRequest;

        // Subsequent chunks: data batches.
        $chunkPairs = array_chunk($pairs, self::INGEST_CHUNK_SIZE);
        foreach ($chunkPairs as $chunk) {
            $batch = new RawWriteBatch();
            $batch->setPairs($chunk);
            if ($ttl !== null && $ttl > 0) {
                $batch->setTtl($ttl);
            }

            $batchRequest = new RawWriteRequest();
            $batchRequest->setBatch($batch);
            $requests[] = $batchRequest;
        }

        // Send via client-streaming RPC.
        /** @var RawWriteResponse $writeResponse */
        $writeResponse = $this->grpc->callStreaming(
            $address,
            self::IMPORT_SST_SERVICE,
            'RawWrite',
            $requests,
            RawWriteResponse::class,
            $this->timeoutConfig->ingestTimeoutMs,
        );

        // Check write response for errors (import_sstpb.Error has getStoreError()).
        $writeError = $writeResponse->getError();
        if ($writeError !== null) {
            $msg = $writeError->getMessage();
            $storeError = $writeError->getStoreError();
            if ($storeError !== null) {
                $msg .= ': ' . json_encode($storeError, JSON_THROW_ON_ERROR);
            }
            throw new RegionException('SST Write', $msg);
        }

        // Get the resulting SST metas.
        $writtenMetas = $writeResponse->getMetas();
        if (count($writtenMetas) === 0) {
            throw new RegionException('SST Write', 'No SST metas returned from write');
        }

        // Ingest each SST.
        foreach ($writtenMetas as $writtenMeta) {
            $this->ingestSst($address, $region, $writtenMeta);
        }
    }

    /**
     * Ingest a single SST file into a region.
     *
     * @throws GrpcException
     * @throws RegionException
     */
    private function ingestSst(
        string $address,
        \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo $region,
        SSTMeta $sstMeta,
    ): void {
        $ingestRequest = new IngestRequest();
        $ingestRequest->setContext(RegionContextFactory::fromRegionInfo($region));
        $ingestRequest->setSst($sstMeta);

        /** @var IngestResponse $ingestResponse */
        $ingestResponse = $this->grpc->call(
            $address,
            self::IMPORT_SST_SERVICE,
            'Ingest',
            $ingestRequest,
            IngestResponse::class,
            $this->timeoutConfig->ingestTimeoutMs,
        );

        // IngestResponse.error is errorpb.Error (standard region error).
        $error = $ingestResponse->getError();
        if ($error !== null) {
            throw RegionException::fromRegionError($error);
        }
    }

    /**
     * Generate a 16-byte UUID for SST identification.
     *
     * @return string 16-byte binary UUID
     */
    private function generateUuid(): string
    {
        return random_bytes(16);
    }

    /**
     * Compute CRC32 checksum of the pairs.
     *
     * @param Pair[] $pairs
     */
    private function computePairsCrc32(array $pairs): int
    {
        $ctx = hash_init('crc32b');
        foreach ($pairs as $pair) {
            hash_update($ctx, $pair->getKey());
            hash_update($ctx, $pair->getValue());
        }

        return (int) hash_final($ctx);
    }
}
