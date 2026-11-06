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
use CrazyGoat\TiKV\Client\Batch\BatchAsyncExecutor;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\BatchPartialFailureException;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\IngestTooLargeException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStoreAddressException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
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
        private int $maxConcurrency = BatchAsyncExecutor::DEFAULT_MAX_CONCURRENCY,
        /**
         * Maximum payload bytes (keys plus values) of one ingest() call;
         * 0 = unlimited (issue #297). The guard runs before any RPC, so an
         * oversized import is rejected instead of buffered into protobuf
         * messages and run out of memory mid-stream.
         */
        private int $maxIngestSize = 0,
    ) {
        if ($maxIngestSize < 0) {
            throw new InvalidArgumentException('maxIngestSize must be >= 0');
        }
    }

    /**
     * Bulk-import key-value pairs into TiKV via SST ingestion.
     *
     * @param array<string, string> $keyValuePairs Key-value pairs (sorted or unsorted)
     * @param int|null $ttl Time-to-live in seconds (null = no TTL)
     *
     * @throws GrpcException On gRPC transport error
     * @throws RegionException On region error
     * @throws InvalidStoreAddressException When PD returns a store address
     *     that fails validation (malformed or outside the allowed set)
     * @throws IngestTooLargeException when the payload exceeds
     *     `maxIngestSize`, before any RPC is issued (issue #297)
     * @throws \CrazyGoat\TiKV\Client\Exception\TiKvException when a key's
     *     region cannot be resolved. Fail-closed: `ingest()` returns void, so
     *     it transmits nothing rather than importing a subset of the pairs
     *     and reporting success (issue #187).
     */
    public function ingest(array $keyValuePairs, ?int $ttl = null): void
    {
        if ($keyValuePairs === []) {
            return;
        }

        // 0. Reject an oversized payload before buffering anything. The
        //    guard is opt-in (0 = unlimited), like the transaction limits of
        //    issue #430.
        $this->assertPayloadWithinLimit($keyValuePairs);

        // 1. Sort input key-value pairs by key (SST requires sorted input).
        ksort($keyValuePairs, SORT_STRING);

        // 2. Get all TiKV stores from PD.
        $stores = $this->pdClient->getAllStores();

        // 3. Switch all stores to import mode, then do the work.
        //    The try/finally ensures stores are always switched back to normal
        //    mode, even if switching to import mode partially fails.
        try {
            $this->switchStoresMode($stores, SwitchMode::Import);

            // 4. Group sorted keys by region. Only keys are materialised
            //    here; the values stay in the caller's array and the Pair
            //    messages are built one chunk at a time while streaming
            //    (issue #297).
            $grouped = $this->groupKeysByRegion($keyValuePairs);

            // 5. For each region: build SST, stream the write, and ingest.
            foreach ($grouped as $group) {
                /** @var \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo $region */
                $region = $group['region'];
                /** @var string[] $regionKeys */
                $regionKeys = $group['keys'];

                $this->ingestRegionKeys($region, $regionKeys, $keyValuePairs, $ttl);
            }
        } finally {
            // 6. Always switch stores back to normal mode, even on failure.
            $this->switchStoresMode($stores, SwitchMode::Normal);
        }
    }

    /**
     * @param array<string, string> $keyValuePairs
     *
     * @throws IngestTooLargeException
     */
    private function assertPayloadWithinLimit(array $keyValuePairs): void
    {
        if ($this->maxIngestSize <= 0) {
            return;
        }

        $bytes = 0;
        foreach ($keyValuePairs as $key => $value) {
            // PHP coerces integer-like string keys ("12345", "0") to int
            // array keys (issue #322).
            $bytes += strlen((string) $key) + strlen($value);
        }

        if ($bytes > $this->maxIngestSize) {
            throw IngestTooLargeException::payloadSize($bytes, $this->maxIngestSize);
        }
    }

    /**
     * Switch all stores to the given mode.
     *
     * Every store's SwitchMode request is dispatched before any response is
     * awaited (at most $maxConcurrency requests in flight), so the pure
     * overhead of an ingest on a many-store cluster no longer scales
     * linearly with the store count (issue #295).
     *
     * Failure semantics match the previous sequential loop: a failure while
     * switching to import mode aborts on the first failing store (the
     * original exception type is preserved), while failures while restoring
     * normal mode are logged and never mask the outcome of the ingest.
     *
     * @param \CrazyGoat\Proto\Metapb\Store[] $stores
     * @throws \Throwable The first underlying failure when importing; the
     *                    exception type matches what the sequential loop
     *                    used to surface (GrpcException, InvalidStoreAddressException, ...)
     */
    private function switchStoresMode(array $stores, int $mode): void
    {
        $modeName = $mode === SwitchMode::Import ? 'import' : 'normal';
        $isImport = $mode === SwitchMode::Import;

        $calls = [];
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
                $this->regionResolver->validateStoreAddress($address, (int) $store->getId());
            } catch (\Throwable $e) {
                $this->logger->error('Failed to switch store to ' . $modeName . ' mode', [
                    'storeId' => $store->getId(),
                    'address' => $address,
                    'exception' => $e,
                ]);

                if ($isImport) {
                    // Import mode: abort before any RPC leaves the client,
                    // exactly like the sequential loop did.
                    throw $e;
                }

                continue;
            }

            $storeId = (int) $store->getId();
            $calls[$storeId] = fn(): GrpcFuture => $this->grpc->callAsync(
                $address,
                self::IMPORT_SST_SERVICE,
                'SwitchMode',
                $this->buildSwitchModeRequest($mode),
                SwitchModeResponse::class,
                $this->timeoutConfig->ingestTimeoutMs,
            );
        }

        if ($calls === []) {
            return;
        }

        try {
            $this->createBatchExecutor()->executeParallelCapped(
                $calls,
                $this->maxConcurrency,
                $this->timeoutConfig->batchDeadlineMs,
            );
        } catch (BatchPartialFailureException $e) {
            foreach ($e->getRegionErrors() as $storeId => $error) {
                $this->logger->error('Failed to switch store to ' . $modeName . ' mode', [
                    'storeId' => $storeId,
                    'exception' => $error,
                ]);
            }

            if ($isImport) {
                // Preserve the original exception type for import mode:
                // rethrow the first underlying error instead of the wrapper.
                // The executor only throws with a non-empty error map.
                $regionErrors = $e->getRegionErrors();
                $firstKey = array_key_first($regionErrors);
                if ($firstKey === null) {
                    return;
                }
                /** @var \CrazyGoat\TiKV\Client\Exception\TiKvException $firstError */
                $firstError = $regionErrors[$firstKey];

                throw $firstError;
            }
        }

        foreach (array_keys($calls) as $storeId) {
            $this->logger->debug('Switched store to ' . $modeName . ' mode', [
                'storeId' => $storeId,
            ]);
        }
    }

    private function buildSwitchModeRequest(int $mode): SwitchModeRequest
    {
        $request = new SwitchModeRequest();
        $request->setMode($mode);

        return $request;
    }

    private function createBatchExecutor(): BatchAsyncExecutor
    {
        return new BatchAsyncExecutor($this->logger);
    }

    /**
     * Group sorted keys by region using batch resolution. Only the keys are
     * copied; the values stay in the caller's map and are read lazily while
     * streaming (issue #297).
     *
     * @param array<string, string> $keyValuePairs
     * @return array<int, array{region: \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo, keys: string[]}>
     */
    private function groupKeysByRegion(array $keyValuePairs): array
    {
        $keys = [];
        foreach ($keyValuePairs as $key => $_) {
            // PHP coerces integer-like string keys ("12345", "0") to int
            // array keys; string-cast before passing to the resolver and the
            // proto setter (issue #322).
            $keys[] = (string) $key;
        }

        $resolvedRegions = $this->regionResolver->batchResolveRegions($keys);

        $grouped = [];
        foreach ($keys as $key) {
            // Fails closed rather than skipping: ingest() returns void, so
            // dropping a key would report a successful bulk import that
            // never wrote it (issue #187).
            $region = RegionGrouper::resolvedRegion($resolvedRegions, $key);

            $regionId = $region->regionId;
            $grouped[$regionId] ??= ['region' => $region, 'keys' => []];
            $grouped[$regionId]['keys'][] = $key;
        }

        return $grouped;
    }

    /**
     * Write the keys of a single region via SST Write RPC and ingest.
     *
     * The data chunks are yielded on demand from a generator, so at most one
     * chunk's `Pair` messages exist at a time instead of a full copy of the
     * payload (issue #297).
     *
     * @param string[] $keys
     * @param array<string, string> $keyValuePairs
     *
     * @throws GrpcException
     * @throws RegionException
     */
    private function ingestRegionKeys(
        \CrazyGoat\TiKV\Client\Region\Dto\RegionInfo $region,
        array $keys,
        array $keyValuePairs,
        ?int $ttl,
    ): void {
        $address = $this->regionResolver->resolveStoreAddress($region->leaderStoreId);

        // Build the SST metadata.
        $uuid = $this->generateUuid();
        $range = new Range();
        $range->setStart($keys[0]);
        $range->setEnd($keys[count($keys) - 1]);

        $regionEpoch = new \CrazyGoat\Proto\Metapb\RegionEpoch();
        $regionEpoch->setConfVer($region->epochConfVer);
        $regionEpoch->setVersion($region->epochVersion);

        $totalBytes = 0;
        foreach ($keys as $key) {
            $totalBytes += strlen($key) + strlen($keyValuePairs[$key]);
        }

        $sstMeta = new SSTMeta();
        $sstMeta->setUuid($uuid);
        $sstMeta->setRange($range);
        $sstMeta->setCfName('default');
        $sstMeta->setRegionId($region->regionId);
        $sstMeta->setRegionEpoch($regionEpoch);
        $sstMeta->setEndKeyExclusive(false);
        $sstMeta->setTotalKvs(count($keys));
        $sstMeta->setTotalBytes($totalBytes);
        $sstMeta->setCrc32($this->computeCrc32($keys, $keyValuePairs));

        // First request: SST metadata. Subsequent requests are built as the
        // stream consumes the generator, one chunk at a time.
        $requests = $this->buildStreamingRequests($sstMeta, $keys, $keyValuePairs, $ttl);

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
     * Build the RawWrite request stream lazily.
     *
     * The generator yields the metadata request first, then one data batch
     * per {@see self::INGEST_CHUNK_SIZE} keys. Each chunk's `Pair` messages
     * are released as soon as the stream asks for the next request, so the
     * payload is not held as a full `Pair[]` array and a full request array
     * at the same time (issue #297).
     *
     * @param string[] $keys
     * @param array<string, string> $keyValuePairs
     * @return \Generator<int, RawWriteRequest>
     */
    private function buildStreamingRequests(
        SSTMeta $sstMeta,
        array $keys,
        array $keyValuePairs,
        ?int $ttl,
    ): \Generator {
        $metaRequest = new RawWriteRequest();
        $metaRequest->setMeta($sstMeta);
        yield $metaRequest;

        $count = count($keys);
        for ($offset = 0; $offset < $count; $offset += self::INGEST_CHUNK_SIZE) {
            $pairs = [];
            $end = min($offset + self::INGEST_CHUNK_SIZE, $count);
            for ($i = $offset; $i < $end; $i++) {
                $key = $keys[$i];
                $pair = new Pair();
                $pair->setKey($key);
                $pair->setValue($keyValuePairs[$key]);
                $pair->setOp(Pair\OP::Put);
                $pairs[] = $pair;
            }

            $batch = new RawWriteBatch();
            $batch->setPairs($pairs);
            if ($ttl !== null && $ttl > 0) {
                $batch->setTtl($ttl);
            }

            $batchRequest = new RawWriteRequest();
            $batchRequest->setBatch($batch);
            yield $batchRequest;
        }
    }

    /**
     * Compute CRC32 checksum over the region's keys and values in order.
     *
     * @param string[] $keys
     * @param array<string, string> $keyValuePairs
     */
    private function computeCrc32(array $keys, array $keyValuePairs): int
    {
        $ctx = hash_init('crc32b');
        foreach ($keys as $key) {
            hash_update($ctx, $key);
            hash_update($ctx, $keyValuePairs[$key]);
        }

        return (int) hash_final($ctx);
    }
}
