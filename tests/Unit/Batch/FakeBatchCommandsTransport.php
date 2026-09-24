<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsTransportInterface;

/**
 * Records every exchange and answers it with a scripted responder.
 * The default responder parses the outer request's inner RawGet requests
 * and echoes each key back as the RawGetResponse value, delivering the
 * responses in REVERSED request_id order to exercise out-of-order
 * correlation.
 */
final class FakeBatchCommandsTransport implements BatchCommandsTransportInterface
{
    /** @var list<array{address: string, ids: list<int>, keysByRequest: list<list<string>>}> */
    public array $exchanges = [];

    public function __construct(
        /** @var \Closure(string, BatchCommandsRequest, list<int>): list<BatchCommandsResponse>|null */
        private readonly \Closure|null $responder = null,
        public \Throwable|null $throwOnCall = null,
    ) {
    }

    /**
     * @param list<int> $requestIds
     * @return list<BatchCommandsResponse>
     */
    public function exchange(
        string $address,
        BatchCommandsRequest $request,
        array $requestIds,
        int $timeoutMs,
    ): array {
        if ($this->throwOnCall instanceof \Throwable) {
            throw $this->throwOnCall;
        }

        $keysByRequest = [];
        $shapeByRequest = [];
        foreach ($request->getRequests() as $oneof) {
            $keys = static fn (): array => [];
            $rawGet = $oneof->getRawGet();
            if ($rawGet !== null) {
                $keysByRequest[] = [$rawGet->getKey()];
                $shapeByRequest[] = 'rawGet';
                continue;
            }
            $rawBatchGet = $oneof->getRawBatchGet();
            if ($rawBatchGet !== null) {
                /** @var list<string> $batchKeys */
                $batchKeys = iterator_to_array($rawBatchGet->getKeys());
                $keysByRequest[] = $batchKeys;
                $shapeByRequest[] = 'rawBatchGet';
                continue;
            }
            $rawBatchPut = $oneof->getRawBatchPut();
            if ($rawBatchPut !== null) {
                $batchKeys = [];
                foreach ($rawBatchPut->getPairs() as $pair) {
                    $batchKeys[] = $pair->getKey();
                }
                $keysByRequest[] = $batchKeys;
                $shapeByRequest[] = 'rawBatchPut';
                continue;
            }
            $rawBatchDelete = $oneof->getRawBatchDelete();
            if ($rawBatchDelete !== null) {
                /** @var list<string> $batchKeys */
                $batchKeys = iterator_to_array($rawBatchDelete->getKeys());
                $keysByRequest[] = $batchKeys;
                $shapeByRequest[] = 'rawBatchDelete';
                continue;
            }
            $rawPut = $oneof->getRawPut();
            if ($rawPut !== null) {
                $keysByRequest[] = [$rawPut->getKey()];
                $shapeByRequest[] = 'rawPut';
                continue;
            }
            $keysByRequest[] = [];
            $shapeByRequest[] = 'unknown';
        }
        $this->exchanges[] = [
            'address' => $address,
            'ids' => $requestIds,
            'keysByRequest' => $keysByRequest,
            'shapeByRequest' => $shapeByRequest,
        ];

        if ($this->responder instanceof \Closure) {
            return ($this->responder)($address, $request, $requestIds);
        }

        // Default: echo every key back as its value, reversed, in the same
        // oneof shape as the request (RawGet → value, RawBatchGet → pairs).
        $ids = array_reverse($requestIds);
        $responses = [];
        foreach ($ids as $id) {
            $index = array_search($id, $requestIds, true);
            $keys = $keysByRequest[$index] ?? [];
            $oneof = new BatchCommandsResponse\Response();
            $shape = $shapeByRequest[$index] ?? 'unknown';
            if ($shape === 'rawBatchGet') {
                $rawBatchGet = new \CrazyGoat\Proto\Kvrpcpb\RawBatchGetResponse();
                $rawBatchGet->setPairs($this->pairs($keys));
                $oneof->setRawBatchGet($rawBatchGet);
            } elseif ($shape === 'rawBatchPut') {
                $oneof->setRawBatchPut(new \CrazyGoat\Proto\Kvrpcpb\RawBatchPutResponse());
            } elseif ($shape === 'rawBatchDelete') {
                $oneof->setRawBatchDelete(new \CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteResponse());
            } elseif ($shape === 'rawPut') {
                $oneof->setRawPut(new \CrazyGoat\Proto\Kvrpcpb\RawPutResponse());
            } else {
                $rawGet = new \CrazyGoat\Proto\Kvrpcpb\RawGetResponse();
                $rawGet->setValue('v:' . implode(',', $keys));
                $oneof->setRawGet($rawGet);
            }
            $responses[] = $oneof;
        }
        $wire = new BatchCommandsResponse();
        $wire->setRequestIds($ids);
        $wire->setResponses($responses);

        return [$wire];
    }

    /**
     * @param list<string> $keys
     * @return list<\CrazyGoat\Proto\Kvrpcpb\KvPair>
     */
    private function pairs(array $keys): array
    {
        $pairs = [];
        foreach ($keys as $key) {
            $pair = new \CrazyGoat\Proto\Kvrpcpb\KvPair();
            $pair->setKey($key);
            $pair->setValue('v:' . $key);
            $pairs[] = $pair;
        }

        return $pairs;
    }
}
