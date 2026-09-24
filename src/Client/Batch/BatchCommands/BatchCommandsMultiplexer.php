<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Batch\BatchCommands;

use CrazyGoat\Proto\Kvrpcpb\BatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchDeleteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawBatchPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawDeleteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException;
use Google\Protobuf\Internal\Message;

/**
 * Multiplexes a set of unary requests over BatchCommands streams, one per
 * store address (issue #418, GAP-04).
 *
 * Per dispatch: every representable entry is wrapped into the
 * BatchCommandsRequest.Request oneof with a unique request_id, grouped by
 * store address, and exchanged over that store's stream (one wire round
 * trip per store instead of one unary RPC per entry). Responses are
 * correlated by request_id, tolerating out-of-order delivery.
 *
 * Entries whose request type is absent from the oneof (RawCompareAndSwap,
 * RawGetKeyTTL, RawChecksum, KvGC, KvImport, …) are reported as fallback —
 * the caller re-dispatches them on the existing unary path. A transport
 * failure throws: the caller falls back to the full unary path.
 */
final class BatchCommandsMultiplexer
{
    /**
     * Unary request class → BatchCommands oneof wiring. The setter is used
     * on BatchCommandsRequest\Request, the getter on the response oneof
     * (BatchCommandsResponse\Response) to extract the inner response.
     *
     * Deliberately minimal: only types the client actually multiplexes this
     * cycle (the raw KV read/write family and the txn single-read family).
     * RawCompareAndSwap, RawGetKeyTTL, RawChecksum, KvGC and KvImport are
     * absent from the TiKV oneof and can never appear here.
     */
    private const ONEOF_MAP = [
        RawGetRequest::class => ['request' => 'setRawGet', 'response' => 'getRawGet'],
        RawPutRequest::class => ['request' => 'setRawPut', 'response' => 'getRawPut'],
        RawDeleteRequest::class => ['request' => 'setRawDelete', 'response' => 'getRawDelete'],
        RawScanRequest::class => ['request' => 'setRawScan', 'response' => 'getRawScan'],
        RawBatchGetRequest::class => ['request' => 'setRawBatchGet', 'response' => 'getRawBatchGet'],
        RawBatchPutRequest::class => ['request' => 'setRawBatchPut', 'response' => 'getRawBatchPut'],
        RawBatchDeleteRequest::class => ['request' => 'setRawBatchDelete', 'response' => 'getRawBatchDelete'],
        GetRequest::class => ['request' => 'setGet', 'response' => 'getGet'],
        ScanRequest::class => ['request' => 'setScan', 'response' => 'getScan'],
        BatchGetRequest::class => ['request' => 'setBatchGet', 'response' => 'getBatchGet'],
    ];

    private int $nextRequestId = 1;

    public function __construct(
        private readonly BatchCommandsTransportInterface $transport,
    ) {
    }

    /**
     * Multiplex the entries over the per-store streams.
     *
     * @param array<array-key, BatchCommandsEntry> $entries fan-out entry key => entry
     * @param int $timeoutMs wall-clock deadline per store round trip in ms; 0 disables it
     *
     * @throws BatchCommandsStreamException on stream
     *         failure (the caller falls back to the unary path)
     */
    public function dispatch(array $entries, int $timeoutMs = 0): BatchCommandsDispatchResult
    {
        $responses = [];
        $fallbackKeys = [];

        // 1. Filter representable entries; assign request_ids up front so
        //    they are unique across the whole dispatch.
        $byAddress = [];
        foreach ($entries as $key => $entry) {
            $oneof = self::ONEOF_MAP[$entry->request::class] ?? null;
            if ($oneof === null) {
                $fallbackKeys[] = $key;
                continue;
            }

            $inner = new BatchCommandsRequest\Request();
            // The oneof setter is matched to the entry's message class via
            // ONEOF_MAP, so the dynamic call always receives the exact type
            // the generated setter expects.
            /** @phpstan-ignore-next-line */
            $inner->{$oneof['request']}($entry->request);

            $byAddress[$entry->address] ??= ['inner' => [], 'ids' => [], 'keys' => []];
            $byAddress[$entry->address]['inner'][] = $inner;
            $byAddress[$entry->address]['ids'][] = $this->nextRequestId;
            $byAddress[$entry->address]['keys'][] = $key;

            ++$this->nextRequestId;
        }

        // 2. One wire round trip per store address. The wire format is
        //    parallel arrays: requests[i] is identified by request_ids[i]
        //    (no per-Request id field).
        foreach ($byAddress as $address => $group) {
            $outer = new BatchCommandsRequest();
            $outer->setRequests($group['inner']);
            $outer->setRequestIds($group['ids']);

            $wireResponses = $this->transport->exchange($address, $outer, $group['ids'], $timeoutMs);

            // 3. Correlate by request_id (out-of-order tolerated): map each
            //    wire response back to the entry that sent that id.
            $keyById = array_combine($group['ids'], $group['keys']);
            foreach ($wireResponses as $wire) {
                $ids = $wire->getRequestIds();
                foreach ($wire->getResponses() as $i => $oneofResponse) {
                    $id = $ids[$i] ?? null;
                    $key = is_int($id) || is_string($id) ? ($keyById[(int) $id] ?? null) : null;
                    if ($key === null) {
                        throw new BatchCommandsStreamException(
                            'BatchCommands response carries a request_id that was not sent on this dispatch',
                        );
                    }
                    $inner = $this->extractResponse($oneofResponse, $entries[$key]->request::class);
                    if (!$inner instanceof Message) {
                        // Response oneof not set (or wrong type) — recover on
                        // the unary path instead of guessing.
                        $fallbackKeys[] = $key;
                        continue;
                    }
                    $responses[$key] = $inner;
                }
            }
        }

        return BatchCommandsDispatchResult::from($responses, $fallbackKeys);
    }

    /**
     * @return Message|null the inner response message, or null when absent
     */
    private function extractResponse(
        \CrazyGoat\Proto\Tikvpb\BatchCommandsResponse\Response $oneofResponse,
        string $requestClass,
    ): ?Message {
        $oneof = self::ONEOF_MAP[$requestClass] ?? null;
        if ($oneof === null) {
            return null;
        }

        return $oneofResponse->{$oneof['response']}();
    }
}
