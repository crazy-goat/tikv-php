<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsEntry;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsMultiplexer;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;

final class BatchCommandsMultiplexerTest extends TestCase
{
    private function entry(string $address, string $key): BatchCommandsEntry
    {
        $request = new RawGetRequest();
        $request->setKey($key);

        return new BatchCommandsEntry($address, $request);
    }

    public function testGroupsEntriesByAddressIntoOneExchangePerStore(): void
    {
        $fake = new FakeBatchCommandsTransport();
        $multiplexer = new BatchCommandsMultiplexer($fake);

        $outcome = $multiplexer->dispatch([
            'a' => $this->entry('tikv1:20160', 'a'),
            'b' => $this->entry('tikv1:20160', 'b'),
            'c' => $this->entry('tikv2:20160', 'c'),
        ]);

        // One wire round trip per store address, not one per entry.
        self::assertCount(2, $fake->exchanges);
        self::assertSame('tikv1:20160', $fake->exchanges[0]['address']);
        self::assertSame('tikv2:20160', $fake->exchanges[1]['address']);
        self::assertCount(2, $fake->exchanges[0]['ids']);
        self::assertCount(1, $fake->exchanges[1]['ids']);

        // No fallbacks: everything was multiplexed and answered.
        self::assertSame([], $outcome->fallbackKeys);
        // Keyed correlation — wire order may differ from entry order.
        $responseKeys = array_keys($outcome->responses);
        sort($responseKeys);
        self::assertSame(['a', 'b', 'c'], $responseKeys);
    }

    public function testAssignsGloballyUniqueRequestIds(): void
    {
        $fake = new FakeBatchCommandsTransport();
        $multiplexer = new BatchCommandsMultiplexer($fake);

        $multiplexer->dispatch([
            'a' => $this->entry('tikv1:20160', 'a'),
            'b' => $this->entry('tikv1:20160', 'b'),
        ]);
        $multiplexer->dispatch([
            'c' => $this->entry('tikv1:20160', 'c'),
        ]);

        $allIds = [
            ...$fake->exchanges[0]['ids'],
            ...$fake->exchanges[1]['ids'],
        ];
        self::assertSame($allIds, array_unique($allIds), 'request_ids must never repeat across dispatches');
    }

    public function testCorrelatesOutOfOrderResponsesBackToEntries(): void
    {
        $fake = new FakeBatchCommandsTransport();
        $multiplexer = new BatchCommandsMultiplexer($fake);

        $outcome = $multiplexer->dispatch([
            'k1' => $this->entry('tikv1:20160', 'alpha'),
            'k2' => $this->entry('tikv1:20160', 'beta'),
            'k3' => $this->entry('tikv1:20160', 'gamma'),
        ]);

        // The fake answers in reversed request_id order; the multiplexer
        // must still map each response to the entry that sent that id.
        $value = static function (Message $response): string {
            assert($response instanceof \CrazyGoat\Proto\Kvrpcpb\RawGetResponse);

            return $response->getValue();
        };
        self::assertSame('v:alpha', $value($outcome->responses['k1']));
        self::assertSame('v:beta', $value($outcome->responses['k2']));
        self::assertSame('v:gamma', $value($outcome->responses['k3']));
    }

    public function testUnsupportedOneofTypesFallBackWithoutExchange(): void
    {
        $fake = new FakeBatchCommandsTransport();
        $multiplexer = new BatchCommandsMultiplexer($fake);

        // RawPutRequest IS representable; use a class absent from the
        // BatchCommands oneof to prove the filter: RawCompareAndSwapRequest
        // can never be multiplexed (it has no Request oneof field).
        $notMultiplexable =
            new \CrazyGoat\Proto\Kvrpcpb\RawCASRequest();

        $outcome = $multiplexer->dispatch([
            'ok' => $this->entry('tikv1:20160', 'a'),
            'no' => new BatchCommandsEntry('tikv1:20160', $notMultiplexable),
            'put' => new BatchCommandsEntry('tikv1:20160', new RawPutRequest()),
        ]);

        self::assertSame(['no'], array_keys($outcome->fallbackKeys));
        // Keyed correlation — wire order may differ from entry order.
        $responseKeys = array_keys($outcome->responses);
        sort($responseKeys);
        self::assertSame(['ok', 'put'], $responseKeys);
        self::assertCount(1, $fake->exchanges);
        self::assertCount(2, $fake->exchanges[0]['ids']);
    }

    public function testResponseWithoutMatchingRequestIdThrows(): void
    {
        $responder = static function (
            string $address,
            \CrazyGoat\Proto\Tikvpb\BatchCommandsRequest $request,
            array $ids,
        ): array {
            $wire = new BatchCommandsResponse();
            $wire->setRequestIds([999]);
            $oneof = new BatchCommandsResponse\Response();
            $rawGet = new \CrazyGoat\Proto\Kvrpcpb\RawGetResponse();
            $rawGet->setValue('bogus');
            $oneof->setRawGet($rawGet);
            $wire->setResponses([$oneof]);

            return [$wire];
        };

        $multiplexer = new BatchCommandsMultiplexer(new FakeBatchCommandsTransport($responder));

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\BatchCommandsStreamException::class);

        $multiplexer->dispatch([
            'a' => $this->entry('tikv1:20160', 'a'),
        ]);
    }
}
