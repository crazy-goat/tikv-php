<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsEmptyRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\TiKV\Client\Batch\BatchCommands\BatchCommandsCorrelator;
use CrazyGoat\TiKV\Client\Grpc\BatchCommandsConnection;
use PHPUnit\Framework\TestCase;

/**
 * Verifies the ext-grpc bidirectional BatchCommands seam against a real
 * TiKV store (issue #418). Runs in the E2E suites (docker, ext-grpc +
 * cluster available); the store address defaults to the in-compose
 * hostname and can be overridden with TIKV_ADDR for local runs.
 */
final class BatchCommandsConnectionE2ETest extends TestCase
{
    private static string $storeAddress = 'tikv1:20160';

    public static function setUpBeforeClass(): void
    {
        if (getenv('TIKV_ADDR') !== false && getenv('TIKV_ADDR') !== '') {
            self::$storeAddress = (string) getenv('TIKV_ADDR');
        }
    }

    public function testEmptyPingRoundTripReceivesResponse(): void
    {
        $channel = new \Grpc\Channel(self::$storeAddress, [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]);
        $connection = BatchCommandsConnection::open($channel);

        $request = new BatchCommandsRequest();
        $inner = new BatchCommandsRequest\Request();
        $inner->setEmpty(new BatchCommandsEmptyRequest());
        $request->setRequests([$inner]);
        $request->setRequestIds([42]);

        $connection->send($request);
        $responses = BatchCommandsCorrelator::drain(
            [42],
            static fn (): ?\CrazyGoat\Proto\Tikvpb\BatchCommandsResponse => $connection->recv(),
            10000,
        );

        // TiKV echoes the request_id back; assert the invariant (the empty
        // ping carries no payload), not any wire framing detail.
        self::assertNotSame([], $responses);
        $allIds = [];
        foreach ($responses as $response) {
            foreach ($response->getRequestIds() as $id) {
                $allIds[] = (int) $id;
            }
        }
        self::assertContains(42, $allIds, 'the empty ping must be answered by request_id');

        $connection->close();
        $channel->close();
    }

    public function testStreamIsReusedAcrossTwoRoundTripsWithTwoRawGets(): void
    {
        $channel = new \Grpc\Channel(self::$storeAddress, [
            'credentials' => \Grpc\ChannelCredentials::createInsecure(),
        ]);
        $connection = BatchCommandsConnection::open($channel);

        // Two round trips on the SAME stream (no half-close in between) —
        // the reuse contract of the per-store stream pool.
        foreach ([1 => 'bc-reuse-a', 2 => 'bc-reuse-b'] as $id => $key) {
            $request = new BatchCommandsRequest();
            $inner = new BatchCommandsRequest\Request();
            $rawGet = new RawGetRequest();
            $rawGet->setKey($key);
            $inner->setRawGet($rawGet);
            $request->setRequests([$inner]);
            $request->setRequestIds([$id]);

            $connection->send($request);
            $responses = BatchCommandsCorrelator::drain(
                [$id],
                static fn (): ?\CrazyGoat\Proto\Tikvpb\BatchCommandsResponse => $connection->recv(),
                10000,
            );

            $answered = [];
            foreach ($responses as $response) {
                foreach ($response->getRequestIds() as $wireId) {
                    $answered[] = (int) $wireId;
                }
            }
            self::assertContains($id, $answered, "round trip $id must be answered on the reused stream");
        }

        $connection->close();
        $channel->close();
    }
}
