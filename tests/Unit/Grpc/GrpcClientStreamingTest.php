<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use PHPUnit\Framework\TestCase;

/**
 * Issue #286 (GRPC-27): `callStreaming()` consumes its request iterable
 * lazily, sends every yielded message in order, and cancels the call when a
 * failure abandons it mid-stream. The original failure always wins over a
 * throwable from `cancel()`, and the abandoned call is still counted as one
 * failed RPC.
 *
 * The `Grpc\Call` is injected through the transport's `callFactory` seam, so
 * the send loop is observable without a server.
 */
final class GrpcClientStreamingTest extends TestCase
{
    use GrpcExtensionGate;

    private const ADDRESS = '127.0.0.1:1';

    private const OPERATION = 'tikvpb.Tikv/RawGet';

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testCallStreamingSendsEveryMessageFromAGeneratorInOrder(): void
    {
        $response = new RawGetResponse();
        $sent = [];

        $call = $this->createMock(\Grpc\Call::class);
        $call->expects($this->never())->method('cancel');
        $call->method('startBatch')->willReturnCallback(
            function (array $batch) use (&$sent, $response): array {
                if (array_key_exists(\Grpc\OP_SEND_MESSAGE, $batch)) {
                    /** @var array{message: string} $send */
                    $send = $batch[\Grpc\OP_SEND_MESSAGE];
                    $sent[] = $send['message'];

                    return [];
                }

                if (array_key_exists(\Grpc\OP_RECV_MESSAGE, $batch)) {
                    return [
                        'status' => ['code' => \Grpc\STATUS_OK, 'details' => 'OK'],
                        'message' => $response->serializeToString(),
                    ];
                }

                return [];
            },
        );

        $grpc = new GrpcClient(
            callFactory: static fn(
                \Grpc\Channel $channel,
                string $path,
                \Grpc\Timeval $deadline,
            ): \Grpc\Call => $call,
        );

        try {
            $grpc->callStreaming(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                $this->requests('a', 'b', 'c'),
                RawGetResponse::class,
            );
        } finally {
            $grpc->close();
        }

        self::assertSame(
            [
                $this->request('a')->serializeToString(),
                $this->request('b')->serializeToString(),
                $this->request('c')->serializeToString(),
            ],
            $sent,
            'Every yielded message must be sent exactly once, in generator order',
        );
    }

    public function testAMidStreamSendFailureCancelsOnceAndRecordsAFailedRpc(): void
    {
        $failure = new \RuntimeException('send failed');
        $metrics = new InMemoryMetrics();
        $sentMessages = 0;

        $call = $this->createMock(\Grpc\Call::class);
        $call->expects($this->once())->method('cancel');
        $call->method('startBatch')->willReturnCallback(
            function (array $batch) use (&$sentMessages, $failure): array {
                if (array_key_exists(\Grpc\OP_SEND_MESSAGE, $batch)) {
                    $sentMessages++;
                    if ($sentMessages === 2) {
                        throw $failure;
                    }
                }

                return [];
            },
        );

        $grpc = new GrpcClient(
            metrics: $metrics,
            callFactory: static fn(\Grpc\Channel $channel, string $path, \Grpc\Timeval $deadline): \Grpc\Call => $call,
        );

        try {
            $grpc->callStreaming(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                $this->requests('a', 'b', 'c'),
                RawGetResponse::class,
            );
            self::fail('The mid-stream send failure must propagate');
        } catch (\RuntimeException $e) {
            self::assertSame($failure, $e, 'The original failure must propagate unchanged');
        } finally {
            $grpc->close();
        }

        self::assertSame(2, $sentMessages, 'The stream stops at the failing message');
        self::assertSame(1, $metrics->getRpcStarted(self::OPERATION));
        self::assertSame(1, $metrics->getRpcFailed(self::OPERATION));
        self::assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
    }

    public function testAThrowableFromCancelDoesNotReplaceTheOriginalFailure(): void
    {
        $failure = new \RuntimeException('send failed');
        $cancelFailure = new \LogicException('cancel failed');

        $call = $this->createMock(\Grpc\Call::class);
        $call->expects($this->once())
            ->method('cancel')
            ->willThrowException($cancelFailure);
        $call->method('startBatch')->willThrowException($failure);

        $grpc = new GrpcClient(
            callFactory: static fn(
                \Grpc\Channel $channel,
                string $path,
                \Grpc\Timeval $deadline,
            ): \Grpc\Call => $call,
        );

        try {
            $grpc->callStreaming(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                $this->requests('a'),
                RawGetResponse::class,
            );
            self::fail('The original send failure must propagate');
        } catch (\RuntimeException $e) {
            self::assertSame($failure, $e, 'cancel() must not mask the original failure');
        } finally {
            $grpc->close();
        }
    }

    /**
     * @return \Generator<int, RawGetRequest>
     */
    private function requests(string ...$keys): \Generator
    {
        foreach ($keys as $key) {
            yield $this->request($key);
        }
    }

    private function request(string $key): RawGetRequest
    {
        $request = new RawGetRequest();
        $request->setKey($key);

        return $request;
    }
}
