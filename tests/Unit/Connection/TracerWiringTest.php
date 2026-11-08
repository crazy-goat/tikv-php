<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\InMemoryTracer;
use CrazyGoat\TiKV\Client\Observability\TraceCategory;
use CrazyGoat\TiKV\Client\Observability\TraceEventInterface;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use CrazyGoat\TiKV\Tests\Unit\Support\RecordingLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\ThrowingTracer;
use PHPUnit\Framework\TestCase;

/**
 * Wiring guard for the new `options['tracer']` (issue #434): the callback a
 * caller injects must reach the factory-built transport and the TxnKv
 * client, a non-conforming value must be rejected, and a throwing tracer
 * must never escape into the data path.
 *
 * `allowInsecure: false` makes the first PD call fail closed in
 * `getChannel()` right after `rpc_started()`, so the whole path runs in the
 * extension-free Unit suite (the same seam as `MetricsWiringTest`).
 */
final class TracerWiringTest extends TestCase
{
    public function testRawKvFactoryTracerReceivesRpcEvents(): void
    {
        $tracer = new InMemoryTracer();

        $client = RawKvClient::create(['127.0.0.1:2379'], options: [
            'allowInsecure' => false,
            'tracer' => $tracer,
        ]);

        try {
            $client->get('key');
            self::fail('Expected the plaintext channel to be refused');
        } catch (InvalidStateException) {
            // The refused channel is the injection point for the PD RPC.
        } finally {
            $client->close();
        }

        $events = $tracer->getEvents();
        self::assertSame(
            ['rpc_started', 'rpc_completed'],
            $tracer->eventNames(TraceCategory::KV_REQUEST),
        );
        self::assertSame('pdpb.PD/GetRegion', $events[0]['fields']['operation']);
        self::assertSame('pdpb.PD/GetRegion', $events[1]['fields']['operation']);
        self::assertSame($events[0]['fields']['eventId'], $events[1]['fields']['eventId']);
        self::assertFalse($events[1]['fields']['success']);
    }

    public function testTxnKvFactoryTracerReceivesTsoEvents(): void
    {
        $tracer = new InMemoryTracer();

        $client = TxnKvClient::create(['127.0.0.1:2379'], options: [
            'allowInsecure' => false,
            'tracer' => $tracer,
        ]);

        try {
            $client->begin();
            self::fail('Expected the plaintext channel to be refused');
        } catch (InvalidStateException) {
            // begin() fetches its start timestamp from PD, so the refused
            // TSO channel is the injection point.
        } finally {
            $client->close();
        }

        $events = $tracer->getEvents();
        self::assertSame(
            ['rpc_started', 'rpc_completed'],
            $tracer->eventNames(TraceCategory::KV_REQUEST),
        );
        self::assertSame('pdpb.PD/Tso', $events[0]['fields']['operation']);
        self::assertSame($events[0]['fields']['eventId'], $events[1]['fields']['eventId']);
    }

    public function testInjectedTracerIsReturnedOnTheBundle(): void
    {
        $tracer = new InMemoryTracer();

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: [
                'grpcClient' => $this->createMock(GrpcClientInterface::class),
                'tracer' => $tracer,
            ],
        );

        self::assertSame($tracer, $bundle->tracer);
    }

    public function testInvalidTracerIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['tracer'] must be an instance of TraceEventInterface");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: [
                'grpcClient' => $this->createMock(GrpcClientInterface::class),
                'tracer' => new \stdClass(),
            ],
        );
    }

    public function testTheTracerSplitIsAnnouncedWhenTheOptionWasGiven(): void
    {
        $logger = new RecordingLogger();

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            $logger,
            options: [
                'grpcClient' => $this->createMock(GrpcClientInterface::class),
                'tracer' => new InMemoryTracer(),
            ],
        );

        self::assertStringContainsString(
            "options['tracer'] backs the client-side trace events only",
            implode("\n", $logger->messages),
        );
    }

    public function testTheTracerSplitIsNotAnnouncedWithoutTheOption(): void
    {
        $logger = new RecordingLogger();

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            $logger,
            options: ['grpcClient' => $this->createMock(GrpcClientInterface::class)],
        );

        self::assertStringNotContainsString(
            "options['tracer']",
            implode("\n", $logger->messages),
        );
    }

    public function testThrowingTracerIsSwallowedByTheTransport(): void
    {
        $logger = new RecordingLogger();

        $client = RawKvClient::create(['127.0.0.1:2379'], $logger, options: [
            'allowInsecure' => false,
            'tracer' => new ThrowingTracer(),
        ]);

        try {
            $client->get('key');
            self::fail('Expected the plaintext channel to be refused');
        } catch (InvalidStateException) {
            // The tracer exception must not replace the transport failure.
        } finally {
            $client->close();
        }

        self::assertStringContainsString(
            'Trace backend threw',
            implode("\n", $logger->messages),
        );
    }

    public function testTracerOptionIsAnInstanceOfTheInterface(): void
    {
        self::assertInstanceOf(TraceEventInterface::class, new InMemoryTracer());
    }
}
