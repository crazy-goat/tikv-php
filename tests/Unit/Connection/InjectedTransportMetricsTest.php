<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Tests\Unit\Support\RecordingLogger;
use PHPUnit\Framework\TestCase;

/**
 * `options['metrics']` is a client-side option, not a transport option
 * (issue #283).
 *
 * The option backs the counters the *clients* keep — the region cache and
 * the retries — so the factory applies it in every case, injected transport
 * or not. The RPC counters of the transport are a different thing: they
 * follow the transport, which was built with its own backend, and the
 * factory cannot change them. The split is announced in the log, but only
 * when the option was actually given — a message about an option the
 * application never set would only be noise.
 *
 * Pure PHP — the injected transport is a mock, so no channel is created and
 * the test runs in the extension-free Unit suite.
 */
final class InjectedTransportMetricsTest extends TestCase
{
    public function testMetricsBackendIsAppliedToTheClientSideWithAnInjectedTransport(): void
    {
        $metrics = $this->createMock(MetricsInterface::class);

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: [
                'grpcClient' => $this->createMock(GrpcClientInterface::class),
                'metrics' => $metrics,
            ],
        );

        $this->assertSame($metrics, $bundle->metrics);
    }

    public function testTheMetricsSplitIsAnnouncedWhenTheOptionWasGiven(): void
    {
        $logger = new RecordingLogger();

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            $logger,
            options: [
                'grpcClient' => $this->createMock(GrpcClientInterface::class),
                'metrics' => $this->createMock(MetricsInterface::class),
            ],
        );

        $this->assertStringContainsString(
            "options['metrics'] backs the client-side counters only",
            implode("\n", $logger->messages),
        );
    }

    public function testTheMetricsSplitIsNotAnnouncedWithoutTheOption(): void
    {
        $logger = new RecordingLogger();

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            $logger,
            options: ['grpcClient' => $this->createMock(GrpcClientInterface::class)],
        );

        $this->assertStringNotContainsString(
            "options['metrics']",
            implode("\n", $logger->messages),
        );
    }
}
