<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Observability;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Observability\SafeLogging;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use CrazyGoat\TiKV\Client\Util\ShutdownState;
use CrazyGoat\TiKV\Tests\Unit\Support\RecordingLogger;
use CrazyGoat\TiKV\Tests\Unit\Support\ThrowingLogger;
use PHPUnit\Framework\TestCase;

/**
 * Regression tests for issue #287 (GRPC-28): logging on a destructor-
 * reachable path must never throw out of the destructor.
 */
final class SafeLoggingTest extends TestCase
{
    protected function tearDown(): void
    {
        ShutdownState::resetForTests();
    }

    public function testLogForwardsTheRecordToTheLogger(): void
    {
        $logger = new RecordingLogger();

        SafeLogging::log($logger, 'error', 'channel failed', ['address' => 'tikv1:20160']);

        $this->assertSame(['channel failed'], $logger->messages);
    }

    public function testLogSwallowsAThrowingLogger(): void
    {
        $this->expectNotToPerformAssertions();

        SafeLogging::log(new ThrowingLogger(), 'error', 'channel failed');
    }

    public function testRawKvClientDestructorSurvivesAThrowingLoggerOnEveryClosePath(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);
        $regionCache = $this->createMock(RegionCacheInterface::class);

        // Make every close step fail so each catch block emits a record
        // through the logger — and the logger throws from every one.
        $regionCache->method('clear')->willThrowException(new \RuntimeException('cache boom'));
        $grpc->method('close')->willThrowException(new \RuntimeException('grpc boom'));
        $pdClient->method('close')->willThrowException(new \RuntimeException('pd boom'));

        $client = new RawKvClient(
            $pdClient,
            $grpc,
            $regionCache,
            logger: new ThrowingLogger(),
        );

        $this->expectNotToPerformAssertions();

        $client->__destruct();
    }

    public function testTxnKvClientDestructorSurvivesAThrowingLoggerOnEveryClosePath(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $grpc = $this->createMock(GrpcClientInterface::class);
        $regionCache = $this->createMock(RegionCacheInterface::class);

        $regionCache->method('clear')->willThrowException(new \RuntimeException('cache boom'));
        $grpc->method('close')->willThrowException(new \RuntimeException('grpc boom'));
        $pdClient->method('close')->willThrowException(new \RuntimeException('pd boom'));

        $client = new TxnKvClient(
            $pdClient,
            $grpc,
            $regionCache,
            logger: new ThrowingLogger(),
        );

        $this->expectNotToPerformAssertions();

        $client->__destruct();
    }
}
