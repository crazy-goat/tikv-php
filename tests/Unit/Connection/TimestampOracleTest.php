<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Pdpb\Timestamp;
use CrazyGoat\Proto\Pdpb\TsoResponse;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class TimestampOracleTest extends TestCase
{
    public function testGetTimestampReturnsComposedTimestamp(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(5);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc->method('call')->willReturn($response);

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 5;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampWithZeroLogical(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(0);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc->method('call')->willReturn($response);

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 0;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampThrowsTiKvExceptionWhenTimestampNull(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $response = new TsoResponse();

        $grpc->method('call')->willReturn($response);

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO response missing timestamp');
        $oracle->getTimestamp();
    }

    public function testGetTimestampThrowsOnGrpcExceptionInsteadOfFabricating(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO request failed');
        $oracle->getTimestamp();
    }

    public function testGetTimestampPreservesGrpcStatusCodeOnFailure(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());

        try {
            $oracle->getTimestamp();
            $this->fail('Expected TiKvException to be thrown');
        } catch (TiKvException $e) {
            $this->assertSame(14, $e->getCode());
            $this->assertInstanceOf(GrpcException::class, $e->getPrevious());
        }
    }

    public function testGetTimestampLogsErrorOnGrpcException(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('error')
            ->with(
                $this->stringContains('refusing to fabricate'),
                $this->callback(
                    fn(array $context): bool => ($context['error'] ?? null) === 'gRPC error: tso unavailable'
                        && ($context['grpcStatusCode'] ?? null) === 14,
                ),
            );

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, $logger);

        try {
            $oracle->getTimestamp();
            $this->fail('Expected TiKvException to be thrown');
        } catch (TiKvException) {
        }
    }

    public function testGetTimestampWithRealPdClientInstance(): void
    {
        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(1);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $pdClient = new PdClient($grpc, '127.0.0.1:2379');

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 1;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampRetriesOnClusterIdMismatch(): void
    {
        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(2);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('mismatch cluster id, need 42 but got 0', 14)),
                $response,
            );

        $pdClient = new PdClient($grpc, '127.0.0.1:2379');

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 2;
        $this->assertSame($expected, $result);
        $this->assertSame(42, $pdClient->getClusterId());
    }

    public function testGetTimestampThrowsWhenClusterIdRetryAlsoFails(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('mismatch cluster id, need 42 but got 0', 14)),
                $this->throwException(new GrpcException('still unavailable', 14)),
            );

        $pdClient = new PdClient($grpc, '127.0.0.1:2379');

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO request failed');
        $oracle->getTimestamp();
    }

    public function testGetTimestampThrowsOnClusterIdMismatchWhenPdClientIsInterfaceMock(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('mismatch cluster id, need 42 but got 0', 14));

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO request failed');
        $oracle->getTimestamp();
    }
}
