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

    public function testGetTimestampFallsBackToLocalOnGrpcException(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());
        $result = $oracle->getTimestamp();

        $this->assertGreaterThan(0, $result);
    }

    public function testGetTimestampFallbackIsMonotonic(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $oracle = new TimestampOracle($grpc, '127.0.0.1:2379', $pdClient, new NullLogger());

        $ts1 = $oracle->getTimestamp();
        $ts2 = $oracle->getTimestamp();

        $this->assertGreaterThanOrEqual($ts1, $ts2);
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
}
