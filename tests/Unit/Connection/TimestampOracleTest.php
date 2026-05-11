<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Pdpb\Timestamp;
use CrazyGoat\Proto\Pdpb\TsoResponse;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
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
}
