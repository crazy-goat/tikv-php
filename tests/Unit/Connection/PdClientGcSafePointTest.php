<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Pdpb\GetGCSafePointResponse;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;

final class PdClientGcSafePointTest extends TestCase
{
    public function testGetGcSafePointReturnsSafePoint(): void
    {
        $response = new GetGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $response->setHeader($header);
        $response->setSafePoint('12345');

        $grpc = $this->mockGrpcCalls(['GetGCSafePoint' => $response]);
        $client = new PdClient($grpc, 'pd:2379');

        $this->assertSame(12345, $client->getGCSafePoint());
    }

    public function testGetGcSafePointFailsClosedOnHeaderError(): void
    {
        $response = new GetGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $error = new \CrazyGoat\Proto\Pdpb\Error();
        $error->setMessage('cluster is bootstrapping');
        $header->setError($error);
        $response->setHeader($header);

        $grpc = $this->mockGrpcCalls(['GetGCSafePoint' => $response]);
        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('PD GetGCSafePoint failed: cluster is bootstrapping');
        $client->getGCSafePoint();
    }

    public function testGetGcSafePointRetriesOnClusterIdMismatch(): void
    {
        $secondHeader = new ResponseHeader();
        $secondHeader->setClusterId(999);
        $second = new GetGCSafePointResponse();
        $second->setHeader($secondHeader);
        $second->setSafePoint('7');

        $callCount = 0;
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnCallback(static function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
            ) use (
                &$callCount,
                $second
): Message {
                $callCount++;
                if ($callCount === 1) {
                    throw new GrpcException('mismatch cluster id, need 999 but got 0', 0);
                }

                return $second;
            });

        $client = new PdClient($grpc, 'pd:2379');
        $this->assertSame(7, $client->getGCSafePoint());
        $this->assertSame(999, $client->getClusterId());
    }

    public function testUpdateServiceGcSafePointReturnsMinSafePoint(): void
    {
        $captured = null;
        $response = new \CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $response->setHeader($header);
        $response->setServiceId('worker-1');
        $response->setMinSafePoint('424242');

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->with(
                'pd:2379',
                'pdpb.PD',
                'UpdateServiceGCSafePoint',
                $this->callback(static function (
                    \CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointRequest $request,
                ) use (&$captured): bool {
                    $captured = $request;

                    return true;
                }),
            )
            ->willReturn($response);

        $client = new PdClient($grpc, 'pd:2379');
        $result = $client->updateServiceGCSafePoint('worker-1', 123456, 600);

        $this->assertSame(424242, $result);
        self::assertNotNull($captured);
        $this->assertSame('worker-1', $captured->getServiceId());
        $this->assertSame('123456', (string) $captured->getSafePoint());
        $this->assertSame('600', (string) $captured->getTtl());
    }

    public function testUpdateServiceGcSafePointRejectsOutOfRangeUint64(): void
    {
        // A uint64 above PHP_INT_MAX cannot exist as a PHP int: the gencode
        // rejects the out-of-range string at the setter, and a value that
        // arrives already-cast is guarded by PdClient::uint64ToInt() —
        // either way the caller never sees a wrapped negative safe point.
        $response = new \CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $response->setHeader($header);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Cannot convert');
        $response->setMinSafePoint('18446744073709551616'); // 2^64: unrepresentable
    }

    public function testUint64GuardRejectsNegativeSafePointValue(): void
    {
        // Defence in depth for the int path: if a value ever arrives as a
        // negative PHP int (gencode intval() of an out-of-range uint64 on
        // some platforms), uint64ToInt() fails closed instead of returning
        // a nonsense safe point. Driven through the public getGCSafePoint()
        // with a stubbed response.
        $response = new GetGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $response->setHeader($header);
        $response->setSafePoint('-5');

        $grpc = $this->mockGrpcCalls(['GetGCSafePoint' => $response]);
        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('invalid GC safe point');
        $client->getGCSafePoint();
    }

    public function testUpdateServiceGcSafePointFailsClosedOnEmptyMessageHeaderError(): void
    {
        // A header error with an empty message is still an error: treating
        // it as success would fabricate a min safe point (default 0).
        $response = new \CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $error = new \CrazyGoat\Proto\Pdpb\Error();
        $error->setMessage('');
        $header->setError($error);
        $response->setHeader($header);

        $grpc = $this->mockGrpcCalls(['UpdateServiceGCSafePoint' => $response]);
        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('unknown PD error');
        $client->updateServiceGCSafePoint('worker-1', 1, 600);
    }

    public function testUpdateServiceGcSafePointRemovalRequiresZeroSafePoint(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');
        $client = new PdClient($grpc, 'pd:2379');

        // ttl <= 0 is a removal (PD's UpdateServiceGCSafePoint removes the
        // registration for any non-positive TTL) — pairing it with a
        // positive safe point is a caller error we reject.
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('safePoint must be 0 when ttlSeconds is <= 0');
        $client->updateServiceGCSafePoint('worker-1', 123456, -1);
    }

    public function testUpdateServiceGcSafePointRejectsEmptyServiceId(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');
        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('serviceId must be a non-empty string');
        $client->updateServiceGCSafePoint('', 123, 600);
    }

    public function testUpdateServiceGcSafePointFailsClosedOnHeaderError(): void
    {
        $response = new \CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $error = new \CrazyGoat\Proto\Pdpb\Error();
        $error->setMessage('service safe point is smaller than current GC safe point');
        $header->setError($error);
        $response->setHeader($header);

        $grpc = $this->mockGrpcCalls(['UpdateServiceGCSafePoint' => $response]);
        $client = new PdClient($grpc, 'pd:2379');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('PD UpdateServiceGCSafePoint failed: service safe point is smaller');
        $client->updateServiceGCSafePoint('worker-1', 1, 600);
    }

    public function testUpdateServiceGcSafePointReturnsNullWhenUnsupported(): void
    {
        $response = new \CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $error = new \CrazyGoat\Proto\Pdpb\Error();
        $error->setMessage('UpdateServiceGCSafePoint is not supported');
        $header->setError($error);
        $response->setHeader($header);

        $grpc = $this->mockGrpcCalls(['UpdateServiceGCSafePoint' => $response]);
        $client = new PdClient($grpc, 'pd:2379');

        $this->assertNull($client->updateServiceGCSafePoint('worker-1', 1, 600));
    }

    public function testUpdateServiceGcSafePointReturnsNullOnUnimplementedGrpcStatus(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new GrpcException('unimplemented method UpdateServiceGCSafePoint', 12));

        $client = new PdClient($grpc, 'pd:2379');
        $this->assertNull($client->updateServiceGCSafePoint('worker-1', 1, 600));
    }

    /**
     * Mock GrpcClientInterface::call() to return a canned response per PD
     * method name.
     *
     * @param array<string, Message> $responses
     */
    private function mockGrpcCalls(array $responses): GrpcClientInterface&\PHPUnit\Framework\MockObject\MockObject
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            static function (string $address, string $service, string $method) use ($responses): Message {
                if (!isset($responses[$method])) {
                    throw new \LogicException("Unexpected PD method: $method");
                }

                return $responses[$method];
            },
        );

        return $grpc;
    }
}
