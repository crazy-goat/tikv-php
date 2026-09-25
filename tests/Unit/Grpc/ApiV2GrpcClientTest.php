<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Grpc\ApiV2GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;

final class ApiV2GrpcClientTest extends TestCase
{
    public function testUnaryCallTransformsRequestAndResponse(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $inner = $this->createMock(GrpcClientInterface::class);
        $inner->expects($this->once())
            ->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use ($codec): Message {
                $this->assertSame('tikv:20160', $address);
                $this->assertSame('tikvpb.Tikv', $service);
                $this->assertSame('RawPut', $method);
                $this->assertSame(RawPutResponse::class, $responseClass);
                $this->assertSame(100, $timeoutMs);
                $this->assertInstanceOf(RawPutRequest::class, $request);
                $this->assertSame($codec->encodeKey('key'), $request->getKey());
                $context = $request->getContext();
                $this->assertInstanceOf(Context::class, $context);
                $this->assertSame(2, $context->getApiVersion());

                return new RawPutResponse();
            });

        $client = new ApiV2GrpcClient($inner, $codec, Mode::Raw);
        $response = $client->call(
            'tikv:20160',
            'tikvpb.Tikv',
            'RawPut',
            $this->rawPutRequest('key'),
            RawPutResponse::class,
            100,
        );

        $this->assertInstanceOf(RawPutResponse::class, $response);
    }

    public function testAsyncCallTransformsResponse(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $innerFuture = GrpcFuture::fromWaiter(
            static function (): Message {
                $pair = new KvPair();
                $pair->setKey("\x72\x00\x00\x2a\x00\xFF");
                $response = new ScanResponse();
                $response->setPairs([$pair]);

                return $response;
            },
        );
        $inner = $this->createMock(GrpcClientInterface::class);
        $inner->expects($this->once())
            ->method('callAsync')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs,
            ) use (
                $innerFuture,
                $codec,
            ): GrpcFuture {
                $this->assertInstanceOf(RawPutRequest::class, $request);
                $this->assertSame($codec->encodeKey('key'), $request->getKey());

                return $innerFuture;
            });

        $client = new ApiV2GrpcClient($inner, $codec, Mode::Raw);
        $future = $client->callAsync(
            'tikv:20160',
            'tikvpb.Tikv',
            'RawPut',
            $this->rawPutRequest('key'),
            RawPutResponse::class,
        );
        $response = $future->wait();
        $this->assertInstanceOf(ScanResponse::class, $response);
        $this->assertSame("\x00\xFF", $response->getPairs()[0]->getKey());
    }

    public function testAsyncCancellationPropagatesToInnerFuture(): void
    {
        $cancelled = false;
        $innerFuture = GrpcFuture::fromWaiter(
            static fn (): Message => new RawPutResponse(),
            static function () use (&$cancelled): void {
                $cancelled = true;
            },
        );
        $inner = $this->createMock(GrpcClientInterface::class);
        $inner->method('callAsync')->willReturn($innerFuture);
        $client = new ApiV2GrpcClient(
            $inner,
            new CodecV2(Mode::Raw, 42, 'tenant-a'),
            Mode::Raw,
        );

        $future = $client->callAsync(
            'tikv:20160',
            'tikvpb.Tikv',
            'RawPut',
            $this->rawPutRequest('key'),
            RawPutResponse::class,
        );
        $future->cancel();

        $this->assertTrue($cancelled);
    }

    public function testV1CodecLeavesMessagesUntouched(): void
    {
        $inner = $this->createMock(GrpcClientInterface::class);
        $request = $this->rawPutRequest('key');
        $inner->expects($this->once())
            ->method('call')
            ->with(
                'tikv:20160',
                'tikvpb.Tikv',
                'RawPut',
                $request,
                RawPutResponse::class,
                null,
            )
            ->willReturn(new RawPutResponse());

        $client = new ApiV2GrpcClient($inner, new CodecV1(Mode::Raw), Mode::Raw);
        $client->call(
            'tikv:20160',
            'tikvpb.Tikv',
            'RawPut',
            $request,
            RawPutResponse::class,
        );
    }

    private function rawPutRequest(string $key): RawPutRequest
    {
        $request = new RawPutRequest();
        $request->setKey($key);

        return $request;
    }
}
