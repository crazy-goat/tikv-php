<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Batch;

use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Batch\GrpcFuture;
use Grpc\Call;
use PHPUnit\Framework\TestCase;

class GrpcFutureTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('grpc')) {
            $this->markTestSkipped('grpc extension is not loaded');
        }
    }

    public function testConstruction(): void
    {
        $call = $this->createMock(Call::class);
        $future = new GrpcFuture($call, RawGetResponse::class);

        $this->assertInstanceOf(GrpcFuture::class, $future);
    }
}
