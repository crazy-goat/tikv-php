<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

trait GrpcExtensionGate
{
    private function requireGrpcExtension(): void
    {
        if (class_exists(\Grpc\Timeval::class)) {
            return;
        }

        if (getenv('REQUIRE_GRPC_EXTENSION') === '1') {
            self::fail('gRPC extension is required');
        }

        $this->markTestSkipped('gRPC extension not available');
    }
}
