<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    public function testAllowedStorePortsDefaultsToNull(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertNull($bundle->allowedStorePorts);
    }

    public function testAllowedStorePortsThreadedThroughBundle(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => [20160, 20161]],
        );

        $this->assertSame([20160, 20161], $bundle->allowedStorePorts);
    }

    public function testAllowedStorePortsExplicitNullIsAccepted(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => null],
        );

        $this->assertNull($bundle->allowedStorePorts);
    }

    public function testAllowedStorePortsRejectsNonArrayValue(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => '20160'],
        );
    }

    public function testAllowedStorePortsRejectsNonIntEntries(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => ['20160']],
        );
    }

    public function testAllowedStorePortsRejectsOutOfRangeEntries(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => [0]],
        );
    }
}
