<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv\Dto;

use CrazyGoat\TiKV\Client\Region\Dto\PeerInfo;
use PHPUnit\Framework\TestCase;

class PeerInfoTest extends TestCase
{
    public function testConstruction(): void
    {
        $peer = new PeerInfo(peerId: 7, storeId: 3);

        $this->assertSame(7, $peer->peerId);
        $this->assertSame(3, $peer->storeId);
    }

    public function testIsReadonly(): void
    {
        $reflection = new \ReflectionClass(PeerInfo::class);
        $this->assertTrue($reflection->isReadOnly());
    }
}
