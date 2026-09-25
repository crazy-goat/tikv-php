<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\KeyspaceResolver;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class KeyspaceResolverTest extends TestCase
{
    public function testResolvesAndCachesName(): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->expects($this->once())
            ->method('getKeyspaceId')
            ->with('tenant-a')
            ->willReturn(42);

        $resolver = new KeyspaceResolver($pd);

        $this->assertSame(42, $resolver->resolve('tenant-a'));
        $this->assertSame(42, $resolver->resolve('tenant-a'));
    }

    public function testEmptyNameUsesDefaultKeyspace(): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->expects($this->once())
            ->method('getKeyspaceId')
            ->with(KeyspaceResolver::DEFAULT_NAME)
            ->willReturn(0);

        $this->assertSame(0, (new KeyspaceResolver($pd))->resolve(''));
    }

    public function testRejectsOutOfRangePdResult(): void
    {
        $pd = $this->createMock(PdClientInterface::class);
        $pd->method('getKeyspaceId')->willReturn(0x1000000);

        $this->expectException(InvalidArgumentException::class);
        (new KeyspaceResolver($pd))->resolve('tenant-a');
    }
}
