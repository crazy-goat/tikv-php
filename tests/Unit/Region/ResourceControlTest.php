<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ResourceControlTest extends TestCase
{
    public function testEmptyOptionsConfigureNothing(): void
    {
        $resourceControl = ResourceControl::fromOptions([]);

        $this->assertTrue($resourceControl->isEmpty());
        $this->assertNull($resourceControl->getGroupName());
        $this->assertNull($resourceControl->getResourceGroupTag());
    }

    public function testOptionsAreRead(): void
    {
        $resourceControl = ResourceControl::fromOptions([
            'resourceGroup' => 'tenant-a',
            'resourceGroupTag' => 'nightly-job',
            'unrelated' => 'ignored',
        ]);

        $this->assertFalse($resourceControl->isEmpty());
        $this->assertSame('tenant-a', $resourceControl->getGroupName());
        $this->assertSame('nightly-job', $resourceControl->getResourceGroupTag());
    }

    public function testApplySetsBothWireFields(): void
    {
        $resourceControl = new ResourceControl('tenant-a', 'nightly-job');
        $context = new Context();

        $resourceControl->apply($context);

        $controlContext = $context->getResourceControlContext();
        $this->assertNotNull($controlContext);
        $this->assertSame('tenant-a', $controlContext->getResourceGroupName());
        $this->assertSame('nightly-job', $context->getResourceGroupTag());
    }

    public function testApplyLeavesAnEmptyContextByteIdentical(): void
    {
        $context = new Context();
        $reference = $context->serializeToString();

        ResourceControl::fromOptions([])->apply($context);

        $this->assertFalse($context->hasResourceControlContext());
        $this->assertSame('', $context->getResourceGroupTag());
        $this->assertSame($reference, $context->serializeToString());
    }

    public function testOverridesApplyPerField(): void
    {
        $defaults = new ResourceControl('default-group', 'default-tag');

        $groupOnly = $defaults->withOverrides(ResourceControl::fromOptions([
            'resourceGroup' => 'override-group',
        ]));
        $this->assertSame('override-group', $groupOnly->getGroupName());
        $this->assertSame('default-tag', $groupOnly->getResourceGroupTag());

        $tagOnly = $defaults->withOverrides(ResourceControl::fromOptions([
            'resourceGroupTag' => 'override-tag',
        ]));
        $this->assertSame('default-group', $tagOnly->getGroupName());
        $this->assertSame('override-tag', $tagOnly->getResourceGroupTag());
    }

    /**
     * @return iterable<string, array{array<string, mixed>, string}>
     */
    public static function invalidOptions(): iterable
    {
        yield 'group is not a string' => [['resourceGroup' => 42], "options['resourceGroup'] must be a string"];
        yield 'tag is not a string' => [['resourceGroupTag' => ['x']], "options['resourceGroupTag'] must be a string"];
        yield 'group is empty' => [['resourceGroup' => ''], "options['resourceGroup'] must not be empty"];
        yield 'tag is empty' => [['resourceGroupTag' => ''], "options['resourceGroupTag'] must not be empty"];
        yield 'group has a slash' => [['resourceGroup' => 'tenant/a'], 'Resource group name must be'];
        yield 'group has a space' => [['resourceGroup' => 'tenant a'], 'Resource group name must be'];
        yield 'group is too long' => [['resourceGroup' => str_repeat('a', 65)], 'Resource group name must be'];
    }

    /**
     * @param array<string, mixed> $options
     */
    #[DataProvider('invalidOptions')]
    public function testInvalidOptionsAreRejected(array $options, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        ResourceControl::fromOptions($options);
    }
}
