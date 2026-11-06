<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\Proto\Kvrpcpb\IsolationLevel;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionContextFactory;
use CrazyGoat\TiKV\Client\Region\ReplicaReadMode;
use CrazyGoat\TiKV\Client\Region\ReplicaReadPolicy;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\Region\ResourceControl;
use PHPUnit\Framework\TestCase;

class RequestContextOptionsTest extends TestCase
{
    private function region(): RegionInfo
    {
        return new RegionInfo(
            regionId: 42,
            leaderPeerId: 7,
            leaderStoreId: 3,
            epochConfVer: 1,
            epochVersion: 10,
        );
    }

    public function testEveryOptionReachesTheSerializedContext(): void
    {
        $options = new RequestContextOptions(
            isolationLevel: IsolationLevel::RC,
            notFillCache: true,
            maxExecutionDurationMs: 2500,
            requestSource: 'tikv-php-test',
            resourceGroupTag: 'nightly-export',
            busyThresholdMs: 500,
            resourceGroupName: 'analytics',
        );

        $context = RegionContextFactory::fromRegionInfo($this->region(), requestContext: $options);

        $this->assertSame(IsolationLevel::RC, $context->getIsolationLevel());
        $this->assertTrue($context->getNotFillCache());
        $this->assertSame(2500, $context->getMaxExecutionDurationMs());
        $this->assertSame('tikv-php-test', $context->getRequestSource());
        $this->assertSame('nightly-export', $context->getResourceGroupTag());
        $this->assertSame(500, $context->getBusyThresholdMs());
        $resourceControlContext = $context->getResourceControlContext();
        $this->assertNotNull($resourceControlContext);
        $this->assertSame('analytics', $resourceControlContext->getResourceGroupName());

        $json = $context->serializeToJsonString();
        $this->assertStringContainsString('"isolationLevel":"RC"', $json);
        $this->assertStringContainsString('"notFillCache":true', $json);
        $this->assertStringContainsString('"maxExecutionDurationMs":"2500"', $json);
        $this->assertStringContainsString('"requestSource":"tikv-php-test"', $json);
        $this->assertStringContainsString('"resourceGroupTag":"bmlnaHRseS1leHBvcnQ="', $json);
        $this->assertStringContainsString('"busyThresholdMs":500', $json);
        $this->assertStringContainsString('"resourceGroupName":"analytics"', $json);
    }

    public function testDefaultOptionsLeaveTheSerializedContextUnchanged(): void
    {
        $baseline = RegionContextFactory::fromRegionInfo($this->region());
        $withDefaults = RegionContextFactory::fromRegionInfo(
            $this->region(),
            requestContext: new RequestContextOptions(),
        );

        $this->assertSame($baseline->serializeToString(), $withDefaults->serializeToString());

        $json = $withDefaults->serializeToJsonString();
        $this->assertStringNotContainsString('isolationLevel', $json);
        $this->assertStringNotContainsString('notFillCache', $json);
        $this->assertStringNotContainsString('maxExecutionDurationMs', $json);
        $this->assertStringNotContainsString('requestSource', $json);
        $this->assertStringNotContainsString('resourceGroupTag', $json);
        $this->assertStringNotContainsString('busyThresholdMs', $json);
        $this->assertStringNotContainsString('resourceControlContext', $json);
    }

    public function testDefaultRequestSourceIsOptIn(): void
    {
        // The constant exists so callers do not have to invent a label, but
        // it is not applied by default: that would change the wire bytes of
        // every request that never asked for attribution (issue #426).
        $this->assertSame('', (new RequestContextOptions())->requestSource);

        $context = RegionContextFactory::fromRegionInfo(
            $this->region(),
            requestContext: new RequestContextOptions(requestSource: RequestContextOptions::DEFAULT_REQUEST_SOURCE),
        );
        $this->assertSame('tikv-php', $context->getRequestSource());
    }

    public function testResolveTargetCarriesTheOptions(): void
    {
        $options = new RequestContextOptions(notFillCache: true, requestSource: 'scan-worker');

        $target = RegionContextFactory::resolveTarget(
            $this->region(),
            new ReplicaReadPolicy(mode: ReplicaReadMode::Leader),
            static fn (): null => null,
            requestContext: $options,
        );

        $this->assertTrue($target->context->getNotFillCache());
        $this->assertSame('scan-worker', $target->context->getRequestSource());
    }

    public function testIsolationLevelMustBeAKnownEnumValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RequestContextOptions(isolationLevel: 99);
    }

    public function testMaxExecutionDurationMustBeInRange(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RequestContextOptions(maxExecutionDurationMs: -1);
    }

    public function testBusyThresholdMustBeInRange(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new RequestContextOptions(busyThresholdMs: RequestContextOptions::MAX_DURATION_MS + 1);
    }

    public function testResourceControlAndRequestContextAreBothApplied(): void
    {
        // The two option objects cover overlapping Context fields (issue #426
        // and #427). Both must reach the wire when a client uses both; the
        // resource-control value is applied first, so a non-default value of
        // the same field in requestContext wins.
        $resourceControl = new ResourceControl('tenant-a', 'nightly-job');
        $requestContext = new RequestContextOptions(
            notFillCache: true,
            requestSource: 'nightly-export',
        );

        $context = RegionContextFactory::fromRegionInfo(
            $this->region(),
            requestContext: $requestContext,
            resourceControl: $resourceControl,
        );

        $this->assertTrue($context->getNotFillCache());
        $this->assertSame('nightly-export', $context->getRequestSource());
        $this->assertSame('nightly-job', $context->getResourceGroupTag());
        $this->assertSame(
            'tenant-a',
            $context->getResourceControlContext()?->getResourceGroupName(),
        );

        $override = new RequestContextOptions(
            resourceGroupTag: 'context-tag',
            resourceGroupName: 'context-group',
        );
        $context = RegionContextFactory::fromRegionInfo(
            $this->region(),
            requestContext: $override,
            resourceControl: $resourceControl,
        );

        $this->assertSame('context-tag', $context->getResourceGroupTag());
        $this->assertSame(
            'context-group',
            $context->getResourceControlContext()?->getResourceGroupName(),
        );
    }
}
