<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #329 (TEST-09): keys whose region cannot be resolved must never be
 * silently dropped from a batch write — RawKvBatch::batchPut() returns void,
 * so a skipped key means the write was never sent while the caller observed
 * success (silent data loss). The fail-closed contract is issue #244.
 */
final class RawKvBatchUnresolvedKeyTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;
    private RawKvBatch $batch;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);

        $this->batch = new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    /**
     * Issue #244 redacts keys in fail-closed messages (KeyRedactor
     * convention): the unresolvable key is identified by its truncated keyed
     * hash plus byte length, never the raw bytes.
     */
    private function expectedRedactedKeyFragment(string $key): string
    {
        return KeyRedactor::redact($key);
    }

    private function region(int $id, string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: $id,
            leaderPeerId: $id,
            leaderStoreId: $id,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    private function createRetryExecutor(): RetryExecutor
    {
        return new RetryExecutor(
            20000,
            600000,
            $this->regionCache,
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new NullLogger(),
        );
    }

    /**
     * Issue #329 criterion: batchPut must either send every key or raise an
     * exception — never return normally having sent only a subset.
     *
     * scanRegions deliberately returns a window that does not cover 'z'
     * (e.g. a stale PD window or the boundary scan bug fixed by #244). The
     * unresolvable key must surface as a typed failure, not vanish.
     */
    public function testBatchPutThrowsWhenAKeyCannotBeResolved(): void
    {
        $this->pdClient->method('scanRegions')->willReturn([$this->region(1, 'a', 'm')]);
        $this->regionCache->method('put');
        $this->grpc->expects($this->never())->method('getChannel');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage(
            $this->expectedRedactedKeyFragment('z') . '; refusing to silently drop',
        );

        $this->batch->batchPut(
            ['a' => '1', 'z' => '2'],
            60,
            $this->createRetryExecutor(),
        );
    }

    /**
     * A resolvable neighbour must not mask the unresolvable key: the call
     * still fails closed and the message names the key that could not be
     * resolved.
     */
    public function testBatchPutReportsUnresolvableKeyAmongResolvableNeighbours(): void
    {
        $this->pdClient->method('scanRegions')->willReturn([$this->region(1, 'a', 'm')]);
        $this->regionCache->method('put');

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage($this->expectedRedactedKeyFragment('z'));

        $this->batch->batchPut(
            ['a' => '1', 'b' => '2', 'z' => '3'],
            60,
            $this->createRetryExecutor(),
        );
    }
}
