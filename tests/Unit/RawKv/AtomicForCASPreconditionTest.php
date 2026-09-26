<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Kvrpcpb\RawCASResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\RawKv\CasResult;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * The atomic-mode precondition of compareAndSwap() / putIfAbsent() (issue #367).
 *
 * The flag is off by default, so both methods throw InvalidStateException
 * against a freshly created client — which is why every CAS example in
 * docs/operations.md, docs/advanced.md and docs/troubleshooting.md has to call
 * setAtomicForCAS(true) first. Without these tests the documentation gap
 * survives: the E2E suite enables atomic mode for every test in setUp(), and
 * the pre-existing RawKvClientTest coverage (Grpc suite) only asserted a
 * prefix of the message.
 *
 * Lives here rather than in RawKvClientTest because the guard is a
 * client-side check made before any RPC: only GrpcClientInterface is mocked
 * (never \Grpc\Call), so the file needs no ext-grpc and therefore also runs
 * under `--testsuite Unit`, which does not install the extension.
 */
class AtomicForCASPreconditionTest extends TestCase
{
    /**
     * Verbatim InvalidStateException message thrown by compareAndSwap()
     * (and therefore by putIfAbsent()) while atomic mode is off. The docs
     * quote it as-is so grepping a log for it finds the section that
     * explains the fix — keep this constant in sync with
     * RawKvClient::compareAndSwap().
     */
    private const ATOMIC_MODE_MESSAGE = 'CompareAndSwap requires atomic mode (enable via setAtomicForCAS(true))';

    private PdClientInterface&MockObject $pdClient;
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private RawKvClient $client;

    protected function setUp(): void
    {
        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->client = new RawKvClient($this->pdClient, $this->grpc, $this->regionCache);
    }

    private function stubRegionLookup(): void
    {
        $region = new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        $this->regionCache->method('getByKey')->willReturn(null);
        $this->regionCache->method('put');
        $this->pdClient->method('getRegion')->willReturn($region);
        $this->pdClient->method('getStore')->willReturn($store);
    }

    private function casResponse(bool $succeeded, ?string $previousValue): RawCASResponse
    {
        $response = new RawCASResponse();
        $response->setSucceed($succeeded);

        if ($previousValue === null) {
            $response->setPreviousNotExist(true);

            return $response;
        }

        $response->setPreviousNotExist(false);
        $response->setPreviousValue($previousValue);

        return $response;
    }

    /**
     * Run an atomic operation and turn the atomic-mode guard into a test
     * failure instead of letting it pass unnoticed: a caller that already
     * called setAtomicForCAS(true) must never see this exception.
     *
     * @param callable(): mixed $operation
     */
    private function assertAtomicModeGuardDoesNotFire(callable $operation): mixed
    {
        try {
            return $operation();
        } catch (InvalidStateException $e) {
            self::fail(
                'atomic-mode guard fired although atomic mode was enabled: ' . $e->getMessage(),
            );
        }
    }

    public function testAtomicModeIsDisabledByDefault(): void
    {
        $this->assertFalse($this->client->isAtomicForCAS());
    }

    public function testSetAtomicForCASIsFluentAndReadable(): void
    {
        $this->assertSame($this->client, $this->client->setAtomicForCAS(true));
        $this->assertTrue($this->client->isAtomicForCAS());
        $this->assertFalse($this->client->setAtomicForCAS(false)->isAtomicForCAS());
    }

    public function testCompareAndSwapThrowsVerbatimMessageWithoutAtomicMode(): void
    {
        $this->stubRegionLookup();
        // The guard fires before any RPC, so nothing may be sent.
        $this->grpc->expects($this->never())->method('call');

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage(self::ATOMIC_MODE_MESSAGE);

        $this->client->compareAndSwap('key', 'old', 'new');
    }

    public function testPutIfAbsentThrowsVerbatimMessageWithoutAtomicMode(): void
    {
        $this->stubRegionLookup();
        $this->grpc->expects($this->never())->method('call');

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage(self::ATOMIC_MODE_MESSAGE);

        $this->client->putIfAbsent('key', 'value');
    }

    public function testCompareAndSwapReachesTheRpcWithAtomicModeEnabled(): void
    {
        $this->stubRegionLookup();
        $this->client->setAtomicForCAS(true);

        // Proves the call is dispatched to RawCompareAndSwap, i.e. the guard
        // let it through instead of throwing.
        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                'tikvpb.Tikv',
                'RawCompareAndSwap',
                $this->anything(),
                RawCASResponse::class,
                $this->anything(),
            )
            ->willReturn($this->casResponse(true, 'old'));

        $result = $this->assertAtomicModeGuardDoesNotFire(
            fn(): mixed => $this->client->compareAndSwap('key', 'old', 'new'),
        );

        $this->assertInstanceOf(CasResult::class, $result);
        $this->assertTrue($result->swapped);
        $this->assertSame('old', $result->previousValue);
    }

    public function testPutIfAbsentReachesTheRpcWithAtomicModeEnabled(): void
    {
        $this->stubRegionLookup();
        $this->client->setAtomicForCAS(true);

        $this->grpc->expects($this->once())
            ->method('call')
            ->with(
                $this->anything(),
                'tikvpb.Tikv',
                'RawCompareAndSwap',
                $this->anything(),
                RawCASResponse::class,
                $this->anything(),
            )
            ->willReturn($this->casResponse(true, null));

        $this->assertNull($this->assertAtomicModeGuardDoesNotFire(
            fn(): mixed => $this->client->putIfAbsent('key', 'value'),
        ));
    }

    public function testDisablingAtomicModeReArmsTheGuard(): void
    {
        $this->stubRegionLookup();
        $this->client->setAtomicForCAS(false);

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage(self::ATOMIC_MODE_MESSAGE);

        $this->client->putIfAbsent('key', 'value');
    }
}
