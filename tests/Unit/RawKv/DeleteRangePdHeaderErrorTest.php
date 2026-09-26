<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Pdpb\Error;
use CrazyGoat\Proto\Pdpb\ErrorType;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\Proto\Pdpb\ScanRegionsResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Exception\PdException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvRangeOps;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * `deleteRange()` must not report success having deleted nothing (issue
 * #234).
 *
 * The pre-fix chain: PD answers `ScanRegions` with gRPC status `OK`, an
 * `INVALID_VALUE` / `NOT_BOOTSTRAPPED` error in the response header and an
 * **empty** `region_metas` list → `PdClient::scanRegions()` returned `[]` →
 * `RawKvRangeOps::deleteRange()` iterated an empty region list, issued no
 * `RawDeleteRange` at all, and returned normally. The caller deleted a range
 * and nothing was deleted, with no exception and no warning: a correctness
 * failure, not an outage.
 *
 * Driven through the real {@see PdClient} and a real {@see RegionCache} with
 * a mocked transport, so the test proves the composition (PD response → PD
 * client → range op) rather than a stub's answer.
 */
final class DeleteRangePdHeaderErrorTest extends TestCase
{
    /** @var list<string> every RPC method the transport was asked for */
    private array $rpcMethods = [];

    public function testDeleteRangeThrowsWhenPdReportsAScanRegionsHeaderError(): void
    {
        $rangeOps = $this->rangeOps($this->errorScanRegionsResponse());

        try {
            $rangeOps->deleteRange('a', 'z');
            self::fail('deleteRange() returned normally without deleting anything');
        } catch (PdException $e) {
            $this->assertSame('ScanRegions', $e->method);
            $this->assertSame(ErrorType::NOT_BOOTSTRAPPED, $e->errorType);
            $this->assertStringContainsString('cluster is not bootstrapped', $e->getMessage());
        }

        // The store was never contacted: the failure is a metadata-plane
        // failure and must not have produced a delete that PD never asked
        // for.
        $this->assertSame(['ScanRegions'], $this->rpcMethods);
    }

    public function testDeletePrefixThrowsTheSameWayWhenPdReportsAHeaderError(): void
    {
        $rangeOps = $this->rangeOps($this->errorScanRegionsResponse());

        $this->expectException(PdException::class);
        $rangeOps->deletePrefix('pre/fix/234');
    }

    private function errorScanRegionsResponse(): ScanRegionsResponse
    {
        $error = new Error();
        $error->setType(ErrorType::NOT_BOOTSTRAPPED);
        $error->setMessage('cluster is not bootstrapped');

        $header = new ResponseHeader();
        $header->setClusterId(100);
        $header->setError($error);

        // region_metas deliberately left empty — that is exactly what PD
        // sends, and what made the old code a silent no-op.
        $response = new ScanRegionsResponse();
        $response->setHeader($header);

        return $response;
    }

    private function rangeOps(ScanRegionsResponse $scanRegionsResponse): RawKvRangeOps
    {
        $response = $scanRegionsResponse;

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use ($response): Message {
                $this->rpcMethods[] = $method;
                self::assertSame('ScanRegions', $method, 'the only PD RPC deleteRange needs is ScanRegions');

                return $response;
            },
        );
        // A delete that reached the store would be a correctness bug of its
        // own; nothing below may open a channel.
        $grpc->expects($this->never())->method('getChannel');
        $grpc->expects($this->never())->method('callAsync');

        $pdClient = new PdClient($grpc, 'pd:2379', new NullLogger());
        $regionCache = new RegionCache();

        return new RawKvRangeOps(
            $pdClient,
            $grpc,
            new RegionResolver($pdClient, $regionCache, pdEndpoints: ['pd:2379']),
            $regionCache,
            new TimeoutConfig(),
            maxBackoffMs: 100,
            serverBusyBudgetMs: 100,
            logger: new NullLogger(),
        );
    }
}
