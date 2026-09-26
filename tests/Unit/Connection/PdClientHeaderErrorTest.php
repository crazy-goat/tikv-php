<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Keyspacepb\LoadKeyspaceResponse;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Pdpb\Error;
use CrazyGoat\Proto\Pdpb\ErrorType;
use CrazyGoat\Proto\Pdpb\GetAllStoresResponse;
use CrazyGoat\Proto\Pdpb\GetMembersResponse;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\GetStoreResponse;
use CrazyGoat\Proto\Pdpb\Member;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\Proto\Pdpb\ScanRegionsResponse;
use CrazyGoat\Proto\Pdpb\UpdateServiceGCSafePointResponse;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Exception\PdException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * A PD response carrying a header error is a failure, not an empty success
 * (issue #234, REG-03).
 *
 * PD answers `NOT_BOOTSTRAPPED`, `ErrNotLeader`, `INVALID_VALUE` and friends
 * with gRPC status `OK`, an error inside `pdpb.ResponseHeader.error` and an
 * **empty payload**. Before the fix, only the two GC-safe-point methods read
 * the header, so every other method read those answers as successes: the
 * dangerous one is `scanRegions()` returning `[]`, because
 * `RawKvRangeOps::deleteRange()` then iterates an empty region list and
 * returns normally having deleted nothing — a correctness failure, not an
 * outage. These tests pin the choke point ({@see PdClient::checkHeader()})
 * for every response-producing method, the typed exception's error type, the
 * not-leader endpoint rotation, and the preserved soft-fail for an old PD
 * without service GC safe points.
 */
final class PdClientHeaderErrorTest extends TestCase
{
    /** @var list<array{address: string, method: string}> */
    private array $rpcLog = [];

    public function testScanRegionsRaisesInsteadOfReturningAnEmptyRegionList(): void
    {
        // The dangerous case: an empty region_metas list with a header error.
        // Pre-fix this was `[]`, and deleteRange() reported success.
        $response = new ScanRegionsResponse();
        $this->setHeaderError($response, ErrorType::NOT_BOOTSTRAPPED, 'cluster is not bootstrapped');

        $client = new PdClient($this->mockGrpc(['ScanRegions' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD ScanRegions failed: cluster is not bootstrapped');
        $client->scanRegions('a', 'z');
    }

    public function testGetRegionRaisesTheHeaderErrorInsteadOfTheGenericNoRegionError(): void
    {
        // Pre-fix the same response produced "PD GetRegion returned no region
        // for key", which names a region-lookup problem rather than PD's
        // actual verdict.
        $response = new GetRegionResponse();
        $this->setHeaderError($response, ErrorType::INVALID_VALUE, 'invalid key range');

        $client = new PdClient($this->mockGrpc(['GetRegion' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD GetRegion failed: invalid key range');
        $client->getRegion('k');
    }

    public function testGetStoreRaisesInsteadOfReturningNull(): void
    {
        // A null store is what RegionResolver turns into
        // StoreNotFoundException — a "this store does not exist" verdict for
        // a store PD never said anything about.
        $response = new GetStoreResponse();
        $this->setHeaderError($response, ErrorType::REGION_NOT_FOUND, 'region not found');

        $client = new PdClient($this->mockGrpc(['GetStore' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD GetStore failed: region not found');
        $client->getStore(1);
    }

    public function testGetAllStoresRaisesInsteadOfReturningAnEmptyStoreList(): void
    {
        $response = new GetAllStoresResponse();
        $this->setHeaderError($response, ErrorType::NOT_BOOTSTRAPPED, 'cluster is not bootstrapped');

        $client = new PdClient($this->mockGrpc(['GetAllStores' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD GetAllStores failed');
        $client->getAllStores();
    }

    public function testGetKeyspaceIdRaisesInsteadOfTheGenericNoKeyspaceError(): void
    {
        $response = new LoadKeyspaceResponse();
        $this->setHeaderError($response, ErrorType::ENTRY_NOT_FOUND, 'keyspace not found');

        $client = new PdClient($this->mockGrpc(['LoadKeyspace' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD LoadKeyspace failed: keyspace not found');
        $client->getKeyspaceId('nope');
    }

    public function testPingRaisesInsteadOfReturningAClusterIdFromAnErrorResponse(): void
    {
        $response = new GetMembersResponse();
        $this->setHeaderError($response, ErrorType::NOT_BOOTSTRAPPED, 'cluster is not bootstrapped');

        $client = new PdClient($this->mockGrpc(['GetMembers' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD GetMembers failed: cluster is not bootstrapped');
        $client->ping();
    }

    public function testTypedHeaderErrorWithAnEmptyMessageIsStillAnError(): void
    {
        // A pdpb.Error with a non-OK type and no text is still an error:
        // reading it as a success is what fabricated a min safe point of 0
        // (#499) and would fabricate an empty region list here.
        $response = new ScanRegionsResponse();
        $this->setHeaderError($response, ErrorType::INVALID_VALUE, '');

        $client = new PdClient($this->mockGrpc(['ScanRegions' => $response]), 'pd:2379');

        try {
            $client->scanRegions('a', 'z');
            self::fail('expected a PdException for a typed header error with an empty message');
        } catch (PdException $e) {
            $this->assertSame('ScanRegions', $e->method);
            $this->assertSame(ErrorType::INVALID_VALUE, $e->errorType);
            $this->assertSame('', $e->pdErrorMessage);
            $this->assertStringContainsString('unknown PD error', $e->getMessage());
        }
    }

    public function testTheErrorTypeIsSurfacedOnTheException(): void
    {
        // The type is what makes the failures distinguishable: a not-leader
        // rejection is worth another endpoint, a NOT_BOOTSTRAPPED one is
        // not, and neither is an INVALID_VALUE.
        $response = new ScanRegionsResponse();
        $this->setHeaderError($response, ErrorType::NOT_BOOTSTRAPPED, 'cluster is not bootstrapped');

        $client = new PdClient($this->mockGrpc(['ScanRegions' => $response]), 'pd:2379');

        try {
            $client->scanRegions('a', 'z');
            self::fail('expected a PdException');
        } catch (PdException $e) {
            $this->assertSame(ErrorType::NOT_BOOTSTRAPPED, $e->errorType);
            $this->assertSame('NOT_BOOTSTRAPPED', $e->getErrorTypeName());
            $this->assertFalse($e->isNotLeader());
            // A typed PD failure is a TiKvException like every other library
            // failure, so existing catch blocks keep working.
            $this->assertInstanceOf(TiKvException::class, $e);
        }
    }

    public function testAnUnknownErrorTypeValueRendersAsItsNumber(): void
    {
        $exception = new PdException('GetRegion', 4242, 'future error type');

        $this->assertSame('4242', $exception->getErrorTypeName());
        $this->assertSame('PD GetRegion failed: future error type', $exception->getMessage());
    }

    public function testANotLeaderHeaderErrorRotatesToTheNextEndpointAndRetries(): void
    {
        $memberCalls = 0;
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use (&$memberCalls): Message {
                $this->rpcLog[] = ['address' => $address, 'method' => $method];

                if ($method === 'GetMembers') {
                    $memberCalls++;
                    // A stale membership view for the first discovery (it
                    // still names pd1); the re-discovery after the not-leader
                    // rejection reports the leader that actually moved.
                    $leaderId = $memberCalls === 1 ? 1 : 2;

                    return $this->membersResponse($leaderId);
                }

                if ($address === 'pd1:2379') {
                    $response = new GetRegionResponse();
                    // PD's own wording for a follower refusing a leader-only
                    // RPC (server.validateRequest). pdpb.ErrorType has no
                    // not-leader member, so this is the signal.
                    $this->setHeaderError(
                        $response,
                        ErrorType::UNKNOWN,
                        'not leader. leader is pd-2',
                    );

                    return $response;
                }

                return $this->regionResponse();
            },
        );

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());
        $region = $client->getRegion('k');

        $this->assertSame(42, $region->regionId);
        $this->assertSame(['pd1:2379', 'pd2:2379'], $this->rpcCalls('GetRegion'));
        $this->assertSame('pd2:2379', $client->currentPdAddress());
    }

    public function testANotLeaderHeaderErrorOnEveryEndpointTriesEachEndpointOnceThenThrows(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            if ($method === 'GetMembers') {
                return $this->membersResponse(1);
            }

            $response = new GetRegionResponse();
            $this->setHeaderError($response, ErrorType::UNKNOWN, 'not leader. leader is pd-3');

            return $response;
        });

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());

        try {
            $client->getRegion('k');
            self::fail('expected a PdException once every endpoint refused with not-leader');
        } catch (PdException $e) {
            $this->assertSame('GetRegion', $e->method);
            $this->assertStringContainsString('not leader', $e->getMessage());
        }

        // "At most once per configured endpoint" — the same budget the
        // transport failover uses, so rotation cannot loop forever.
        $this->assertSame(['pd1:2379', 'pd2:2379'], $this->rpcCalls('GetRegion'));
    }

    public function testANonLeaderHeaderErrorIsNotRetried(): void
    {
        // NOT_BOOTSTRAPPED cannot be satisfied by another endpoint, so
        // rotating would only multiply the failure (and the RPC load) on a
        // cluster that is not coming up.
        $response = new GetRegionResponse();
        $this->setHeaderError($response, ErrorType::NOT_BOOTSTRAPPED, 'cluster is not bootstrapped');

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use ($response): Message {
                $this->rpcLog[] = ['address' => $address, 'method' => $method];

                return $response;
            },
        );

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());

        try {
            $client->getRegion('k');
            self::fail('expected a PdException');
        } catch (PdException $e) {
            $this->assertSame(ErrorType::NOT_BOOTSTRAPPED, $e->errorType);
        }

        $this->assertSame(['pd1:2379'], $this->rpcCalls('GetRegion'));
    }

    public function testPingRecordsTheLeaderUrlFromItsOwnGetMembersResponse(): void
    {
        // ping() issues GetMembers — the RPC whose purpose is to report the
        // leader — and must record what it learns. Discovery before it named
        // pd1; by the time ping's own answer arrives the leader is pd2.
        $memberCalls = 0;
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use (&$memberCalls): Message {
                $this->rpcLog[] = ['address' => $address, 'method' => $method];
                $memberCalls++;

                return $this->membersResponse($memberCalls === 1 ? 1 : 2);
            },
        );

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());
        $this->assertSame(100, $client->ping());
        $this->assertSame('pd2:2379', $client->currentPdAddress());
    }

    public function testPingIgnoresALeaderUrlThatIsNotAConfiguredEndpoint(): void
    {
        // PD advertises member URLs we were never configured with (a NAT'd
        // or rewritten address, or a rogue PD). Adopting one would defeat
        // the configured-endpoint validation, so the recorded leader must be
        // one of ours or nothing.
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            // Member 1 is the leader and advertises an address we were never
            // configured with.
            return $this->membersResponse(1, ['evil.example:2379']);
        });

        $client = new PdClient($grpc, 'pd1:2379', new NullLogger());
        $client->ping();

        $this->assertSame('pd1:2379', $client->currentPdAddress());
    }

    public function testUpdateServiceGcSafePointStillSoftFailsOnAnUnsupportedFeatureHeaderError(): void
    {
        // A cluster without service GC safe points is a supported
        // configuration, so the soft-fail must survive the move of the check
        // into the choke point: the failure now arrives as a PdException
        // rather than a response the method inspects itself.
        $response = new UpdateServiceGCSafePointResponse();
        $this->setHeaderError($response, ErrorType::INVALID_VALUE, 'UpdateServiceGCSafePoint is not supported');

        $client = new PdClient($this->mockGrpc(['UpdateServiceGCSafePoint' => $response]), 'pd:2379');

        $this->assertNull($client->updateServiceGCSafePoint('worker-1', 1, 600));
    }

    public function testUpdateServiceGcSafePointStillFailsClosedOnAnyOtherHeaderError(): void
    {
        $response = new UpdateServiceGCSafePointResponse();
        $this->setHeaderError(
            $response,
            ErrorType::INVALID_VALUE,
            'service safe point is smaller than current GC safe point',
        );

        $client = new PdClient($this->mockGrpc(['UpdateServiceGCSafePoint' => $response]), 'pd:2379');

        $this->expectException(PdException::class);
        $this->expectExceptionMessage('PD UpdateServiceGCSafePoint failed: service safe point is smaller');
        $client->updateServiceGCSafePoint('worker-1', 1, 600);
    }

    public function testAResponseWithoutAHeaderErrorIsNotAffected(): void
    {
        // The choke point must not turn every PD answer into a failure: an
        // ordinary successful response (header present, no error) still works.
        $client = new PdClient($this->mockGrpc(['GetRegion' => $this->regionResponse()]), 'pd:2379');

        $this->assertSame(42, $client->getRegion('k')->regionId);
        $this->assertSame(['pd:2379'], $this->rpcCalls('GetRegion'));
    }

    /**
     * Attach a pdpb.ResponseHeader carrying an error to a PD response.
     *
     * The union in @param is what makes `setHeader()` a known call here
     * rather than a `method_exists()` dance on a bare Message.
     *
     * @param GetAllStoresResponse|GetMembersResponse|GetRegionResponse
     *        |GetStoreResponse|LoadKeyspaceResponse|ScanRegionsResponse
     *        |UpdateServiceGCSafePointResponse $response
     * @param int $type pdpb.ErrorType value
     */
    private function setHeaderError(Message $response, int $type, string $message): void
    {
        $error = new Error();
        $error->setType($type);
        $error->setMessage($message);

        $header = new ResponseHeader();
        $header->setClusterId(100);
        $header->setError($error);

        $response->setHeader($header);
    }

    /**
     * @param list<string> $urls client URLs of the members, one per member id
     *        starting at 1; defaults to `['pd1:2379', 'pd2:2379']`
     */
    private function membersResponse(int $leaderId, ?array $urls = null): GetMembersResponse
    {
        $urls ??= ['pd1:2379', 'pd2:2379'];

        $response = new GetMembersResponse();

        $leader = new Member();
        $leader->setMemberId($leaderId);
        $response->setLeader($leader);

        $members = [];
        foreach ($urls as $index => $url) {
            $member = new Member();
            $member->setMemberId($index + 1);
            $member->setClientUrls([$url]);
            $members[] = $member;
        }
        $response->setMembers($members);

        $header = new ResponseHeader();
        $header->setClusterId(100);
        $response->setHeader($header);

        return $response;
    }

    private function regionResponse(): GetRegionResponse
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(10);

        $region = new Region();
        $region->setId(42);
        $region->setRegionEpoch($epoch);

        $response = new GetRegionResponse();
        $header = new ResponseHeader();
        $header->setClusterId(100);
        $response->setHeader($header);
        $response->setRegion($region);

        return $response;
    }

    /**
     * @param array<string, Message> $responses
     */
    private function mockGrpc(array $responses): GrpcClientInterface
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use ($responses): Message {
                $this->rpcLog[] = ['address' => $address, 'method' => $method];
                if (!isset($responses[$method])) {
                    throw new \LogicException("Unexpected PD method: $method");
                }

                return $responses[$method];
            },
        );

        return $grpc;
    }

    /** @return list<string> */
    private function rpcCalls(string $method): array
    {
        $addresses = [];
        foreach ($this->rpcLog as $entry) {
            if ($entry['method'] === $method) {
                $addresses[] = $entry['address'];
            }
        }

        return $addresses;
    }
}
