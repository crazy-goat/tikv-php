<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Pdpb\GetMembersResponse;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\Member;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Multi-endpoint PD failover (issue #416, GAP-02): leader discovery via
 * GetMembers, failover on transport failure, and TSO following the leader.
 */
class PdClientFailoverTest extends TestCase
{
    /** @var list<array{address: string, method: string}> */
    private array $rpcLog = [];

    public function testPrefersDiscoveredLeaderOverFirstConfiguredEndpoint(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            if ($method === 'GetMembers') {
                return $this->membersResponse(
                    leaderId: 2,
                    members: [[2, ['pd2:2379']], [1, ['pd1:2379']]],
                );
            }

            return $this->regionResponse();
        });

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());
        $client->getRegion('k');

        $this->assertSame('pd2:2379', $client->currentPdAddress());
        $calls = $this->rpcCalls('GetRegion');
        $this->assertCount(1, $calls);
        $this->assertSame('pd2:2379', $calls[0]);
    }

    public function testTransportFailureTriggersRediscoveryAndRetryAgainstAnotherMember(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            if ($method === 'GetMembers') {
                // Discovery keeps reporting pd1 as leader (e.g. stale
                // membership view); pd1 does not answer anyway.
                return $this->membersResponse(
                    leaderId: 1,
                    members: [[1, ['pd1:2379']], [2, ['pd2:2379']]],
                );
            }

            // GetRegion: the first attempt (against pd1, the reported
            // leader) dies on transport level; later attempts succeed.
            if ($address === 'pd1:2379') {
                throw new GrpcException('unavailable', 14);
            }

            return $this->regionResponse();
        });

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());
        $region = $client->getRegion('k');

        $this->assertSame(42, $region->regionId);

        // First GetRegion attempt on the (reported) leader pd1, then a
        // successful retry on pd2.
        $this->assertSame(['pd1:2379', 'pd2:2379'], $this->rpcCalls('GetRegion'));
    }

    public function testLeaderChangeIsFollowedOnSubsequentCalls(): void
    {
        $memberCalls = 0;
        $regionCalls = 0;
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(
            function (string $address, string $service, string $method) use (&$memberCalls, &$regionCalls): Message {
                $this->rpcLog[] = ['address' => $address, 'method' => $method];

                if ($method === 'GetMembers') {
                    $memberCalls++;
                    // First discovery reports pd1 as leader; later discovery
                    // calls report pd2 — the leader has changed.
                    $leaderId = $memberCalls === 1 ? 1 : 2;

                    return $this->membersResponse(
                        leaderId: $leaderId,
                        members: [[$leaderId, ["pd{$leaderId}:2379"]], [$leaderId === 1 ? 2 : 1, ['pd2:2379']]],
                    );
                }

                $regionCalls++;
                if ($address === 'pd1:2379' && $regionCalls > 1) {
                    // The old leader now refuses connections.
                    throw new GrpcException('unavailable', 14);
                }

                return $this->regionResponse();
            },
        );

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());

        // First call: leader is pd1, discovery confirms it.
        $client->getRegion('k');
        $this->assertSame('pd1:2379', $client->currentPdAddress());

        // Leader moves to pd2; the next call fails over and stays there.
        $this->rpcLog = [];
        $client->getRegion('k');

        $this->assertSame('pd2:2379', $client->currentPdAddress());
        $this->assertSame(['pd1:2379', 'pd2:2379'], $this->rpcCalls('GetRegion'));
    }

    public function testMemberUnavailableDuringDiscoveryFallsThroughToNextMember(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            if ($method === 'GetMembers') {
                // The (dead) default endpoint cannot answer discovery either;
                // the second member answers with itself as leader.
                if ($address === 'pd1:2379') {
                    throw new GrpcException('unavailable', 14);
                }

                return $this->membersResponse(
                    leaderId: 2,
                    members: [[2, ['pd2:2379']], [1, ['pd1:2379']]],
                );
            }

            throw new GrpcException('unavailable', 14);
        });

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());

        try {
            $client->getRegion('k');
            $this->fail('Expected GrpcException');
        } catch (GrpcException $e) {
            $this->assertSame(14, $e->grpcStatusCode);
        }

        // Discovery: the first pass fell through the dead pd1 to pd2, which
        // took over as GetRegion target; the failover after the pd2 GetRegion
        // failure re-probed pd1 (still dead) before giving up.
        $this->assertSame(['pd1:2379', 'pd2:2379', 'pd1:2379'], $this->rpcCalls('GetMembers'));
        $this->assertSame(['pd2:2379'], $this->rpcCalls('GetRegion'));
    }

    public function testAllMembersDownPropagatesOriginalTransportFailure(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            throw new GrpcException('unavailable', 14);
        });

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());

        try {
            $client->getRegion('k');
            $this->fail('Expected GrpcException');
        } catch (GrpcException $e) {
            $this->assertSame(14, $e->grpcStatusCode);
        }

        // Opportunistic discovery tried pd1 (down) then pd2 (down) before the
        // GetRegion attempt on pd1; failover rediscovery retried pd2 once more.
        $this->assertCount(4, $this->rpcLog);
        $this->assertSame(['pd1:2379'], $this->rpcCalls('GetRegion'));
        $this->assertSame(['pd1:2379', 'pd2:2379', 'pd2:2379'], $this->rpcCalls('GetMembers'));
    }

    public function testTimestampOracleFollowsTheSameLeaderAfterFailover(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            if ($method === 'GetMembers') {
                return $this->membersResponse(
                    leaderId: 2,
                    members: [[2, ['pd2:2379']], [1, ['pd1:2379']]],
                );
            }

            if ($method === 'GetRegion' && $address === 'pd1:2379') {
                throw new GrpcException('unavailable', 14);
            }

            if ($method === 'GetRegion') {
                return $this->regionResponse();
            }

            // Tso
            $ts = new \CrazyGoat\Proto\Pdpb\Timestamp();
            $ts->setPhysical(1715000000000);
            $ts->setLogical(5);

            $response = new \CrazyGoat\Proto\Pdpb\TsoResponse();
            $response->setTimestamp($ts);

            return $response;
        });

        $client = new PdClient($grpc, ['pd1:2379', 'pd2:2379'], new NullLogger());
        $client->getRegion('k');

        $this->rpcLog = [];
        $client->getTimestamp();

        $tsoCalls = $this->rpcCalls('Tso');
        $this->assertSame(['pd2:2379'], $tsoCalls);
    }

    public function testSingleEndpointNeverRunsDiscovery(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnCallback(function (string $address, string $service, string $method): Message {
            $this->rpcLog[] = ['address' => $address, 'method' => $method];

            return $this->regionResponse();
        });

        $client = new PdClient($grpc, 'pd1:2379', new NullLogger());
        $client->getRegion('k');

        $this->assertSame([], $this->rpcCalls('GetMembers'));
        $this->assertSame(['pd1:2379'], $this->rpcCalls('GetRegion'));
    }

    /**
     * @param list<array{0: int|string, 1: list<string>}> $members
     */
    private function membersResponse(int|string $leaderId, array $members): GetMembersResponse
    {
        $response = new GetMembersResponse();

        $leader = new Member();
        $leader->setMemberId($leaderId);
        $response->setLeader($leader);

        $protos = [];
        foreach ($members as [$memberId, $urls]) {
            $member = new Member();
            $member->setMemberId($memberId);
            $member->setClientUrls($urls);
            $protos[] = $member;
        }
        $response->setMembers($protos);

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

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);

        return $response;
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
