<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\Action;
use CrazyGoat\Proto\Kvrpcpb\CheckSecondaryLocksResponse;
use CrazyGoat\Proto\Kvrpcpb\CheckTxnStatusResponse;
use CrazyGoat\Proto\Kvrpcpb\LockInfo;
use CrazyGoat\Proto\Kvrpcpb\ResolveLockResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\Proto\Metapb\Region;
use CrazyGoat\Proto\Metapb\RegionEpoch;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\Proto\Pdpb\GetMembersResponse;
use CrazyGoat\Proto\Pdpb\GetRegionResponse;
use CrazyGoat\Proto\Pdpb\Member;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\Proto\Pdpb\ScanRegionsResponse;
use CrazyGoat\Proto\Pdpb\Timestamp;
use CrazyGoat\Proto\Pdpb\TsoResponse;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Every PD, TSO and lock-resolution RPC carries a finite deadline (#260).
 *
 * The three classes each have their *own* deadline, configured through
 * {@see TimeoutConfig}: `pdTimeoutMs` for the metadata RPCs, `tsoTimeoutMs`
 * for `Tso`, `lockResolveTimeoutMs` for `KvCheckTxnStatus` /
 * `KvCheckSecondaryLocks` / `KvResolveLock`. They are separate fields
 * because the failure modes are separate — a hung PD takes every region
 * lookup and every transaction begin down with it, a slow store is a
 * per-region problem the retry executor already handles — and because PD,
 * TSO and lock resolution were exactly the three places that passed **no**
 * timeout at all, so `GrpcClient::call()` gave them an unbounded
 * `Timeval::infFuture()` deadline.
 *
 * The regression is asserted on the argument the call site hands the
 * transport, because that is the only thing the previous implementation got
 * wrong. {@see testEveryGrpcCallSiteInTheThreeClassesPassesADeadline()} is
 * the exhaustive companion: a per-entry-point test can only cover the paths
 * it drives, while the source scan covers the call sites it does not.
 */
class RpcDeadlineTest extends TestCase
{
    private const PD_DEADLINE_MS = 1234;
    private const TSO_DEADLINE_MS = 2345;
    private const LOCK_RESOLVE_DEADLINE_MS = 3456;

    private const REGION_ID = 1;
    private const START_TS = 1000;
    private const TSO_PHYSICAL = 1_715_000_000_000;

    /** @var list<array{string, ?int}> method name and the timeoutMs it was given */
    private array $calls = [];

    private RegionInfo $region;

    protected function setUp(): void
    {
        $this->calls = [];
        $this->region = new RegionInfo(
            regionId: self::REGION_ID,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        );
    }

    // ========================================================================
    // PdClient
    // ========================================================================

    public function testPdClientGetRegionPassesThePdDeadline(): void
    {
        $this->pdClient('pd:2379')->getRegion('some-key');

        $this->assertDeadlineOn('GetRegion', self::PD_DEADLINE_MS);
    }

    public function testPdClientScanRegionsPassesThePdDeadline(): void
    {
        $this->pdClient('pd:2379')->scanRegions('a', 'm');

        $this->assertDeadlineOn('ScanRegions', self::PD_DEADLINE_MS);
    }

    public function testPdClientClusterIdRetryPassesThePdDeadlineOnBothAttempts(): void
    {
        $attempts = 0;
        $grpc = $this->recordingGrpc(function (string $method) use (&$attempts): Message {
            self::assertSame('GetRegion', $method);
            $attempts++;
            if ($attempts === 1) {
                // How a fresh client meets PD: cluster_id=0 is rejected and
                // the real cluster id named. The retry must not lose the
                // deadline on the way.
                throw new GrpcException('mismatch cluster id, need 999 but got 0', 2);
            }

            return $this->getRegionResponse();
        });

        (new PdClient($grpc, 'pd:2379', timeoutConfig: $this->timeoutConfig()))->getRegion('some-key');

        self::assertSame(2, $attempts, 'the cluster-id retry must have run');
        $this->assertDeadlineOn('GetRegion', self::PD_DEADLINE_MS, 2);
    }

    public function testPdClientLeaderDiscoveryPassesThePdDeadline(): void
    {
        $grpc = $this->recordingGrpc(fn(string $method): Message => $method === 'GetMembers'
            ? $this->getMembersResponse(2)
            : $this->getRegionResponse());

        // Two endpoints, so the first RPC runs the opportunistic GetMembers
        // discovery — a call site of its own.
        (new PdClient($grpc, ['pd1:2379', 'pd2:2379'], timeoutConfig: $this->timeoutConfig()))
            ->getRegion('some-key');

        $this->assertDeadlineOn('GetMembers', self::PD_DEADLINE_MS);
        $this->assertDeadlineOn('GetRegion', self::PD_DEADLINE_MS);
    }

    public function testPdClientFailoverRediscoveryPassesThePdDeadline(): void
    {
        $attempts = 0;
        $grpc = $this->recordingGrpc(function (string $method) use (&$attempts): Message {
            if ($method === 'GetMembers') {
                return $this->getMembersResponse(2);
            }
            self::assertSame('GetRegion', $method);
            $attempts++;
            if ($attempts === 1) {
                // A transport-level failure (not a cluster-id mismatch) is
                // what drives failoverAfterTransportFailure() and with it a
                // fresh GetMembers against the other endpoint.
                throw new GrpcException('failed to connect to all addresses', 14);
            }

            return $this->getRegionResponse();
        });

        (new PdClient($grpc, ['pd1:2379', 'pd2:2379'], timeoutConfig: $this->timeoutConfig()))
            ->getRegion('some-key');

        self::assertSame(2, $attempts);
        $this->assertDeadlineOn('GetRegion', self::PD_DEADLINE_MS, 2);
        $this->assertDeadlineOn('GetMembers', self::PD_DEADLINE_MS);
    }

    // ========================================================================
    // TimestampOracle
    // ========================================================================

    public function testTimestampOracleGetTimestampPassesTheTsoDeadline(): void
    {
        self::assertGreaterThan(0, $this->timestampOracle()->getTimestamp());

        $this->assertDeadlineOn('Tso', self::TSO_DEADLINE_MS);
    }

    public function testTimestampOracleLowResolutionTimestampPassesTheTsoDeadline(): void
    {
        $this->timestampOracle()->getLowResolutionTimestamp();

        $this->assertDeadlineOn('Tso', self::TSO_DEADLINE_MS);
    }

    public function testTimestampOracleBatchPassesTheTsoDeadline(): void
    {
        $this->timestampOracle()->getTimestampBatch(4);

        $this->assertDeadlineOn('Tso', self::TSO_DEADLINE_MS);
    }

    public function testTimestampOracleExplicitTimeoutOverridesTheConfiguredOne(): void
    {
        $this->timestampOracle()->getTimestamp(777);

        $this->assertDeadlineOn('Tso', 777);
    }

    public function testTimestampOracleClusterIdRetryPassesTheTsoDeadlineOnBothAttempts(): void
    {
        $attempts = 0;
        $oracle = $this->timestampOracle(function (string $method) use (&$attempts): Message {
            self::assertSame('Tso', $method);
            $attempts++;
            if ($attempts === 1) {
                throw new GrpcException('mismatch cluster id, need 7 but got 0', 2);
            }

            return $this->tsoResponse();
        });

        $oracle->getTimestamp();

        self::assertSame(2, $attempts, 'the cluster-id retry must have run');
        $this->assertDeadlineOn('Tso', self::TSO_DEADLINE_MS, 2);
    }

    public function testPdClientGetTimestampInheritsTheTsoDeadlineFromTheTimeoutConfig(): void
    {
        $client = new PdClient($this->recordingGrpc(), 'pd:2379', timeoutConfig: $this->timeoutConfig());
        $client->getTimestamp();

        $this->assertDeadlineOn('Tso', self::TSO_DEADLINE_MS);
    }

    // ========================================================================
    // LockResolver
    // ========================================================================

    public function testLockResolverCheckTxnStatusPassesTheLockResolveDeadline(): void
    {
        $this->lockResolver(1)->resolveLock('some-key', $this->lockInfo());

        $this->assertDeadlineOn('KvCheckTxnStatus', self::LOCK_RESOLVE_DEADLINE_MS);
    }

    public function testLockResolverCommittedPathPassesTheLockResolveDeadline(): void
    {
        $this->lockResolver(42)->resolveLock('some-key', $this->lockInfo());

        $this->assertDeadlineOn('KvResolveLock', self::LOCK_RESOLVE_DEADLINE_MS);
    }

    public function testLockResolverRolledBackPathPassesTheLockResolveDeadline(): void
    {
        // commitVersion 0 with no live TTL: TiKV has determined the
        // transaction, so the rollback resolve runs.
        $this->lockResolver(0)->resolveLock('some-key', $this->lockInfo());

        $this->assertDeadlineOn('KvResolveLock', self::LOCK_RESOLVE_DEADLINE_MS);
    }

    public function testLockResolverAsyncCommitPathPassesTheLockResolveDeadline(): void
    {
        $this->lockResolver(0)->resolveLock('some-key', $this->lockInfo(asyncCommit: true));

        $this->assertDeadlineOn('KvCheckSecondaryLocks', self::LOCK_RESOLVE_DEADLINE_MS);
        $this->assertDeadlineOn('KvResolveLock', self::LOCK_RESOLVE_DEADLINE_MS);
    }

    public function testLockResolverFetchesItsCurrentTsWithTheTsoDeadline(): void
    {
        $observedTsoTimeout = null;
        $pdClient = $this->pdClientMock();
        $pdClient->method('getLowResolutionTimestamp')
            ->willReturnCallback(function (?int $timeoutMs = null) use (&$observedTsoTimeout): int {
                $observedTsoTimeout = $timeoutMs;

                return self::START_TS + 1;
            });

        $this->lockResolverWith($pdClient, 1)->resolveLock('some-key', $this->lockInfo());

        self::assertSame(
            self::TSO_DEADLINE_MS,
            $observedTsoTimeout,
            'checkTxnStatus() asks PD for a timestamp before it calls the '
            . 'store; that TSO fetch must carry tsoTimeoutMs, not the store '
            . 'write timeout.',
        );
    }

    // ========================================================================
    // Exhaustiveness: no call site in the three classes may omit the deadline
    // ========================================================================

    public function testEveryGrpcCallSiteInTheThreeClassesPassesADeadline(): void
    {
        $offenders = [];
        $total = 0;

        foreach ($this->scannedClasses() as $class) {
            foreach ($this->grpcCallSites($class) as [$line, $method, $deadlineArgument]) {
                $total++;
                if ($deadlineArgument !== null) {
                    continue;
                }
                $offenders[] = sprintf(
                    '%s:%d calls GrpcClientInterface::%s() without a deadline argument',
                    $this->relative($class),
                    $line,
                    $method,
                );
            }
        }

        self::assertSame([], $offenders, implode("\n", $offenders));

        // Without this the test would also pass if the scan silently stopped
        // finding anything: the three classes carry 10 call sites today.
        self::assertSame(10, $total, 'The call-site scan found a different number '
            . 'of sites than the three classes have; the scan is broken, not the '
            . 'source.');
    }

    /**
     * The three classes the issue names, as absolute paths. Explicit rather
     * than discovered by a glob, so a rename fails the test instead of
     * quietly dropping the class from the scan.
     *
     * @return list<string>
     */
    private function scannedClasses(): array
    {
        $root = dirname(__DIR__, 3);

        return [
            $root . '/src/Client/Connection/PdClient.php',
            $root . '/src/Client/Connection/TimestampOracle.php',
            $root . '/src/Client/TxnKv/LockResolver.php',
        ];
    }

    /**
     * Every `->call()` / `->callAsync()` / `->callStreaming()` site in a file,
     * as (line, method, source text of the last top-level argument or null
     * when the call passes fewer arguments than the deadline's position).
     *
     * Token-based rather than regex-based, so a call spread over many lines
     * or containing a nested `)` is still parsed correctly.
     *
     * @return list<array{int, string, ?string}>
     */
    private function grpcCallSites(string $file): array
    {
        $code = file_get_contents($file);
        self::assertNotFalse($code, "Unable to read {$file}");

        $tokens = token_get_all($code);
        $count = count($tokens);
        $sites = [];

        for ($i = 0; $i < $count; $i++) {
            $token = $tokens[$i];
            if (!is_array($token) || $token[0] !== T_STRING) {
                continue;
            }
            $method = $token[1];
            if (!in_array($method, ['call', 'callAsync', 'callStreaming'], true)) {
                continue;
            }

            $previous = $i - 1;
            while ($previous >= 0 && is_array($tokens[$previous]) && $tokens[$previous][0] === T_WHITESPACE) {
                $previous--;
            }
            if (
                $previous < 0
                || !is_array($tokens[$previous])
                || $tokens[$previous][0] !== T_OBJECT_OPERATOR
            ) {
                continue;
            }

            $open = $i + 1;
            while ($open < $count && is_array($tokens[$open]) && $tokens[$open][0] === T_WHITESPACE) {
                $open++;
            }
            if ($open >= $count || $tokens[$open] !== '(') {
                continue;
            }

            $sites[] = [$token[2], $method, $this->deadlineArgument($tokens, $open)];
        }

        return $sites;
    }

    /**
     * Source text of a call's deadline argument, or null when the call has
     * fewer than six arguments or the sixth is a literal `null`.
     *
     * All three entry points take the deadline sixth —
     * `call()`/`callAsync()` as (address, service, method, request,
     * responseClass, timeoutMs), `callStreaming()` as (address, service,
     * method, requests, responseClass, timeoutMs) — so position six is the
     * one that matters, not "the last thing written".
     *
     * @param array<int, array{int, string, int}|string> $tokens
     */
    private function deadlineArgument(array $tokens, int $openParenthesis): ?string
    {
        $count = count($tokens);
        $depth = 0;
        $position = 0;
        $current = '';
        $arguments = [];

        for ($i = $openParenthesis; $i < $count; $i++) {
            $token = $tokens[$i];
            $text = is_array($token) ? $token[1] : $token;
            if (in_array($text, ['(', '[', '{'], true)) {
                $depth++;
                if ($depth === 1) {
                    continue;
                }
            } elseif (in_array($text, [')', ']', '}'], true)) {
                $depth--;
                if ($depth === 0) {
                    if (trim($current) !== '') {
                        $arguments[$position] = $current;
                    }

                    break;
                }
            } elseif ($text === ',' && $depth === 1) {
                $arguments[$position] = $current;
                $position++;
                $current = '';

                continue;
            }
            $current .= $text;
        }

        $deadline = trim($arguments[5] ?? '');

        // A deadline that is literally null is as bad as a missing one: null
        // is what every one of these call sites used to omit, and the
        // transport would have turned it into an unbounded deadline.
        return $deadline === '' || strcasecmp($deadline, 'null') === 0 ? null : $deadline;
    }

    private function relative(string $file): string
    {
        return str_replace(dirname(__DIR__, 3) . '/', '', $file);
    }

    // ========================================================================
    // Fixtures
    // ========================================================================

    private function timeoutConfig(): TimeoutConfig
    {
        return new TimeoutConfig(
            pdTimeoutMs: self::PD_DEADLINE_MS,
            tsoTimeoutMs: self::TSO_DEADLINE_MS,
            lockResolveTimeoutMs: self::LOCK_RESOLVE_DEADLINE_MS,
        );
    }

    /**
     * A GrpcClientInterface double that records (method, timeoutMs) for every
     * call and answers from $respond.
     *
     * @param (\Closure(string): Message)|null $respond
     */
    private function recordingGrpc(?\Closure $respond = null): GrpcClientInterface&MockObject
    {
        $respond ??= (fn(string $method): Message => match ($method) {
            'GetRegion' => $this->getRegionResponse(),
            'ScanRegions' => new ScanRegionsResponse(),
            'GetMembers' => $this->getMembersResponse(1),
            'Tso' => $this->tsoResponse(),
            default => throw new \RuntimeException("Unexpected method: {$method}"),
        });

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')
            ->willReturnCallback(function (
                string $address,
                string $service,
                string $method,
                Message $request,
                string $responseClass,
                ?int $timeoutMs = null,
            ) use ($respond): Message {
                $this->calls[] = [$method, $timeoutMs];

                return $respond($method);
            });

        return $grpc;
    }

    /**
     * @param string|list<string> $endpoints
     */
    private function pdClient(string|array $endpoints): PdClient
    {
        return new PdClient($this->recordingGrpc(), $endpoints, timeoutConfig: $this->timeoutConfig());
    }

    /**
     * @param (\Closure(string): Message)|null $respond
     */
    private function timestampOracle(?\Closure $respond = null): TimestampOracle
    {
        $respond ??= function (string $method): Message {
            self::assertSame('Tso', $method);

            return $this->tsoResponse();
        };

        return new TimestampOracle(
            $this->recordingGrpc($respond),
            'pd:2379',
            static fn (): int => 1,
            static function (int $clusterId): void {
            },
            new NullLogger(),
            tsoTimeoutMs: self::TSO_DEADLINE_MS,
        );
    }

    private function lockResolver(int $commitVersion): LockResolver
    {
        return $this->lockResolverWith($this->pdClientMock(), $commitVersion);
    }

    /**
     * A LockResolver over a recording transport that answers
     * KvCheckTxnStatus with $commitVersion, plus the store lookups its
     * RegionResolver needs.
     */
    private function lockResolverWith(PdClientInterface&MockObject $pdClient, int $commitVersion): LockResolver
    {
        $regionCache = $this->createMock(RegionCacheInterface::class);
        $regionCache->method('getByKey')->willReturn($this->region);

        $grpc = $this->recordingGrpc(fn(string $method): Message => match ($method) {
            'KvCheckTxnStatus' => $this->checkTxnStatusResponse($commitVersion),
            'KvCheckSecondaryLocks' => $this->checkSecondaryLocksResponse(),
            'KvResolveLock' => new ResolveLockResponse(),
            default => throw new \RuntimeException("Unexpected method: {$method}"),
        });

        return new LockResolver(
            $grpc,
            new RegionResolver($pdClient, $regionCache),
            $regionCache,
            $pdClient,
            self::START_TS,
            $this->timeoutConfig(),
        );
    }

    private function pdClientMock(): PdClientInterface&MockObject
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('store:20160');

        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getStore')->willReturn($store);
        $pdClient->method('getRegion')->willReturn($this->region);
        $pdClient->method('scanRegions')->willReturn([$this->region]);
        $pdClient->method('getLowResolutionTimestamp')->willReturn(self::START_TS + 1);

        return $pdClient;
    }

    private function lockInfo(bool $asyncCommit = false): LockInfo
    {
        $lock = new LockInfo();
        $lock->setKey('some-key');
        $lock->setLockVersion(self::START_TS);
        $lock->setPrimaryLock('some-key');
        if ($asyncCommit) {
            $lock->setUseAsyncCommit(true);
            $lock->setMinCommitTs(5555);
            $lock->setSecondaries(['other-key']);
        }

        return $lock;
    }

    private function checkTxnStatusResponse(int $commitVersion): CheckTxnStatusResponse
    {
        $response = new CheckTxnStatusResponse();
        $response->setCommitVersion($commitVersion);
        $response->setAction(Action::NoAction);
        $response->setLockTtl(0);

        return $response;
    }

    private function checkSecondaryLocksResponse(): CheckSecondaryLocksResponse
    {
        $response = new CheckSecondaryLocksResponse();
        // No locks returned: the transaction is determined, and the response
        // commit_ts is its final decision.
        $response->setCommitTs(5555);

        return $response;
    }

    private function getRegionResponse(): GetRegionResponse
    {
        $epoch = new RegionEpoch();
        $epoch->setConfVer(1);
        $epoch->setVersion(1);

        $region = new Region();
        $region->setId(self::REGION_ID);
        $region->setRegionEpoch($epoch);

        $leader = new Peer();
        $leader->setId(1);
        $leader->setStoreId(1);

        $header = new ResponseHeader();
        $header->setClusterId(100);

        $response = new GetRegionResponse();
        $response->setHeader($header);
        $response->setRegion($region);
        $response->setLeader($leader);

        return $response;
    }

    private function getMembersResponse(int $leaderId): GetMembersResponse
    {
        $member = new Member();
        $member->setMemberId((string) $leaderId);
        $member->setClientUrls(["pd{$leaderId}:2379"]);

        $leader = new Member();
        $leader->setMemberId((string) $leaderId);

        $response = new GetMembersResponse();
        $response->setMembers([$member]);
        $response->setLeader($leader);

        return $response;
    }

    private function tsoResponse(): TsoResponse
    {
        $timestamp = new Timestamp();
        $timestamp->setPhysical(self::TSO_PHYSICAL);
        $timestamp->setLogical(1);

        $response = new TsoResponse();
        $response->setTimestamp($timestamp);
        $response->setCount(1);

        return $response;
    }

    // ========================================================================
    // Assertions
    // ========================================================================

    /**
     * Every recorded call to $method carried $expectedMs, and none carried a
     * missing or non-positive deadline.
     *
     * @param int|null $expectedCalls when given, the exact call count expected
     */
    private function assertDeadlineOn(string $method, int $expectedMs, ?int $expectedCalls = null): void
    {
        $seen = [];
        foreach ($this->calls as [$called, $timeoutMs]) {
            if ($called !== $method) {
                continue;
            }
            $seen[] = $timeoutMs;
            self::assertNotNull(
                $timeoutMs,
                "{$method}() was issued with a null timeoutMs, which GrpcClient "
                . 'turns into an unbounded deadline (issue #260).',
            );
            self::assertGreaterThan(
                0,
                $timeoutMs,
                "{$method}() was issued with a non-positive timeoutMs (issue #260).",
            );
            self::assertSame($expectedMs, $timeoutMs, "{$method}() used the wrong deadline");
        }

        self::assertNotSame([], $seen, "{$method}() was never called — the test drove nothing");
        if ($expectedCalls !== null) {
            self::assertCount($expectedCalls, $seen);
        }
    }
}
