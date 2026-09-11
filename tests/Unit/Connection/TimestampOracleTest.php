<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Pdpb\Timestamp;
use CrazyGoat\Proto\Pdpb\TsoRequest;
use CrazyGoat\Proto\Pdpb\TsoResponse;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class TimestampOracleTest extends TestCase
{
    public function testGetTimestampReturnsComposedTimestamp(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(5);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc->method('call')->willReturn($response);

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 5;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampWithZeroLogical(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(0);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc->method('call')->willReturn($response);

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 0;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampThrowsTiKvExceptionWhenTimestampNull(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $response = new TsoResponse();

        $grpc->method('call')->willReturn($response);

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO response missing timestamp');
        $oracle->getTimestamp();
    }

    public function testGetTimestampThrowsOnGrpcExceptionInsteadOfFabricating(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO request failed');
        $oracle->getTimestamp();
    }

    public function testGetTimestampPreservesGrpcStatusCodeOnFailure(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );

        try {
            $oracle->getTimestamp();
            $this->fail('Expected TiKvException to be thrown');
        } catch (TiKvException $e) {
            $this->assertSame(14, $e->getCode());
            $this->assertInstanceOf(GrpcException::class, $e->getPrevious());
        }
    }

    public function testGetTimestampLogsErrorOnGrpcException(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('tso unavailable', 14));

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('error')
            ->with(
                $this->stringContains('refusing to fabricate'),
                $this->callback(
                    fn(array $context): bool => ($context['error'] ?? null) === 'gRPC error: tso unavailable'
                        && ($context['grpcStatusCode'] ?? null) === 14,
                ),
            );

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            $logger,
        );

        try {
            $oracle->getTimestamp();
            $this->fail('Expected TiKvException to be thrown');
        } catch (TiKvException) {
        }
    }

    public function testGetTimestampForwardsTimeoutToGrpcCall(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(3);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc->expects($this->once())
            ->method('call')
            ->with(
                '127.0.0.1:2379',
                'pdpb.PD',
                'Tso',
                $this->isInstanceOf(TsoRequest::class),
                TsoResponse::class,
                5000,
            )
            ->willReturn($response);

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );

        $this->assertSame((1715000000000 << 18) | 3, $oracle->getTimestamp(5000));
    }

    public function testGetTimestampWithRealPdClientInstance(): void
    {
        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(1);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturn($response);

        $pdClient = new PdClient($grpc, '127.0.0.1:2379');

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            $pdClient->getClusterId(...),
            $pdClient->setClusterId(...),
            new NullLogger(),
        );
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 1;
        $this->assertSame($expected, $result);
    }

    public function testGetTimestampRetriesOnClusterIdMismatch(): void
    {
        $ts = new Timestamp();
        $ts->setPhysical(1715000000000);
        $ts->setLogical(2);

        $response = new TsoResponse();
        $response->setTimestamp($ts);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('mismatch cluster id, need 42 but got 0', 14)),
                $response,
            );

        $pdClient = new PdClient($grpc, '127.0.0.1:2379');

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            $pdClient->getClusterId(...),
            $pdClient->setClusterId(...),
            new NullLogger(),
        );
        $result = $oracle->getTimestamp();

        $expected = (1715000000000 << 18) | 2;
        $this->assertSame($expected, $result);
        $this->assertSame(42, $pdClient->getClusterId());
    }

    public function testGetTimestampThrowsWhenClusterIdRetryAlsoFails(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new GrpcException('mismatch cluster id, need 42 but got 0', 14)),
                $this->throwException(new GrpcException('still unavailable', 14)),
            );

        $pdClient = new PdClient($grpc, '127.0.0.1:2379');

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            $pdClient->getClusterId(...),
            $pdClient->setClusterId(...),
            new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO request failed');
        $oracle->getTimestamp();
    }

    public function testGetTimestampThrowsOnClusterIdMismatchWhenPdClientIsInterfaceMock(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        $grpc->method('call')
            ->willThrowException(new GrpcException('mismatch cluster id, need 42 but got 0', 14));

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
        );

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('TSO request failed');
        $oracle->getTimestamp();
    }

    // ==================================================================
    // Batch TSO (issue #420, GAP-06)
    // ==================================================================

    private function makeTsoResponse(int $physical, int $logical, int $count): TsoResponse
    {
        $ts = new Timestamp();
        $ts->setPhysical($physical);
        $ts->setLogical($logical);

        $response = new TsoResponse();
        $response->setTimestamp($ts);
        $response->setCount($count);

        return $response;
    }

    private function makePooledResponse(int $firstTs, int $count): TsoResponse
    {
        // PD's response timestamp is the highest (last) of the granted
        // range; the oracle derives `first … last` from it.
        $last = $firstTs + $count - 1;
        $ts = new Timestamp();
        $ts->setPhysical($last >> 18);
        $ts->setLogical($last & ((1 << 18) - 1));

        $response = new TsoResponse();
        $response->setTimestamp($ts);
        $response->setCount($count);

        return $response;
    }

    private function makeOracle(
        GrpcClientInterface&MockObject $grpc,
        ?int $stalenessMs = null,
        ?\Closure $clock = null,
        ?int $poolSize = null,
        ?int $poolMaxAgeMs = null,
        ?\Closure $pid = null,
    ): TimestampOracle {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturn(1);

        return new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn() => $pdClient->getClusterId(),
            fn(int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
            $stalenessMs,
            $clock,
            $poolSize,
            $poolMaxAgeMs,
            $pid,
        );
    }

    public function testGetTimestampBatchSendsCountOnWireAndHandsOutConsecutiveTimestamps(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);

        // Logical counter near the 18-bit wrap: the handout must cross
        // into the next physical millisecond while staying monotonic.
        $grpc->expects($this->once())
            ->method('call')
            ->with(
                '127.0.0.1:2379',
                'pdpb.PD',
                'Tso',
                $this->callback(fn (TsoRequest $request): bool => $request->getCount() === 64),
                TsoResponse::class,
                null,
            )
            ->willReturn($this->makeTsoResponse(1715000000000, (1 << 18) - 2, 64));

        $oracle = $this->makeOracle($grpc);
        $range = $oracle->getTimestampBatch(64);

        // PD's response timestamp is the highest of the grant: the handout
        // ends at the response timestamp.
        $this->assertCount(64, $range);
        $this->assertSame(((1715000000000 << 18) + ((1 << 18) - 2)), $range[63]);
        // Monotonic and consecutive; walking back crosses into the
        // previous physical millisecond (plain integer arithmetic).
        for ($i = 1; $i < 64; $i++) {
            $this->assertSame($range[$i - 1] + 1, $range[$i], "element $i");
        }
        $this->assertSame((((1715000000000 << 18) + ((1 << 18) - 2)) - 63), $range[0]);
        $this->assertSame((((1715000000001 << 18)) - 65), $range[0]);
    }

    public function testGetTimestampBatchRejectsCountBelowOne(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');

        $oracle = $this->makeOracle($grpc);

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Timestamp batch count must be >= 1');
        $oracle->getTimestampBatch(0);
    }

    public function testGetTimestampBatchNeverExceedsGrantedCount(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        // PD grants fewer timestamps than requested (count=2 for count=64).
        $grpc->method('call')->willReturn($this->makeTsoResponse(1715000000000, 7, 2));

        $oracle = $this->makeOracle($grpc);
        $range = $oracle->getTimestampBatch(64);

        $this->assertCount(2, $range);
        $this->assertSame(((1715000000000 << 18) + 6), $range[0]);
        $this->assertSame(((1715000000000 << 18) + 7), $range[1]);
    }

    public function testGetTimestampRequestsPooledBatchAndHandsOutConsecutiveTimestamps(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->with(
                '127.0.0.1:2379',
                'pdpb.PD',
                'Tso',
                $this->callback(fn (TsoRequest $request): bool => $request->getCount() === 4),
                TsoResponse::class,
                null,
            )
            ->willReturn($this->makePooledResponse(1000, 4));

        $oracle = $this->makeOracle($grpc, poolSize: 4);

        $this->assertSame(1000, $oracle->getTimestamp());
        $this->assertSame(1001, $oracle->getTimestamp());
        $this->assertSame(1002, $oracle->getTimestamp());
        $this->assertSame(1003, $oracle->getTimestamp());
    }

    public function testGetTimestampPoolSizeOneDisablesPooling(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(3))
            ->method('call')
            ->with(
                '127.0.0.1:2379',
                'pdpb.PD',
                'Tso',
                $this->callback(fn (TsoRequest $request): bool => $request->getCount() === 1),
                TsoResponse::class,
                null,
            )
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 1),
                $this->makePooledResponse(1001, 1),
                $this->makePooledResponse(1002, 1),
            );

        $oracle = $this->makeOracle($grpc, poolSize: 1);

        $this->assertSame(1000, $oracle->getTimestamp());
        $this->assertSame(1001, $oracle->getTimestamp());
        $this->assertSame(1002, $oracle->getTimestamp());
    }

    public function testGetTimestampRejectsPoolSizeBelowOne(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Timestamp pool size must be >= 1');
        $this->makeOracle($grpc, poolSize: 0);
    }

    public function testGetTimestampRejectsPoolSizeAboveMaximum(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Timestamp pool size must be <= 1000');
        $this->makeOracle($grpc, poolSize: TimestampOracle::MAX_TIMESTAMP_POOL_SIZE + 1);
    }

    public function testGetTimestampRejectsNegativePoolMaxAge(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->never())->method('call');

        $this->expectException(\CrazyGoat\TiKV\Client\Exception\InvalidArgumentException::class);
        $this->expectExceptionMessage('Timestamp pool max age must be >= 0');
        $this->makeOracle($grpc, poolMaxAgeMs: -1);
    }

    public function testHundredSequentialGetTimestampsUseFewerThanTenGrpcCalls(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpcCalls = 0;
        $nextTs = 1000;
        $grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                TsoRequest $request,
            ) use (
                &$grpcCalls,
                &$nextTs,
            ): TsoResponse {
                $grpcCalls++;
                $count = $request->getCount();
                $response = $this->makePooledResponse($nextTs, $count);
                $nextTs += $count;

                return $response;
            },
        );

        $oracle = $this->makeOracle($grpc, clock: static fn (): int => 1_000_000);

        $previous = null;
        for ($i = 0; $i < 100; $i++) {
            $ts = $oracle->getTimestamp();
            if ($previous !== null) {
                $this->assertGreaterThan($previous, $ts, "timestamp $i is not strictly increasing");
            }
            $previous = $ts;
        }

        $this->assertLessThan(10, $grpcCalls, '100 pooled calls must fit in fewer than 10 TSO RPCs');
        // 100 calls with the default pool of 64 need exactly two grants.
        $this->assertSame(2, $grpcCalls);
    }

    public function testGetTimestampIsStrictlyMonotonicAcrossPoolRefills(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpcCalls = 0;
        $nextTs = 1000;
        $grpc->method('call')->willReturnCallback(
            function (
                string $address,
                string $service,
                string $method,
                TsoRequest $request,
            ) use (
                &$grpcCalls,
                &$nextTs,
            ): TsoResponse {
                $grpcCalls++;
                $count = $request->getCount();
                $response = $this->makePooledResponse($nextTs, $count);
                $nextTs += $count;

                return $response;
            },
        );

        // A tiny pool forces several refills over the run.
        $oracle = $this->makeOracle(
            $grpc,
            clock: static fn (): int => 1_000_000,
            poolSize: 4,
        );

        $previous = null;
        for ($i = 0; $i < 10; $i++) {
            $ts = $oracle->getTimestamp();
            if ($previous !== null) {
                $this->assertGreaterThan($previous, $ts, "timestamp $i is not strictly increasing");
            }
            $previous = $ts;
        }

        $this->assertSame(3, $grpcCalls, '10 calls with a 4-timestamp pool need three grants');
    }

    public function testGetTimestampDiscardsPoolOutsidePhysicalWindow(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 64),
                $this->makePooledResponse(2000, 64),
            );

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        $oracle = $this->makeOracle($grpc, clock: $clock, poolSize: 64, poolMaxAgeMs: 100);

        $this->assertSame(1000, $oracle->getTimestamp());
        $nowMs = 1_000_200; // age 200ms > the 100ms window: pool must be discarded

        $this->assertSame(2000, $oracle->getTimestamp());
    }

    public function testDefaultPoolMaxAgeAllowsReuseAtTheBound(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        // Exactly one RPC: the second call is still inside the default bound.
        $grpc->expects($this->once())
            ->method('call')
            ->willReturn($this->makePooledResponse(1000, 64));

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        // No explicit poolMaxAgeMs: pins the shipped default bound.
        $oracle = $this->makeOracle($grpc, clock: $clock);

        $this->assertSame(1000, $oracle->getTimestamp());
        $nowMs += TimestampOracle::DEFAULT_TIMESTAMP_POOL_MAX_AGE_MS;

        $this->assertSame(1001, $oracle->getTimestamp());
    }

    public function testDefaultPoolMaxAgeRefillsOncePastTheBound(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 64),
                $this->makePooledResponse(2000, 64),
            );

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        // No explicit poolMaxAgeMs: pins the shipped default bound.
        $oracle = $this->makeOracle($grpc, clock: $clock);

        $this->assertSame(1000, $oracle->getTimestamp());
        $nowMs += TimestampOracle::DEFAULT_TIMESTAMP_POOL_MAX_AGE_MS + 1;

        // The stale pool must not be served; a fresh grant is required.
        $this->assertSame(2000, $oracle->getTimestamp());
    }

    public function testGetTimestampDiscardsPoolWhenClusterIdChanges(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 64),
                $this->makePooledResponse(2000, 64),
            );

        $clusterId = 1;
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getClusterId')->willReturnCallback(
            function () use (&$clusterId): int {
                return $clusterId;
            },
        );
        $pdClient->method('setClusterId');

        $oracle = new TimestampOracle(
            $grpc,
            '127.0.0.1:2379',
            fn (): ?int => $pdClient->getClusterId(),
            fn (int $id) => $pdClient->setClusterId($id),
            new NullLogger(),
            null,
            static fn (): int => 1_000_000,
        );

        $this->assertSame(1000, $oracle->getTimestamp());
        $clusterId = 2;

        $this->assertSame(2000, $oracle->getTimestamp());
    }

    public function testGetTimestampDiscardsPoolAfterProcessChange(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 64),
                $this->makePooledResponse(2000, 64),
            );

        $pid = 1000;
        $oracle = $this->makeOracle(
            $grpc,
            clock: static fn (): int => 1_000_000,
            pid: function () use (&$pid): int {
                return $pid;
            },
        );

        $this->assertSame(1000, $oracle->getTimestamp());
        $pid = 2000; // simulate the pool crossing a fork boundary

        $this->assertSame(2000, $oracle->getTimestamp());
    }

    public function testGetTimestampBatchDiscardsPool(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 64),
                $this->makePooledResponse(5000, 1),
                $this->makePooledResponse(6000, 64),
            );

        $oracle = $this->makeOracle($grpc, clock: static fn (): int => 1_000_000);

        // Fill the pool with 1000..1063; the batch advances PD to 5000 and
        // must drop the pool so getTimestamp() cannot return 1001 next.
        $this->assertSame(1000, $oracle->getTimestamp());
        $this->assertSame([5000], $oracle->getTimestampBatch(1));
        $this->assertSame(6000, $oracle->getTimestamp());
    }

    public function testLowResolutionTimestampDoesNotDisturbPool(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 64),
                $this->makeTsoResponse(1715000000000, 9, 1),
            );

        $oracle = $this->makeOracle(
            $grpc,
            stalenessMs: 100,
            clock: static fn (): int => 1_000_000,
        );

        // Fill the pooled range 1000..1063; 1000 is served by the first call.
        $this->assertSame(1000, $oracle->getTimestamp());
        // The low-resolution path fetches its own timestamp and must not
        // drop or perturb the pool.
        $this->assertSame(((1715000000000 << 18) + 9), $oracle->getLowResolutionTimestamp());
        // Still served from the pool: no third RPC.
        $this->assertSame(1001, $oracle->getTimestamp());
    }

    public function testFailedPoolRefillLeavesNothingPooledToServe(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(3))
            ->method('call')
            ->willReturnOnConsecutiveCalls(
                $this->makePooledResponse(1000, 2),
                $this->throwException(new GrpcException('tso unavailable', 14)),
                $this->makePooledResponse(2000, 1),
            );

        $oracle = $this->makeOracle(
            $grpc,
            clock: static fn (): int => 1_000_000,
            poolSize: 2,
        );

        $this->assertSame(1000, $oracle->getTimestamp());
        $this->assertSame(1001, $oracle->getTimestamp());

        try {
            // Pool exhausted -> refill; the TSO RPC fails closed.
            $oracle->getTimestamp();
            $this->fail('Expected TiKvException to be thrown');
        } catch (TiKvException) {
        }

        // Nothing from the failed refill may be served: the next call must
        // perform a fresh RPC rather than replay a pooled/stale value.
        $this->assertSame(2000, $oracle->getTimestamp());
    }

    // ==================================================================
    // Low-resolution timestamp cache (issue #420, GAP-06)
    // ==================================================================

    public function testLowResolutionTimestampReusesCacheWithinStalenessBound(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        // Exactly one TSO RPC: the second call inside the staleness
        // window must be served from the cache.
        $grpc->expects($this->once())
            ->method('call')
            ->willReturn($this->makeTsoResponse(1715000000000, 9, 1));

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        $oracle = $this->makeOracle($grpc, stalenessMs: 100, clock: $clock);

        $this->assertSame(((1715000000000 << 18) + 9), $oracle->getLowResolutionTimestamp());
        $nowMs = 1_000_100; // exactly at the bound: still fresh
        $this->assertSame(((1715000000000 << 18) + 9), $oracle->getLowResolutionTimestamp());
    }

    public function testLowResolutionTimestampRefetchesWhenStalenessBoundExceeded(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnOnConsecutiveCalls(
            $this->makeTsoResponse(1715000000000, 9, 1),
            $this->makeTsoResponse(1715000000001, 10, 1),
        );

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        $oracle = $this->makeOracle($grpc, stalenessMs: 100, clock: $clock);

        $this->assertSame(((1715000000000 << 18) + 9), $oracle->getLowResolutionTimestamp());
        $nowMs = 1_000_101; // one ms past the bound
        $this->assertSame(((1715000000001 << 18) + 10), $oracle->getLowResolutionTimestamp());
    }

    public function testLowResolutionTimestampRefetchesWhenClockMovesBackwards(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->method('call')->willReturnOnConsecutiveCalls(
            $this->makeTsoResponse(1715000000000, 9, 1),
            $this->makeTsoResponse(1715000000001, 10, 1),
        );

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        $oracle = $this->makeOracle($grpc, stalenessMs: 100, clock: $clock);

        $this->assertSame(((1715000000000 << 18) + 9), $oracle->getLowResolutionTimestamp());
        $nowMs = 999_999; // clock jump backwards: negative age must not count as fresh
        $this->assertSame(((1715000000001 << 18) + 10), $oracle->getLowResolutionTimestamp());
    }

    public function testLowResolutionTimestampWithoutBoundFetchesFreshEveryCall(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturn($this->makeTsoResponse(1715000000000, 9, 1));

        $oracle = $this->makeOracle($grpc);

        $oracle->getLowResolutionTimestamp();
        $oracle->getLowResolutionTimestamp();
    }

    public function testLowResolutionTimestampZeroStalenessNeverServesCache(): void
    {
        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->exactly(2))
            ->method('call')
            ->willReturn($this->makeTsoResponse(1715000000000, 9, 1));

        $nowMs = 1_000_000;
        $clock = function () use (&$nowMs): int {
            return $nowMs;
        };
        $oracle = $this->makeOracle($grpc, stalenessMs: 0, clock: $clock);

        $oracle->getLowResolutionTimestamp();
        $nowMs = 1_000_001; // any elapsed ms exceeds the 0 bound
        $oracle->getLowResolutionTimestamp();
    }
}
