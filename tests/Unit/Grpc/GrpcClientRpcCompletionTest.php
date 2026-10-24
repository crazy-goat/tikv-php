<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Observability\InMemoryMetrics;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

/**
 * Issue #274 (GRPC-15): every rpcStarted() of call() / callStreaming() is
 * matched by exactly one rpcCompleted(), wherever the call fails, and a
 * metrics backend that throws cannot replace the transport exception.
 *
 * Plaintext is refused (`allowInsecure: false`), so getChannel() throws
 * right after rpcStarted(), before any send.
 */
final class GrpcClientRpcCompletionTest extends TestCase
{
    use GrpcExtensionGate;

    private const ADDRESS = '127.0.0.1:1';

    private const OPERATION = 'tikvpb.Tikv/RawGet';

    protected function setUp(): void
    {
        $this->requireGrpcExtension();
    }

    public function testCallFailingInGetChannelCompletesOnceAsFailed(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(allowInsecure: false, metrics: $metrics);

        try {
            $grpc->call(self::ADDRESS, 'tikvpb.Tikv', 'RawGet', new RawGetRequest(), RawGetResponse::class);
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            $this->assertSame(1, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(1, $metrics->getRpcFailed(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
        } finally {
            $grpc->close();
        }
    }

    public function testStreamingFailingInGetChannelCompletesOnceAsFailed(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(allowInsecure: false, metrics: $metrics);

        try {
            $grpc->callStreaming(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                [new RawGetRequest()],
                RawGetResponse::class,
            );
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            $this->assertSame(1, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(1, $metrics->getRpcFailed(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
        } finally {
            $grpc->close();
        }
    }

    public function testStreamingTransportFailureCompletesOnceAsFailed(): void
    {
        $metrics = new InMemoryMetrics();
        $grpc = new GrpcClient(metrics: $metrics);

        // Nothing listens on the port: the streaming sends and the receive
        // fail with a transport error. A protobuf message cannot be mocked or
        // subclassed, so a failure that is raised from inside the send loop
        // itself cannot be injected; the send loop is inside the same
        // try/finally as the receive.
        try {
            $grpc->callStreaming(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                [new RawGetRequest(), new RawGetRequest()],
                RawGetResponse::class,
                timeoutMs: 500,
            );
            $this->fail('Expected GrpcException from the dead endpoint');
        } catch (GrpcException) {
            $this->assertSame(1, $metrics->getRpcStarted(self::OPERATION));
            $this->assertSame(1, $metrics->getRpcFailed(self::OPERATION));
            $this->assertSame(0, $metrics->getRpcSucceeded(self::OPERATION));
        } finally {
            $grpc->close();
        }
    }

    /**
     * The other half of the metric: a backend that throws from
     * `rpcStarted()` must not abort the call either (issue #759, GRPC-15).
     *
     * The `rpcStarted()` emission sits *before* the call, so a throw from it
     * would abort a call that never happened and replace the transport
     * exception the caller has to see.
     */
    public function testABackendThrowingFromRpcStartedDoesNotMaskTheTransportException(): void
    {
        $backendError = new \LogicException('metrics backend down');
        $metrics = $this->createMock(MetricsInterface::class);
        $metrics->expects($this->once())
            ->method('rpcStarted')
            ->willThrowException($backendError);
        $metrics->expects($this->once())
            ->method('rpcCompleted')
            ->with(self::OPERATION, $this->isType('float'), false);

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('warning')
            ->with(
                $this->anything(),
                $this->callback(
                    static fn(array $context): bool => ($context['operation'] ?? null) === self::OPERATION
                        && ($context['exception'] ?? null) === $backendError,
                ),
            );

        $grpc = new GrpcClient(logger: $logger, allowInsecure: false, metrics: $metrics);

        try {
            $grpc->call(self::ADDRESS, 'tikvpb.Tikv', 'RawGet', new RawGetRequest(), RawGetResponse::class);
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            // The original exception, not the LogicException from the backend.
            $this->addToAssertionCount(1);
        } finally {
            $grpc->close();
        }
    }

    /**
     * The same for the streaming path: a backend that throws from
     * `rpcStarted()` must not abort the call either (issue #759, GRPC-15).
     */
    public function testABackendThrowingFromRpcStartedDoesNotMaskTheStreamingException(): void
    {
        $backendError = new \LogicException('metrics backend down');
        $metrics = $this->createMock(MetricsInterface::class);
        $metrics->expects($this->once())
            ->method('rpcStarted')
            ->willThrowException($backendError);
        $metrics->expects($this->once())
            ->method('rpcCompleted')
            ->with(self::OPERATION, $this->isType('float'), false);

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('warning');

        $grpc = new GrpcClient(logger: $logger, allowInsecure: false, metrics: $metrics);

        try {
            $grpc->callStreaming(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                [new RawGetRequest()],
                RawGetResponse::class,
            );
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            // The original exception, not the LogicException from the backend.
            $this->addToAssertionCount(1);
        } finally {
            $grpc->close();
        }
    }

    /**
     * …and for the asynchronous path (issue #759, GRPC-15).
     */
    public function testABackendThrowingFromRpcStartedDoesNotMaskTheAsyncException(): void
    {
        $backendError = new \LogicException('metrics backend down');
        $metrics = $this->createMock(MetricsInterface::class);
        $metrics->expects($this->once())
            ->method('rpcStarted')
            ->willThrowException($backendError);
        // The send never happens, so the RPC completes as failed.
        $metrics->expects($this->once())
            ->method('rpcCompleted')
            ->with(self::OPERATION, $this->isType('float'), false);

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('warning');

        $grpc = new GrpcClient(logger: $logger, allowInsecure: false, metrics: $metrics);

        try {
            $grpc->callAsync(
                self::ADDRESS,
                'tikvpb.Tikv',
                'RawGet',
                new RawGetRequest(),
                RawGetResponse::class,
            );
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            // The original exception, not the LogicException from the backend.
            $this->addToAssertionCount(1);
        } finally {
            $grpc->close();
        }
    }

    /**
     * A backend that throws a `\TypeError` — not an `\Exception` — must not
     * break the call either: the guard catches `\Throwable` (issue #759).
     */
    public function testABackendThrowingATypeErrorDoesNotMaskTheTransportException(): void
    {
        $backendError = new \TypeError('metrics backend returned the wrong type');
        $metrics = $this->createMock(MetricsInterface::class);
        $metrics->expects($this->once())
            ->method('rpcStarted')
            ->willThrowException($backendError);
        $metrics->expects($this->once())
            ->method('rpcCompleted')
            ->with(self::OPERATION, $this->isType('float'), false);

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('warning');

        $grpc = new GrpcClient(logger: $logger, allowInsecure: false, metrics: $metrics);

        try {
            $grpc->call(self::ADDRESS, 'tikvpb.Tikv', 'RawGet', new RawGetRequest(), RawGetResponse::class);
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            // The original exception, not the TypeError from the backend.
            $this->addToAssertionCount(1);
        } finally {
            $grpc->close();
        }
    }

    public function testThrowingMetricsBackendDoesNotMaskTheTransportException(): void
    {
        $backendError = new \LogicException('metrics backend down');
        $metrics = $this->createMock(MetricsInterface::class);
        $metrics->expects($this->once())->method('rpcStarted');
        $metrics->expects($this->once())
            ->method('rpcCompleted')
            ->with(self::OPERATION, $this->isType('float'), false)
            ->willThrowException($backendError);

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('warning')
            ->with(
                $this->anything(),
                $this->callback(
                    static fn(array $context): bool => ($context['operation'] ?? null) === self::OPERATION
                        && ($context['exception'] ?? null) === $backendError,
                ),
            );

        $grpc = new GrpcClient(logger: $logger, allowInsecure: false, metrics: $metrics);

        try {
            $grpc->call(self::ADDRESS, 'tikvpb.Tikv', 'RawGet', new RawGetRequest(), RawGetResponse::class);
            $this->fail('Expected InvalidStateException from the refused plaintext channel');
        } catch (InvalidStateException) {
            // The original exception, not the LogicException from the backend.
            $this->addToAssertionCount(1);
        } finally {
            $grpc->close();
        }
    }
}
