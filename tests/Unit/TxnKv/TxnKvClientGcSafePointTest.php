<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Connection\SafePointCache;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnAbortedByGcException;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * GC safe-point validation at begin() (issue #422, AC-2).
 *
 * The validation lives in TxnKvClient::validateStartTsAgainstGcSafePoint()
 * behind an optional SafePointCache; these tests construct the client
 * directly (constructor path, no ConnectionFactory) and drive the cache
 * with a fake fetch closure, so no PD round trip is involved.
 *
 * create()-level option validation is covered by the *Rejects* tests: the
 * option resolution throws before any connection is attempted, so the real
 * ConnectionFactory wiring is safe to exercise in unit tests. Whether
 * validation is enabled by default is asserted structurally — create()
 * passes a cache unless options['gcSafePointValidation'] === false — and
 * behaviourally by the begin() tests through the constructor path.
 */
final class TxnKvClientGcSafePointTest extends TestCase
{
    public function testBeginThrowsWhenStartTsBelowSafePoint(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(900);

        $client = new TxnKvClient(
            $pdClient,
            $this->createMock(GrpcClientInterface::class),
            safePointCache: $this->cacheAt(1000),
        );

        try {
            $client->begin();
            self::fail('Expected TxnAbortedByGcException');
        } catch (TxnAbortedByGcException $e) {
            $this->assertStringContainsString('900', $e->getMessage());
            $this->assertStringContainsString('1000', $e->getMessage());
            $this->assertStringContainsString('holdGcSafePoint', $e->getMessage());
        }
    }

    public function testBeginSucceedsWhenStartTsAboveSafePoint(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1500);

        $client = new TxnKvClient(
            $pdClient,
            $this->createMock(GrpcClientInterface::class),
            safePointCache: $this->cacheAt(1000),
        );

        $txn = $client->begin();

        $this->assertInstanceOf(Transaction::class, $txn);
        $this->assertSame(1500, $txn->getStartTs());
    }

    public function testBeginSucceedsWhenStartTsEqualsSafePoint(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1000);

        $client = new TxnKvClient(
            $pdClient,
            $this->createMock(GrpcClientInterface::class),
            safePointCache: $this->cacheAt(1000),
        );

        $txn = $client->begin();

        $this->assertInstanceOf(Transaction::class, $txn);
        $this->assertSame(1000, $txn->getStartTs());
    }

    public function testBeginFailsOpenWhenSafePointFetchFails(): void
    {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(1); // far below any real safe point

        $client = new TxnKvClient(
            $pdClient,
            $this->createMock(GrpcClientInterface::class),
            safePointCache: new SafePointCache(
                static function (): never {
                    throw new TiKvException('PD unreachable');
                },
                30000,
            ),
        );

        // Fail-open: begin() must succeed despite the failed safe-point
        // fetch. A PD outage must not invent a new hard failure mode for
        // clients whose PD serves TSO fine but not the GC RPC.
        $txn = $client->begin();

        $this->assertInstanceOf(Transaction::class, $txn);
        $this->assertSame(1, $txn->getStartTs());
    }

    public function testCreateAcceptsExplicitValidationEnabled(): void
    {
        // Option resolution runs before any network activity; the cache
        // itself is lazy, so construction with validation enabled is safe.
        $client = TxnKvClient::create(
            ['pd:2379'],
            options: ['gcSafePointValidation' => true],
        );

        $this->assertInstanceOf(TxnKvClient::class, $client);
    }

    public function testConstructorRejectsNegativeRefreshInterval(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("options['gcSafePointRefreshMs'] must be >= 1");
        TxnKvClient::create(
            ['pd:2379'],
            options: ['gcSafePointRefreshMs' => 0],
        );
    }

    public function testConstructorRejectsNonIntRefreshInterval(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("options['gcSafePointRefreshMs'] must be an int (milliseconds)");
        TxnKvClient::create(
            ['pd:2379'],
            options: ['gcSafePointRefreshMs' => '30000'],
        );
    }

    public function testConstructorRejectsNonBoolValidationFlag(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("options['gcSafePointValidation'] must be a bool");
        TxnKvClient::create(
            ['pd:2379'],
            options: ['gcSafePointValidation' => 'yes'],
        );
    }

    /**
     * A SafePointCache whose fetch always returns the given safe point.
     */
    private function cacheAt(int $safePoint): SafePointCache
    {
        return new SafePointCache(
            static fn (): int => $safePoint,
            30000,
        );
    }
}
