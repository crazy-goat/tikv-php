<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\RawKv\SstIngestor;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Retry\RetryExecutor;
use CrazyGoat\TiKV\Client\TxnKv\LockResolver;
use CrazyGoat\TiKV\Client\TxnKv\Transaction;
use CrazyGoat\TiKV\Client\TxnKv\TransactionStatus;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * Issue #187 (RAW-02) and issue #181 (RC-2, the root cause four auditors
 * filed independently): a key whose region cannot be resolved is an internal
 * error, never a valid outcome. Every region-grouping loop fails closed
 * instead of `continue`-ing past the key, because all of these entry
 * points have no way to report a partial result:
 *
 *  - `batchPut()`, `batchDelete()` and `ingest()` return `void`, so a
 *    skipped key was a write the caller was told had succeeded and that was
 *    never sent to the cluster;
 *  - `batchGet()` returns a map, and a skipped key became a `null` value
 *    indistinguishable from a legitimately missing key;
 *  - `Transaction::commit()` returns `void` and then reports
 *    {@see TransactionStatus::Committed}, so a skipped key was a
 *    `set(K); commit()` pair that was a **total** no-op and said so.
 *
 * ## How the resolver is faked
 *
 * The acceptance criteria say "a mocked resolver that returns no region for
 * one key". `RegionResolver` is `final`, so PHPUnit cannot mock it (and a
 * subclass seam is deliberately not added for a test). The equivalent is
 * mocked at the layer below it: a mocked `PdClientInterface` whose
 * `scanRegions()` answers with a window that does not cover the key, which is
 * exactly the condition the issue describes for a real cluster ("$resolved[$key]
 * is missing"). The fail-closed `batchResolveRegions()` (issue #244) then
 * refuses the whole batch, so the entry points below must propagate it.
 *
 * Because the failure happens before a single gRPC call is constructed, none
 * of these tests touches `\Grpc\Call` and the whole class can live in the
 * extension-free `Unit` suite (the CI `unit-tests` job runs `php -n`). The
 * grouping loop's own throw is covered by
 * {@see \CrazyGoat\TiKV\Tests\Unit\Region\ResolvedRegionSeamTest}, which
 * reaches it with a hand-built partial map.
 */
class UnresolvedRegionFailsClosedTest extends TestCase
{
    private GrpcClientInterface&MockObject $grpc;
    private RegionCacheInterface&MockObject $regionCache;
    private PdClientInterface&MockObject $pdClient;

    protected function setUp(): void
    {
        $this->grpc = $this->createMock(GrpcClientInterface::class);
        $this->regionCache = $this->createMock(RegionCacheInterface::class);
        $this->pdClient = $this->createMock(PdClientInterface::class);
    }

    /**
     * A window covering `'a'` … `'m'` only: `'z'` is outside every returned
     * region, so it cannot be assigned to one.
     */
    private function givenAPdWindowThatMissesKeyZ(): void
    {
        $this->pdClient->method('scanRegions')->willReturn([
            $this->region(1, 'a', 'm'),
        ]);
        $this->regionCache->method('put');
    }

    /**
     * `'z'` is always the unresolvable key, and it is always identified in
     * the message in its redacted form — never as raw bytes (issue #269).
     */
    private function expectRejectionOfUnresolvableKeyZ(): void
    {
        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('"' . bin2hex('z') . '" (1 bytes); refusing to silently drop');
    }

    /**
     * Not one byte may reach the cluster when a key of the batch cannot be
     * routed — a partially-sent batch is exactly the reported failure.
     * Verified at teardown, so it holds for the `void` entry points too.
     */
    private function expectNoRequestOnTheWire(): void
    {
        $this->grpc->expects(self::never())->method('getChannel');
        $this->grpc->expects(self::never())->method('call');
        $this->grpc->expects(self::never())->method('callAsync');
        $this->grpc->expects(self::never())->method('callStreaming');
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

    private function retryExecutor(): RetryExecutor
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

    private function rawKvBatch(): RawKvBatch
    {
        return new RawKvBatch(
            $this->grpc,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    private function sstIngestor(): SstIngestor
    {
        return new SstIngestor(
            $this->grpc,
            $this->pdClient,
            new RegionResolver($this->pdClient, $this->regionCache),
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    private function transaction(bool $pessimistic): Transaction
    {
        $regionResolver = new RegionResolver($this->pdClient, $this->regionCache);

        return new Transaction(
            txnId: 'unresolvable-181',
            startTs: 1000,
            pessimistic: $pessimistic,
            priority: 0,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: new LockResolver($this->grpc, $regionResolver, $this->regionCache, $this->pdClient, 1000),
            regionResolver: $regionResolver,
            maxBackoffMs: 100,
        );
    }

    /**
     * The issue's own wording: "for a transaction, `set(K); commit()` can be
     * a total no-op reported as `Committed`".
     *
     * Three properties, because each is a way the claim can come back:
     *
     *  - the failure **names the key**, in its redacted form. This is the
     *    assertion that bites. `TwoPhaseCommitter::commit()` also carries a
     *    count guard from #208 that throws `InvalidStateException('Not all
     *    transaction mutations were assigned to a region; refusing to report
     *    commit success')` when the grouped mutations outnumber the write
     *    set — so a silently-skipped key still fails, but a caller learns
     *    only that *something* was unassignable and not which key, and the
     *    guard lives one layer below the region resolution that named it.
     *  - the status is **not** `Committed`, so the transaction never claims
     *    the write happened;
     *  - **nothing reached the wire** — a total no-op that at least cost a
     *    prewrite would be a different bug.
     */
    public function testSetThenCommitWithAnUnresolvableKeyNeverReportsCommitted(): void
    {
        $this->givenAPdWindowThatMissesKeyZ();
        $this->expectNoRequestOnTheWire();

        $txn = $this->transaction(pessimistic: false);
        $txn->set('z', 'v1');

        $threw = null;
        try {
            $txn->commit();
        } catch (TiKvException $e) {
            $threw = $e;
        }

        self::assertInstanceOf(
            TiKvException::class,
            $threw,
            'commit() must not return normally with an unroutable key in the write set',
        );
        self::assertStringContainsString(
            '"' . bin2hex('z') . '" (1 bytes); refusing to silently drop',
            $threw->getMessage(),
            'the failure must name the key that could not be routed, not just report a count mismatch',
        );
        self::assertNotSame(
            TransactionStatus::Committed,
            $txn->getStatus(),
            'a transaction that wrote nothing must not report Committed',
        );
    }

    /**
     * The pessimistic path fails *earlier* — at the write, not at the commit —
     * because the physical lock is acquired inside `set()` by default
     * (#437). Worth its own case: the guarantee is then "an unroutable key
     * cannot even be staged", which is a stronger claim than the commit one
     * and would be lost silently if `set()` were ever made to buffer again.
     */
    public function testPessimisticSetWithAnUnresolvableKeyFailsAtTheWrite(): void
    {
        $this->givenAPdWindowThatMissesKeyZ();
        $this->expectNoRequestOnTheWire();

        $txn = $this->transaction(pessimistic: true);

        $threw = null;
        try {
            $txn->set('z', 'v1');
        } catch (TiKvException $e) {
            $threw = $e;
        }

        self::assertInstanceOf(
            TiKvException::class,
            $threw,
            'set() must not return normally for a key that cannot be locked',
        );
        self::assertStringContainsString(
            '"' . bin2hex('z') . '" (1 bytes); refusing to silently drop',
            $threw->getMessage(),
        );
        self::assertNotSame(TransactionStatus::Committed, $txn->getStatus());
    }

    /**
     * `batchPut()` returns `void`: an unresolvable key has no way to be
     * reported, so the call must throw instead of returning normally having
     * written only `'a'`.
     */
    public function testBatchPutThrowsInsteadOfReturningVoidWithAnUnresolvableKey(): void
    {
        $this->givenAPdWindowThatMissesKeyZ();
        $this->expectNoRequestOnTheWire();
        $this->expectRejectionOfUnresolvableKeyZ();

        $this->rawKvBatch()->batchPut(['a' => '1', 'z' => '2'], 60, $this->retryExecutor());
    }

    public function testBatchDeleteThrowsInsteadOfSilentlySkippingTheKey(): void
    {
        $this->givenAPdWindowThatMissesKeyZ();
        $this->expectNoRequestOnTheWire();
        $this->expectRejectionOfUnresolvableKeyZ();

        $this->rawKvBatch()->batchDelete(['a', 'z'], $this->retryExecutor());
    }

    /**
     * The read variant of the same hole. Before the fix `'z'` came back as
     * `['a' => null, 'z' => null]`, which a caller cannot tell apart from
     * "the key does not exist" — so a read-after-write check reported "not
     * found" instead of an error.
     */
    public function testBatchGetThrowsInsteadOfSubstitutingNullForTheKey(): void
    {
        $this->givenAPdWindowThatMissesKeyZ();
        $this->expectNoRequestOnTheWire();
        $this->expectRejectionOfUnresolvableKeyZ();

        $this->rawKvBatch()->batchGet(['a', 'z'], $this->retryExecutor());
    }

    /**
     * `ingest()` is the site that still had the silent `continue` on its own
     * terms: with no stores reported by PD it never issues an RPC at all, so
     * the grouping loop is the only thing that can notice the unresolvable
     * pair. Before the fix `ingest()` returned normally and `'z'` was simply
     * never written — a "successful" bulk import that lost data, with the
     * stores already switched to import mode and back again.
     */
    public function testIngestThrowsInsteadOfSilentlySkippingTheUnresolvablePair(): void
    {
        $this->givenAPdWindowThatMissesKeyZ();
        $this->pdClient->method('getAllStores')->willReturn([]);
        $this->expectNoRequestOnTheWire();
        $this->expectRejectionOfUnresolvableKeyZ();

        $this->sstIngestor()->ingest(['a' => '1', 'z' => '2']);
    }
}
