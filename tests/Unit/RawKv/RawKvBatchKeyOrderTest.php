<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\RawKv\RawKvBatch;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

/**
 * `RawKvBatch::keyInRegion()` — the region-membership test the batch
 * dispatch/refresh path uses to decide whether a region's cached view still
 * covers the keys it was grouped under (issue #261 acceptance criteria).
 *
 * It is private, so it is invoked through reflection exactly like the
 * existing `RawKvBatchTest::testKeyInRegionUsesByteOrderNotNumericOrder()` —
 * but that class lives in the `Grpc` suite (it also mocks `\Grpc\Call`),
 * while this one is pure PHP so the table runs in the no-extension `Unit`
 * job, which is where the rest of the byte-order regressions live.
 *
 * Every row is a pair on which PHP's own relational operators give a
 * different answer than byte order, either because both operands are numeric
 * strings with a different numeric order ('20' vs '100') or because PHP
 * considers two distinct keys numerically equal ('007' vs '7', '1e3' vs
 * '1000') and therefore cannot order them at all.
 */
#[CoversClass(RawKvBatch::class)]
final class RawKvBatchKeyOrderTest extends TestCase
{
    private RawKvBatch $batch;

    protected function setUp(): void
    {
        $this->batch = new RawKvBatch(
            $this->createMock(GrpcClientInterface::class),
            new RegionResolver(
                $this->createMock(PdClientInterface::class),
                $this->createMock(RegionCacheInterface::class),
            ),
            new TimeoutConfig(),
            new NullLogger(),
        );
    }

    private function region(string $startKey, string $endKey): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: $startKey,
            endKey: $endKey,
        );
    }

    /**
     * [key, region start, region end, expected membership in byte order].
     *
     * @return iterable<string, array{string, string, string, bool}>
     */
    public static function numericKeyBoundaryPairs(): iterable
    {
        // Leading zeros: PHP reads "007" and "7" as the same number, so
        // neither is less nor greater than the other — yet they are two
        // distinct TiKV keys ('0' = 0x30 < '7' = 0x37), and a region boundary
        // between them routes real traffic.
        yield '007 before 7' => ['007', '7', '', false];
        yield '7 after 007' => ['7', '007', '', true];
        yield '007 is its own start key' => ['007', '007', '1', true];

        // Exponent notation: "1e3" == "1000" numerically, so PHP cannot order
        // them at all; bytewise '1e3' > '1000' ('e' = 0x65 > '0' = 0x30).
        yield '1e3 after 1000' => ['1e3', '1000', '', true];
        yield '1000 before 1e3' => ['1000', '1e3', '', false];
        yield '1e3 is the exclusive end' => ['1e3', '1000', '1e3', false];
        yield '1000 inside the 1e3 region' => ['1000', '1e3', '2e3', false];

        // Multi-digit decimals whose numeric order is the reverse of their
        // byte order ('2' = 0x32 > '1' = 0x31).
        yield '20 is not below 100' => ['20', '', '100', false];
        yield '20 is above 100' => ['20', '100', '', true];
        yield '0999 is below 100' => ['0999', '', '100', true];
        yield '0100 is below 99' => ['0100', '', '99', true];
        yield '99 is above 0100' => ['99', '0100', '', true];
        yield '99 is not below 0100' => ['99', '', '0100', false];

        // Half-open semantics on boundaries that PHP folds onto themselves.
        yield 'start key is inclusive' => ['100', '100', '200', true];
        yield 'end key is exclusive' => ['200', '100', '200', false];
        // '-6' > '-5' bytewise ('6' = 0x36 > '5' = 0x35) even though -6 < -5
        // as a number, so a signed boundary is the one region bound PHP reads
        // backwards in both directions.
        yield 'negative decimal start is inclusive' => ['-5', '-5', '0', true];
        yield 'negative decimal sorts after its start key' => ['-6', '-5', '0', true];

        // An empty end key is +infinity, and a key equal to the start is in.
        yield 'unbounded region contains its start' => ['0', '0', '', true];
        yield 'unbounded region excludes a smaller key' => ['0999', '1', '', false];
    }

    #[DataProvider('numericKeyBoundaryPairs')]
    public function testKeyInRegionAgreesWithAStrcmpReference(
        string $key,
        string $startKey,
        string $endKey,
        bool $expected,
    ): void {
        $method = new \ReflectionMethod(RawKvBatch::class, 'keyInRegion');
        $actual = $method->invoke($this->batch, $key, $this->region($startKey, $endKey));

        self::assertSame(
            $expected,
            $actual,
            sprintf(
                'byte order puts "%s" %s [%s, %s)',
                $key,
                $expected ? 'inside' : 'outside',
                $startKey,
                $endKey,
            ),
        );

        // The independent reference: half-open [start, end) straight on
        // strcmp(), with no shared code with KeyOrder.
        $reference = strcmp($key, $startKey) >= 0 && ($endKey === '' || strcmp($key, $endKey) < 0);
        self::assertSame($reference, $actual);
    }

    /**
     * The table above is only worth running while at least some of its rows
     * are ones PHP mis-orders: a mutation that turns keyInRegion() back into
     * `$key >= $start && ($end === '' || $key < $end)` would otherwise pass
     * every non-discriminating row.
     *
     * The bound is a floor, not the exact count. Measured today: 12 of the 19
     * rows discriminate and 7 do not, so `> 10` fails the moment two
     * discriminating rows are lost or rewritten into a shape PHP happens to
     * agree with — the previous `> 5` would have kept passing with 7 of the 12
     * gone. Raise the number when the table grows; the comment records the
     * value the bound was calibrated against.
     */
    public function testTheTableContainsRowsPhpMisorders(): void
    {
        $discriminating = 0;
        $total = 0;
        foreach (self::numericKeyBoundaryPairs() as [$key, $startKey, $endKey, $expected]) {
            ++$total;
            $phpVerdict = $this->phpVerdict($key, '>=', $startKey)
                && ($endKey === '' || $this->phpVerdict($key, '<', $endKey));
            if ($phpVerdict !== $expected) {
                ++$discriminating;
            }
        }

        self::assertGreaterThan(
            10,
            $discriminating,
            sprintf(
                'the table must keep rows on which PHP\'s numeric order differs from byte order (currently %d of %d)',
                $discriminating,
                $total,
            ),
        );
    }

    /**
     * PHP's own verdict for two strings — the pre-#186 comparison, kept here
     * only to prove the table discriminates. The parameters are deliberately
     * not named like keys: the #186 PHPStan rule (issue #186) reports
     * relational operators between key-like names, and this is a test of what
     * that operator does, not library code.
     */
    private function phpVerdict(string $left, string $operator, string $right): bool
    {
        return match ($operator) {
            '<' => $left < $right,
            '<=' => $left <= $right,
            '>' => $left > $right,
            default => $left >= $right,
        };
    }
}
