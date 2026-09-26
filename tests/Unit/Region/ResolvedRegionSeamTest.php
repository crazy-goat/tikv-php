<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Region;

use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RegionGrouper;
use PHPUnit\Framework\TestCase;

/**
 * Issue #187: the guard every region-grouping loop reads its resolved map
 * through.
 *
 * `RegionResolver` is `final` and its fail-closed contract (issue #244) means
 * a partial map is not something production code can hand to a grouping loop:
 * `batchResolveRegions()` throws before it can return one. That is exactly why
 * the loop-level guard is unreachable today — and also why it cannot be
 * covered from the outside. The one way to exercise it is to hand the seam a
 * partial map directly, which is what this class does, so the guard is a
 * tested contract rather than an unreachable branch: it is the thing that
 * turns "a future refactor weakened the resolver" from silent data loss into
 * a `TiKvException`.
 *
 * The behaviour it pins, and that a skip would break:
 *  - an unresolvable key throws a typed, redacted failure naming the key —
 *    it is never skipped, because `batchPut()`/`ingest()` return `void` and
 *    `batchGet()` cannot tell a skip from a missing value;
 *  - the failure is a `TiKvException`, the type the resolver and both
 *    `RegionGrouper` groupers already use for it, so a caller has one thing
 *    to catch;
 *  - the message carries `KeyRedactor::redact($key)`, never the raw key.
 */
class ResolvedRegionSeamTest extends TestCase
{
    private function region(int $id, string $startKey = '', string $endKey = ''): RegionInfo
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

    public function testReturnsTheRegionAssignedToTheKey(): void
    {
        $first = $this->region(1, 'a', 'm');
        $second = $this->region(2, 'm');

        $resolved = ['a' => $first, 'm' => $second];

        self::assertSame($first, RegionGrouper::resolvedRegion($resolved, 'a'));
        self::assertSame($second, RegionGrouper::resolvedRegion($resolved, 'm'));
    }

    public function testThrowsNamingTheKeyWhenTheMapLacksIt(): void
    {
        $resolved = ['a' => $this->region(1, 'a', 'm')];

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage(
            'Region could not be resolved for key "7a" (1 bytes); refusing to silently drop it from the batch',
        );

        RegionGrouper::resolvedRegion($resolved, 'z');
    }

    /**
     * The same message wording the two merged fail-closed sites already use,
     * asserted here so the whole client keeps saying one thing about this
     * failure: `RegionResolver::batchResolveRegions()` (PD's side) and
     * `RegionGrouper` (the map's side) differ only in which component could
     * not answer, and every grouping loop now routes through this one copy.
     */
    public function testMessageIsTheOneSharedByEveryGroupingLoop(): void
    {
        $this->expectException(TiKvException::class);
        $this->expectExceptionMessageMatches('/refusing to silently drop it from the batch$/');

        RegionGrouper::resolvedRegion([], 'user:secret-tenant-id');
    }

    /**
     * The message must identify *which* key could not be routed while never
     * putting raw key bytes in it (issue #269). 12 bytes is past the
     * redactor's 8-byte hex prefix, so this also pins the truncated form.
     */
    public function testMessageRedactsTheKeyItNames(): void
    {
        try {
            RegionGrouper::resolvedRegion([], 'user:secret-tenant-id');
            self::fail('an unresolvable key must fail the call');
        } catch (TiKvException $e) {
            // Asserted as the redactor's 8-byte hex prefix plus the total
            // length rather than as one literal: `KeyRedactor`'s own
            // long-key form has an unbalanced quote (KeyRedactor.php:70, see
            // also KeyRedactorTest and docs/troubleshooting.md), and this
            // test is about the redaction, not about that formatting.
            self::assertStringContainsString('757365723a736563', $e->getMessage());
            self::assertStringContainsString('(21 bytes)', $e->getMessage());
            self::assertStringNotContainsString('secret-tenant-id', $e->getMessage());
            self::assertStringNotContainsString('user:', $e->getMessage());
        }
    }

    /**
     * A canonical decimal key is stored under its `int` form (issue #261), so
     * the lookup must accept the `int` the loop iterates with — `$keyValuePairs`
     * hands `batchPut()` an `int` key for `['1000' => …]`, and a seam that
     * only accepted `string` would fail closed on a key that *was* resolved.
     */
    public function testAcceptsTheIntArrayKeyFormADecimalKeyIsStoredUnder(): void
    {
        $region = $this->region(7);

        self::assertSame($region, RegionGrouper::resolvedRegion([1000 => $region], 1000));
        // A string lookup finds the same entry: PHP casts it the same way.
        self::assertSame($region, RegionGrouper::resolvedRegion([1000 => $region], '1000'));
    }
}
