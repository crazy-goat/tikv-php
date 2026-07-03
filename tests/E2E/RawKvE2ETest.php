<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\RawKv\CasResult;
use CrazyGoat\TiKV\Client\RawKv\ChecksumResult;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end tests requiring running TiKV cluster.
 *
 * Run with: docker-compose --profile test up --build php-test
 *
 * Reverse scan semantics (per kvrpcpb.proto and Go client-go):
 *   reverseScan(startKey, endKey) scans [endKey, startKey) in descending order
 *   - startKey = upper bound (exclusive)
 *   - endKey   = lower bound (inclusive)
 */
class RawKvE2ETest extends TestCase
{
    private static ?RawKvClient $client = null;

    private RawKvClient $testClient;

    /** @var string[] Keys created during the current test, cleaned up in tearDown */
    private array $keysToCleanup = [];

    public static function setUpBeforeClass(): void
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        self::$client = RawKvClient::create($pdEndpoints, options: ['insecure' => true]);
    }

    public static function tearDownAfterClass(): void
    {
        if (self::$client instanceof \CrazyGoat\TiKV\Client\RawKv\RawKvClient) {
            self::$client->close();
            self::$client = null;
        }
    }

    protected function setUp(): void
    {
        if (!self::$client instanceof \CrazyGoat\TiKV\Client\RawKv\RawKvClient) {
            $this->markTestSkipped('TiKV cluster not available');
        }
        $this->testClient = self::$client;
        $this->testClient->setAtomicForCAS(true);
        $this->keysToCleanup = [];
    }

    protected function tearDown(): void
    {
        foreach ($this->keysToCleanup as $key) {
            try {
                $this->testClient->delete($key);
            } catch (\Exception) {
                // Ignore errors during cleanup
            }
        }
    }

    /**
     * Helper: put key-value pairs and register them for cleanup.
     *
     * @param array<string, string> $pairs
     */
    private function putAndTrack(array $pairs): void
    {
        $this->testClient->batchPut($pairs);
        foreach (array_keys($pairs) as $key) {
            $this->keysToCleanup[] = $key;
        }
    }

    /**
     * Helper: put a single key-value pair and register for cleanup.
     */
    private function putOneAndTrack(string $key, string $value): void
    {
        $this->testClient->put($key, $value);
        $this->keysToCleanup[] = $key;
    }

    // ========================================================================
    // Basic CRUD
    // ========================================================================

    public function testPutAndGet(): void
    {
        $this->putOneAndTrack('test-key', 'test-value');

        $this->assertEquals('test-value', $this->testClient->get('test-key'));
    }

    public function testGetNonExistentKey(): void
    {
        $this->assertNull($this->testClient->get('non-existent-key-' . uniqid()));
    }

    public function testPutOverwrite(): void
    {
        $this->putOneAndTrack('test-key', 'value-1');
        $this->putOneAndTrack('test-key', 'value-2');

        $this->assertEquals('value-2', $this->testClient->get('test-key'));
    }

    public function testDelete(): void
    {
        $this->putOneAndTrack('test-key', 'test-value');
        $this->assertEquals('test-value', $this->testClient->get('test-key'));

        $this->testClient->delete('test-key');
        $this->assertNull($this->testClient->get('test-key'));
    }

    public function testDeleteNonExistentKey(): void
    {
        // Should not throw
        $this->testClient->delete('non-existent-key-' . uniqid());
        $this->addToAssertionCount(1);
    }

    public function testMultipleKeys(): void
    {
        $pairs = ['test-key-1' => 'value-1', 'test-key-2' => 'value-2'];
        $this->putAndTrack($pairs);

        foreach ($pairs as $key => $value) {
            $this->assertEquals($value, $this->testClient->get($key));
        }
    }

    public function testBinaryData(): void
    {
        $key = 'binary-key';
        $value = "\x00\x01\x02\x03\xff\xfe\xfd\xfc";

        $this->putOneAndTrack($key, $value);
        $this->assertEquals($value, $this->testClient->get($key));
    }

    public function testLargeValue(): void
    {
        $key = 'large-key';
        $value = str_repeat('x', 1024 * 1024); // 1MB

        $this->putOneAndTrack($key, $value);
        $this->assertEquals($value, $this->testClient->get($key));
    }

    public function testEmptyValue(): void
    {
        $key = 'empty-key';
        $this->putOneAndTrack($key, '');

        $this->assertEquals('', $this->testClient->get($key));
    }

    // ========================================================================
    // Batch operations
    // ========================================================================

    public function testBatchPutAndBatchGet(): void
    {
        $pairs = [
            'batch-key-1' => 'value-1',
            'batch-key-2' => 'value-2',
            'batch-key-3' => 'value-3',
        ];
        $this->putAndTrack($pairs);

        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertEquals($pairs, $results);
    }

    public function testBatchGetWithMissingKeys(): void
    {
        $this->putOneAndTrack('batch-key-1', 'value-1');
        $missingKey = 'non-existent-batch-key-' . uniqid();

        $results = $this->testClient->batchGet(['batch-key-1', $missingKey]);

        $this->assertEquals('value-1', $results['batch-key-1']);
        $this->assertNull($results[$missingKey]);
    }

    public function testBatchGetReturnsKeysInOrder(): void
    {
        $pairs = ['batch-key-1' => 'value-1', 'batch-key-2' => 'value-2'];
        $this->putAndTrack($pairs);

        // Request in reverse order
        $results = $this->testClient->batchGet(['batch-key-2', 'batch-key-1']);
        $this->assertEquals(['batch-key-2' => 'value-2', 'batch-key-1' => 'value-1'], $results);
    }

    public function testBatchGetEmptyArray(): void
    {
        $this->assertEquals([], $this->testClient->batchGet([]));
    }

    public function testBatchPutEmptyArray(): void
    {
        $this->testClient->batchPut([]);
        $this->addToAssertionCount(1);
    }

    public function testBatchDelete(): void
    {
        $pairs = ['batch-key-1' => 'value-1', 'batch-key-2' => 'value-2'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertEquals($pairs, $results);

        $this->testClient->batchDelete(array_keys($pairs));

        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertNull($results['batch-key-1']);
        $this->assertNull($results['batch-key-2']);
    }

    public function testBatchDeleteNonExistentKeys(): void
    {
        $this->testClient->batchDelete(['non-existent-key-' . uniqid(), 'non-existent-key-' . uniqid()]);
        $this->addToAssertionCount(1);
    }

    public function testBatchDeleteEmptyArray(): void
    {
        $this->testClient->batchDelete([]);
        $this->addToAssertionCount(1);
    }

    public function testBatchPutOverwritesExistingKeys(): void
    {
        $this->putOneAndTrack('batch-key-1', 'old-value');
        $this->assertEquals('old-value', $this->testClient->get('batch-key-1'));

        $this->testClient->batchPut(['batch-key-1' => 'new-value']);
        $this->assertEquals('new-value', $this->testClient->get('batch-key-1'));
    }

    public function testBatchOperationsWithBinaryData(): void
    {
        $pairs = [
            'batch-key-1' => "\x00\x01\x02\x03",
            'batch-key-2' => "\xff\xfe\xfd\xfc",
        ];
        $this->putAndTrack($pairs);

        $this->assertEquals($pairs, $this->testClient->batchGet(array_keys($pairs)));
    }

    // ========================================================================
    // Forward scan
    // ========================================================================

    public function testScan(): void
    {
        $pairs = ['scan-a' => 'value-a', 'scan-b' => 'value-b', 'scan-c' => 'value-c'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scan('scan-a', 'scan-d');

        $this->assertCount(3, $results);
        $this->assertEquals('scan-a', $results[0]['key']);
        $this->assertEquals('value-a', $results[0]['value']);
        $this->assertEquals('scan-b', $results[1]['key']);
        $this->assertEquals('scan-c', $results[2]['key']);
    }

    public function testScanStartKeyIsInclusive(): void
    {
        $pairs = ['scan-inc-a' => 'va', 'scan-inc-b' => 'vb', 'scan-inc-c' => 'vc'];
        $this->putAndTrack($pairs);

        // Start exactly at 'scan-inc-a' — it should be included
        $results = $this->testClient->scan('scan-inc-a', 'scan-inc-d');
        $keys = array_column($results, 'key');

        $this->assertContains('scan-inc-a', $keys, 'startKey should be inclusive in forward scan');
    }

    public function testScanEndKeyIsExclusive(): void
    {
        $pairs = ['scan-exc-a' => 'va', 'scan-exc-b' => 'vb', 'scan-exc-c' => 'vc'];
        $this->putAndTrack($pairs);

        // End exactly at 'scan-exc-c' — it should NOT be included
        $results = $this->testClient->scan('scan-exc-a', 'scan-exc-c');
        $keys = array_column($results, 'key');

        $this->assertContains('scan-exc-a', $keys);
        $this->assertContains('scan-exc-b', $keys);
        $this->assertNotContains('scan-exc-c', $keys, 'endKey should be exclusive in forward scan');
    }

    public function testScanWithLimit(): void
    {
        $pairs = ['scan-limit-1' => 'v1', 'scan-limit-2' => 'v2', 'scan-limit-3' => 'v3'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scan('scan-limit-', 'scan-limit.', 2);

        $this->assertCount(2, $results);
        $this->assertEquals('scan-limit-1', $results[0]['key']);
        $this->assertEquals('scan-limit-2', $results[1]['key']);
    }

    public function testScanLimitZeroReturnsAll(): void
    {
        $pairs = ['scan-all-a' => 'va', 'scan-all-b' => 'vb', 'scan-all-c' => 'vc'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scan('scan-all-', 'scan-all.', 0);

        $this->assertCount(3, $results);
    }

    public function testScanLimitOne(): void
    {
        $pairs = ['scan-one-a' => 'va', 'scan-one-b' => 'vb'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scan('scan-one-', 'scan-one.', 1);

        $this->assertCount(1, $results);
        $this->assertEquals('scan-one-a', $results[0]['key']);
    }

    public function testScanKeyOnly(): void
    {
        $this->putOneAndTrack('scan-keyonly', 'secret-value');

        $results = $this->testClient->scan('scan-keyonly', 'scan-keyonly.', 0, true);

        $this->assertCount(1, $results);
        $this->assertEquals('scan-keyonly', $results[0]['key']);
        $this->assertNull($results[0]['value']);
    }

    public function testScanEmptyRange(): void
    {
        $results = $this->testClient->scan('non-existent-prefix-', 'non-existent-prefix.');

        $this->assertCount(0, $results);
        $this->assertEquals([], $results);
    }

    public function testScanReturnsAscendingOrder(): void
    {
        // Insert in random order
        $this->putOneAndTrack('scan-ord-c', 'vc');
        $this->putOneAndTrack('scan-ord-a', 'va');
        $this->putOneAndTrack('scan-ord-b', 'vb');

        $results = $this->testClient->scan('scan-ord-', 'scan-ord.');
        $keys = array_column($results, 'key');

        $this->assertEquals(['scan-ord-a', 'scan-ord-b', 'scan-ord-c'], $keys);
    }

    public function testScanWithBinaryKeys(): void
    {
        // Keys with binary content, lexicographically ordered
        $k1 = "scan-bin-\x01";
        $k2 = "scan-bin-\x02";
        $k3 = "scan-bin-\x03";

        $this->putOneAndTrack($k1, 'v1');
        $this->putOneAndTrack($k2, 'v2');
        $this->putOneAndTrack($k3, 'v3');

        $results = $this->testClient->scan("scan-bin-\x00", "scan-bin-\x04");

        $this->assertCount(3, $results);
        $this->assertEquals($k1, $results[0]['key']);
        $this->assertEquals($k2, $results[1]['key']);
        $this->assertEquals($k3, $results[2]['key']);
    }

    public function testScanSingleKeyRange(): void
    {
        $this->putOneAndTrack('scan-single', 'val');

        // Range that contains exactly one key: [scan-single, scan-single\x00)
        $results = $this->testClient->scan('scan-single', "scan-single\x00");

        $this->assertCount(1, $results);
        $this->assertEquals('scan-single', $results[0]['key']);
    }

    // ========================================================================
    // Scan prefix
    // ========================================================================

    public function testScanPrefix(): void
    {
        $pairs = [
            'user:alice' => 'Alice',
            'user:bob' => 'Bob',
            'user:charlie' => 'Charlie',
            'other:data' => 'Other',
        ];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scanPrefix('user:');

        $this->assertCount(3, $results);
        $keys = array_column($results, 'key');
        $this->assertContains('user:alice', $keys);
        $this->assertContains('user:bob', $keys);
        $this->assertContains('user:charlie', $keys);
        $this->assertNotContains('other:data', $keys);
    }

    public function testScanPrefixWithLimit(): void
    {
        $pairs = ['pref-limit-1' => 'v1', 'pref-limit-2' => 'v2', 'pref-limit-3' => 'v3'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scanPrefix('pref-limit-', 2);

        $this->assertCount(2, $results);
    }

    public function testScanPrefixNoMatches(): void
    {
        $results = $this->testClient->scanPrefix('zzz-no-match-' . uniqid());

        $this->assertCount(0, $results);
    }

    public function testScanPrefixKeyOnly(): void
    {
        $pairs = ['pref-ko-a' => 'secret-a', 'pref-ko-b' => 'secret-b'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->scanPrefix('pref-ko-', 0, true);

        $this->assertCount(2, $results);
        foreach ($results as $result) {
            $this->assertNull($result['value'], 'keyOnly scan should return null values');
        }
    }

    // ========================================================================
    // Reverse scan — native TiKV reverse=true
    //
    // Protocol semantics (kvrpcpb.proto):
    //   reverseScan(startKey, endKey) scans [endKey, startKey) in descending order
    //   startKey = upper bound (exclusive)
    //   endKey   = lower bound (inclusive)
    // ========================================================================

    public function testReverseScanBasic(): void
    {
        $pairs = ['rev-x' => 'value-x', 'rev-y' => 'value-y', 'rev-z' => 'value-z'];
        $this->putAndTrack($pairs);

        // reverseScan(startKey='rev-z\x00', endKey='rev-w')
        // scans [rev-w, rev-z\x00) in descending order => rev-z, rev-y, rev-x
        $results = $this->testClient->reverseScan("rev-z\x00", 'rev-w');

        $this->assertCount(3, $results);
        $this->assertEquals('rev-z', $results[0]['key']);
        $this->assertEquals('value-z', $results[0]['value']);
        $this->assertEquals('rev-y', $results[1]['key']);
        $this->assertEquals('rev-x', $results[2]['key']);
    }

    public function testReverseScanStartKeyIsExclusive(): void
    {
        $pairs = ['rev-exc-a' => 'va', 'rev-exc-b' => 'vb', 'rev-exc-c' => 'vc'];
        $this->putAndTrack($pairs);

        // startKey='rev-exc-c' is exclusive, so 'rev-exc-c' should NOT appear
        $results = $this->testClient->reverseScan('rev-exc-c', 'rev-exc-a');
        $keys = array_column($results, 'key');

        $this->assertNotContains('rev-exc-c', $keys, 'startKey should be exclusive in reverse scan');
        $this->assertContains('rev-exc-b', $keys);
        $this->assertContains('rev-exc-a', $keys);
    }

    public function testReverseScanEndKeyIsInclusive(): void
    {
        $pairs = ['rev-inc-a' => 'va', 'rev-inc-b' => 'vb', 'rev-inc-c' => 'vc'];
        $this->putAndTrack($pairs);

        // endKey='rev-inc-a' is inclusive, so 'rev-inc-a' SHOULD appear
        $results = $this->testClient->reverseScan("rev-inc-c\x00", 'rev-inc-a');
        $keys = array_column($results, 'key');

        $this->assertContains('rev-inc-a', $keys, 'endKey should be inclusive in reverse scan');
    }

    public function testReverseScanWithLimit(): void
    {
        $pairs = ['rev-lim-a' => 'va', 'rev-lim-b' => 'vb', 'rev-lim-c' => 'vc'];
        $this->putAndTrack($pairs);

        // Scan from above 'rev-lim-c' down to 'rev-lim-', limit 2
        $results = $this->testClient->reverseScan("rev-lim-c\x00", 'rev-lim-', 2);

        $this->assertCount(2, $results);
        $this->assertEquals('rev-lim-c', $results[0]['key']);
        $this->assertEquals('rev-lim-b', $results[1]['key']);
    }

    public function testReverseScanLimitOne(): void
    {
        $pairs = ['rev-l1-a' => 'va', 'rev-l1-b' => 'vb', 'rev-l1-c' => 'vc'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan("rev-l1-c\x00", 'rev-l1-', 1);

        $this->assertCount(1, $results);
        $this->assertEquals('rev-l1-c', $results[0]['key']);
    }

    public function testReverseScanEmptyRange(): void
    {
        $results = $this->testClient->reverseScan('zzz-no-match.', 'zzz-no-match-');

        $this->assertCount(0, $results);
    }

    public function testReverseScanKeyOnly(): void
    {
        $pairs = ['rev-ko-a' => 'secret-a', 'rev-ko-b' => 'secret-b'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan("rev-ko-b\x00", 'rev-ko-a', 0, true);

        $this->assertCount(2, $results);
        $this->assertEquals('rev-ko-b', $results[0]['key']);
        $this->assertNull($results[0]['value'], 'keyOnly reverse scan should return null values');
        $this->assertEquals('rev-ko-a', $results[1]['key']);
        $this->assertNull($results[1]['value']);
    }

    public function testReverseScanReturnsDescendingOrder(): void
    {
        // Insert in random order
        $this->putOneAndTrack('rev-ord-b', 'vb');
        $this->putOneAndTrack('rev-ord-d', 'vd');
        $this->putOneAndTrack('rev-ord-a', 'va');
        $this->putOneAndTrack('rev-ord-c', 'vc');

        $results = $this->testClient->reverseScan('rev-ord-e', 'rev-ord-');
        $keys = array_column($results, 'key');

        $this->assertEquals(
            ['rev-ord-d', 'rev-ord-c', 'rev-ord-b', 'rev-ord-a'],
            $keys,
            'Reverse scan must return keys in descending lexicographic order'
        );
    }

    public function testReverseScanWithBinaryKeys(): void
    {
        $k1 = "rev-bin-\x01";
        $k2 = "rev-bin-\x02";
        $k3 = "rev-bin-\x03";

        $this->putOneAndTrack($k1, 'v1');
        $this->putOneAndTrack($k2, 'v2');
        $this->putOneAndTrack($k3, 'v3');

        $results = $this->testClient->reverseScan("rev-bin-\x04", "rev-bin-\x00");

        $this->assertCount(3, $results);
        $this->assertEquals($k3, $results[0]['key']);
        $this->assertEquals($k2, $results[1]['key']);
        $this->assertEquals($k1, $results[2]['key']);
    }

    public function testReverseScanSingleKey(): void
    {
        $this->putOneAndTrack('rev-single', 'val');

        // Range [rev-single, rev-single\x00) contains exactly one key
        $results = $this->testClient->reverseScan("rev-single\x00", 'rev-single');

        $this->assertCount(1, $results);
        $this->assertEquals('rev-single', $results[0]['key']);
        $this->assertEquals('val', $results[0]['value']);
    }

    public function testReverseScanLimitZeroReturnsAll(): void
    {
        $pairs = ['rev-all-a' => 'va', 'rev-all-b' => 'vb', 'rev-all-c' => 'vc'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan('rev-all-d', 'rev-all-', 0);

        $this->assertCount(3, $results);
    }

    public function testReverseScanValuesAreCorrect(): void
    {
        $pairs = ['rev-val-a' => 'alpha', 'rev-val-b' => 'bravo', 'rev-val-c' => 'charlie'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan('rev-val-d', 'rev-val-');

        $this->assertCount(3, $results);
        // Verify each key has the correct value
        $resultMap = [];
        foreach ($results as $r) {
            $resultMap[$r['key']] = $r['value'];
        }
        $this->assertEquals('alpha', $resultMap['rev-val-a']);
        $this->assertEquals('bravo', $resultMap['rev-val-b']);
        $this->assertEquals('charlie', $resultMap['rev-val-c']);
    }

    // ========================================================================
    // Forward scan ↔ Reverse scan consistency
    // ========================================================================

    public function testScanAndReverseScanReturnSameKeys(): void
    {
        $pairs = [
            'consist-1' => 'value-1',
            'consist-2' => 'value-2',
            'consist-3' => 'value-3',
        ];
        $this->putAndTrack($pairs);

        // Forward scan [consist-, consist.)
        $forward = $this->testClient->scan('consist-', 'consist.');

        // Reverse scan [consist-, consist.) — same logical range, reversed
        $reverse = $this->testClient->reverseScan('consist.', 'consist-');

        $this->assertCount(3, $forward, 'Forward scan should find 3 keys');
        $this->assertCount(3, $reverse, 'Reverse scan should find 3 keys');

        $forwardKeys = array_column($forward, 'key');
        $reverseKeys = array_column($reverse, 'key');

        $this->assertEquals(
            array_reverse($forwardKeys),
            $reverseKeys,
            'Reverse scan keys should be the mirror of forward scan keys'
        );
    }

    public function testScanAndReverseScanReturnSameValues(): void
    {
        $pairs = [
            'val-consist-a' => 'alpha',
            'val-consist-b' => 'bravo',
            'val-consist-c' => 'charlie',
        ];
        $this->putAndTrack($pairs);

        $forward = $this->testClient->scan('val-consist-', 'val-consist.');
        $reverse = $this->testClient->reverseScan('val-consist.', 'val-consist-');

        // Build key=>value maps from both
        $forwardMap = [];
        foreach ($forward as $r) {
            $forwardMap[$r['key']] = $r['value'];
        }
        $reverseMap = [];
        foreach ($reverse as $r) {
            $reverseMap[$r['key']] = $r['value'];
        }

        $this->assertEquals($forwardMap, $reverseMap, 'Both scans should return identical key-value pairs');
    }

    public function testScanAndReverseScanWithLimitAreComplementary(): void
    {
        $pairs = [
            'comp-a' => 'va',
            'comp-b' => 'vb',
            'comp-c' => 'vc',
            'comp-d' => 'vd',
            'comp-e' => 've',
        ];
        $this->putAndTrack($pairs);

        // Forward scan first 2
        $forwardFirst2 = $this->testClient->scan('comp-', 'comp.', 2);
        // Reverse scan last 2
        $reverseLast2 = $this->testClient->reverseScan('comp.', 'comp-', 2);

        $forwardKeys = array_column($forwardFirst2, 'key');
        $reverseKeys = array_column($reverseLast2, 'key');

        $this->assertEquals(['comp-a', 'comp-b'], $forwardKeys, 'Forward limit=2 should return first 2');
        $this->assertEquals(['comp-e', 'comp-d'], $reverseKeys, 'Reverse limit=2 should return last 2');

        // They should not overlap
        $this->assertEmpty(
            array_intersect($forwardKeys, $reverseKeys),
            'First 2 forward and last 2 reverse should not overlap with 5 keys'
        );
    }

    // ========================================================================
    // Edge cases and boundary conditions
    // ========================================================================

    public function testScanAdjacentKeys(): void
    {
        // Keys that are lexicographically adjacent
        $this->putOneAndTrack("adj-\x00", 'v0');
        $this->putOneAndTrack("adj-\x01", 'v1');

        $results = $this->testClient->scan("adj-\x00", "adj-\x02");

        $this->assertCount(2, $results);
    }

    public function testReverseScanAdjacentKeys(): void
    {
        $this->putOneAndTrack("rev-adj-\x00", 'v0');
        $this->putOneAndTrack("rev-adj-\x01", 'v1');

        $results = $this->testClient->reverseScan("rev-adj-\x02", "rev-adj-\x00");

        $this->assertCount(2, $results);
        $this->assertEquals("rev-adj-\x01", $results[0]['key']);
        $this->assertEquals("rev-adj-\x00", $results[1]['key']);
    }

    public function testScanAfterDeleteReturnsNothing(): void
    {
        $this->putOneAndTrack('scan-del-a', 'va');
        $this->putOneAndTrack('scan-del-b', 'vb');

        $this->testClient->delete('scan-del-a');
        $this->testClient->delete('scan-del-b');

        $results = $this->testClient->scan('scan-del-', 'scan-del.');
        $this->assertCount(0, $results);
    }

    public function testReverseScanAfterDeleteReturnsNothing(): void
    {
        $this->putOneAndTrack('rev-del-a', 'va');
        $this->putOneAndTrack('rev-del-b', 'vb');

        $this->testClient->delete('rev-del-a');
        $this->testClient->delete('rev-del-b');

        $results = $this->testClient->reverseScan('rev-del.', 'rev-del-');
        $this->assertCount(0, $results);
    }

    public function testScanAfterOverwriteReturnsNewValues(): void
    {
        $this->putOneAndTrack('scan-ow-a', 'old-a');
        $this->putOneAndTrack('scan-ow-b', 'old-b');

        // Overwrite
        $this->testClient->put('scan-ow-a', 'new-a');
        $this->testClient->put('scan-ow-b', 'new-b');

        $results = $this->testClient->scan('scan-ow-', 'scan-ow.');

        $this->assertCount(2, $results);
        $this->assertEquals('new-a', $results[0]['value']);
        $this->assertEquals('new-b', $results[1]['value']);
    }

    public function testReverseScanAfterOverwriteReturnsNewValues(): void
    {
        $this->putOneAndTrack('rev-ow-a', 'old-a');
        $this->putOneAndTrack('rev-ow-b', 'old-b');

        $this->testClient->put('rev-ow-a', 'new-a');
        $this->testClient->put('rev-ow-b', 'new-b');

        $results = $this->testClient->reverseScan('rev-ow.', 'rev-ow-');

        $this->assertCount(2, $results);
        $this->assertEquals('new-b', $results[0]['value']);
        $this->assertEquals('new-a', $results[1]['value']);
    }

    public function testScanPartialDeleteShowsRemainingKeys(): void
    {
        $this->putOneAndTrack('scan-pd-a', 'va');
        $this->putOneAndTrack('scan-pd-b', 'vb');
        $this->putOneAndTrack('scan-pd-c', 'vc');

        $this->testClient->delete('scan-pd-b');

        $results = $this->testClient->scan('scan-pd-', 'scan-pd.');
        $keys = array_column($results, 'key');

        $this->assertCount(2, $results);
        $this->assertContains('scan-pd-a', $keys);
        $this->assertNotContains('scan-pd-b', $keys);
        $this->assertContains('scan-pd-c', $keys);
    }

    public function testReverseScanPartialDeleteShowsRemainingKeys(): void
    {
        $this->putOneAndTrack('rev-pd-a', 'va');
        $this->putOneAndTrack('rev-pd-b', 'vb');
        $this->putOneAndTrack('rev-pd-c', 'vc');

        $this->testClient->delete('rev-pd-b');

        $results = $this->testClient->reverseScan('rev-pd.', 'rev-pd-');
        $keys = array_column($results, 'key');

        $this->assertCount(2, $results);
        $this->assertEquals('rev-pd-c', $keys[0]);
        $this->assertEquals('rev-pd-a', $keys[1]);
    }

    public function testScanLimitExceedingTotalKeysReturnsAll(): void
    {
        $pairs = ['scan-big-a' => 'va', 'scan-big-b' => 'vb'];
        $this->putAndTrack($pairs);

        // Limit 100 but only 2 keys exist
        $results = $this->testClient->scan('scan-big-', 'scan-big.', 100);

        $this->assertCount(2, $results);
    }

    public function testReverseScanLimitExceedingTotalKeysReturnsAll(): void
    {
        $pairs = ['rev-big-a' => 'va', 'rev-big-b' => 'vb'];
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan('rev-big.', 'rev-big-', 100);

        $this->assertCount(2, $results);
    }

    public function testScanManyKeys(): void
    {
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('scan-many-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $results = $this->testClient->scan('scan-many-', 'scan-many.');

        $this->assertCount(50, $results);

        // Verify ascending order
        $keys = array_column($results, 'key');
        $sorted = $keys;
        sort($sorted);
        $this->assertEquals($sorted, $keys, 'Forward scan of many keys should be in ascending order');
    }

    public function testReverseScanManyKeys(): void
    {
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('rev-many-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan('rev-many.', 'rev-many-');

        $this->assertCount(50, $results);

        // Verify descending order
        $keys = array_column($results, 'key');
        $sorted = $keys;
        rsort($sorted);
        $this->assertEquals($sorted, $keys, 'Reverse scan of many keys should be in descending order');
    }

    public function testReverseScanManyKeysWithLimit(): void
    {
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('rev-ml-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $results = $this->testClient->reverseScan('rev-ml.', 'rev-ml-', 10);

        $this->assertCount(10, $results);
        // Should be the last 10 keys in descending order
        $this->assertEquals('rev-ml-049', $results[0]['key']);
        $this->assertEquals('rev-ml-040', $results[9]['key']);
    }

    // ========================================================================
    // ScanIterator
    // ========================================================================

    public function testScanIteratorReturnsSameResultsAsScan(): void
    {
        $pairs = ['sit-a' => 'va', 'sit-b' => 'vb', 'sit-c' => 'vc'];
        $this->putAndTrack($pairs);

        $scanResults = $this->testClient->scan('sit-', 'sit.', 0);
        $iteratorResults = [];
        foreach ($this->testClient->scanIterator('sit-', 'sit.', 10) as $key => $value) {
            $iteratorResults[$key] = $value;
        }

        $expected = [];
        foreach ($scanResults as $r) {
            $expected[$r['key']] = $r['value'];
        }

        $this->assertSame($expected, $iteratorResults);
    }

    public function testScanIteratorWithSmallBatchSize(): void
    {
        $pairs = [];
        for ($i = 0; $i < 10; $i++) {
            $pairs[sprintf('sit-small-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $iterator = $this->testClient->scanIterator('sit-small-', 'sit-small.', 2);
        $results = iterator_to_array($iterator);

        $this->assertCount(10, $results);
        $this->assertArrayHasKey('sit-small-000', $results);
        $this->assertSame('value-0', $results['sit-small-000']);
        $this->assertArrayHasKey('sit-small-009', $results);
        $this->assertSame('value-9', $results['sit-small-009']);
    }

    public function testScanIteratorEmptyRange(): void
    {
        $iterator = $this->testClient->scanIterator('sit-empty-zzz', 'sit-empty-zzzz', 10);
        $results = iterator_to_array($iterator);

        $this->assertCount(0, $results);
    }

    public function testScanIteratorWithSmallBatchReturnsAllResults(): void
    {
        $pairs = ['sit-lim-a' => 'va', 'sit-lim-b' => 'vb', 'sit-lim-c' => 'vc'];
        $this->putAndTrack($pairs);

        // batchSize=2 means 2 per page, but all 3 keys should be returned
        $iteratorResults = [];
        foreach ($this->testClient->scanIterator('sit-lim-', 'sit-lim.', 2) as $key => $value) {
            $iteratorResults[$key] = $value;
        }

        $this->assertCount(3, $iteratorResults);
        $this->assertSame('va', $iteratorResults['sit-lim-a']);
        $this->assertSame('vb', $iteratorResults['sit-lim-b']);
        $this->assertSame('vc', $iteratorResults['sit-lim-c']);
    }

    public function testScanIteratorPrefix(): void
    {
        $pairs = [
            'sit-pref:alice' => 'Alice',
            'sit-pref:bob' => 'Bob',
            'other:data' => 'Other',
        ];
        $this->putAndTrack($pairs);

        $iterator = $this->testClient->scanPrefixIterator('sit-pref:', 10);
        $results = iterator_to_array($iterator);

        $this->assertCount(2, $results);
        $this->assertArrayHasKey('sit-pref:alice', $results);
        $this->assertArrayHasKey('sit-pref:bob', $results);
        $this->assertArrayNotHasKey('other:data', $results);
    }

    public function testScanIteratorKeyOnly(): void
    {
        $this->testClient->put('sit-ko-a', 'secret-a');
        $this->keysToCleanup[] = 'sit-ko-a';
        $this->testClient->put('sit-ko-b', 'secret-b');
        $this->keysToCleanup[] = 'sit-ko-b';

        $results = [];
        foreach ($this->testClient->scanIterator('sit-ko-', 'sit-ko.', 10, true) as $key => $value) {
            $results[$key] = $value;
        }

        $this->assertCount(2, $results);
        $this->assertNull($results['sit-ko-a']);
        $this->assertNull($results['sit-ko-b']);
    }

    public function testScanIteratorMultipleBatchesAcrossRegions(): void
    {
        // Insert enough keys to span multiple batches
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('sit-reg-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $iterator = $this->testClient->scanIterator('sit-reg-', 'sit-reg.', 10);
        $results = iterator_to_array($iterator);

        $this->assertCount(50, $results);

        // Verify all values are correct
        for ($i = 0; $i < 50; $i++) {
            $key = sprintf('sit-reg-%03d', $i);
            $this->assertArrayHasKey($key, $results);
            $this->assertSame("value-$i", $results[$key]);
        }
    }

    public function testScanIteratorRewind(): void
    {
        $pairs = ['sit-rew-a' => 'va', 'sit-rew-b' => 'vb'];
        $this->putAndTrack($pairs);

        $iterator = $this->testClient->scanIterator('sit-rew-', 'sit-rew.', 10);

        // First pass
        $first = iterator_to_array($iterator);
        $this->assertCount(2, $first);

        // Rewind
        $iterator->rewind();
        $second = iterator_to_array($iterator);

        $this->assertSame($first, $second);
    }

    public function testScanIteratorEarlyBreak(): void
    {
        $pairs = [];
        for ($i = 0; $i < 20; $i++) {
            $pairs[sprintf('sit-brk-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $iterator = $this->testClient->scanIterator('sit-brk-', 'sit-brk.', 5);
        $count = 0;
        foreach ($iterator as $value) {
            $count++;
            if ($count >= 3) {
                break;
            }
        }

        $this->assertSame(3, $count);
    }

    // ========================================================================
    // DeleteRange
    // ========================================================================

    public function testDeleteRangeBasic(): void
    {
        $pairs = ['dr-a' => 'va', 'dr-b' => 'vb', 'dr-c' => 'vc'];
        $this->putAndTrack($pairs);

        // Delete range [dr-a, dr-d) — should delete all three
        $this->testClient->deleteRange('dr-a', 'dr-d');

        $this->assertNull($this->testClient->get('dr-a'));
        $this->assertNull($this->testClient->get('dr-b'));
        $this->assertNull($this->testClient->get('dr-c'));
    }

    public function testDeleteRangePartial(): void
    {
        $pairs = ['dr-part-a' => 'va', 'dr-part-b' => 'vb', 'dr-part-c' => 'vc'];
        $this->putAndTrack($pairs);

        // Delete only [dr-part-a, dr-part-c) — should delete a and b, keep c
        $this->testClient->deleteRange('dr-part-a', 'dr-part-c');

        $this->assertNull($this->testClient->get('dr-part-a'));
        $this->assertNull($this->testClient->get('dr-part-b'));
        $this->assertEquals(
            'vc',
            $this->testClient->get('dr-part-c'),
            'endKey is exclusive — dr-part-c should survive',
        );
    }

    public function testDeleteRangeStartKeyIsInclusive(): void
    {
        $this->putOneAndTrack('dr-inc-a', 'va');
        $this->putOneAndTrack('dr-inc-b', 'vb');

        $this->testClient->deleteRange('dr-inc-a', 'dr-inc-c');

        $this->assertNull($this->testClient->get('dr-inc-a'), 'startKey should be inclusive');
    }

    public function testDeleteRangeEndKeyIsExclusive(): void
    {
        $this->putOneAndTrack('dr-exc-a', 'va');
        $this->putOneAndTrack('dr-exc-b', 'vb');

        $this->testClient->deleteRange('dr-exc-a', 'dr-exc-b');

        $this->assertNull($this->testClient->get('dr-exc-a'));
        $this->assertEquals('vb', $this->testClient->get('dr-exc-b'), 'endKey should be exclusive');
    }

    public function testDeleteRangeEmptyRange(): void
    {
        $this->putOneAndTrack('dr-empty-a', 'va');

        // Delete a range with no keys
        $this->testClient->deleteRange('dr-empty-zzz', 'dr-empty-zzzz');

        // Original key should be untouched
        $this->assertEquals('va', $this->testClient->get('dr-empty-a'));
    }

    public function testDeleteRangeSameStartAndEnd(): void
    {
        $this->putOneAndTrack('dr-same', 'val');

        // Same start and end — should be a no-op
        $this->testClient->deleteRange('dr-same', 'dr-same');

        $this->assertEquals('val', $this->testClient->get('dr-same'));
    }

    public function testDeleteRangeSingleKey(): void
    {
        $this->putOneAndTrack('dr-single', 'val');

        // Range [dr-single, dr-single\x00) contains exactly one key
        $this->testClient->deleteRange('dr-single', "dr-single\x00");

        $this->assertNull($this->testClient->get('dr-single'));
    }

    public function testDeleteRangeVerifyWithScan(): void
    {
        $pairs = [
            'dr-scan-a' => 'va',
            'dr-scan-b' => 'vb',
            'dr-scan-c' => 'vc',
            'dr-scan-d' => 'vd',
        ];
        $this->putAndTrack($pairs);

        // Delete middle range [dr-scan-b, dr-scan-d)
        $this->testClient->deleteRange('dr-scan-b', 'dr-scan-d');

        $results = $this->testClient->scan('dr-scan-', 'dr-scan.');
        $keys = array_column($results, 'key');

        $this->assertCount(2, $results);
        $this->assertEquals(['dr-scan-a', 'dr-scan-d'], $keys);
    }

    public function testDeleteRangeManyKeys(): void
    {
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('dr-many-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        // Verify all exist
        $results = $this->testClient->scan('dr-many-', 'dr-many.');
        $this->assertCount(50, $results);

        // Delete all
        $this->testClient->deleteRange('dr-many-', 'dr-many.');

        // Verify all gone
        $results = $this->testClient->scan('dr-many-', 'dr-many.');
        $this->assertCount(0, $results);
    }

    public function testDeleteRangeWithBinaryKeys(): void
    {
        $k1 = "dr-bin-\x01";
        $k2 = "dr-bin-\x02";
        $k3 = "dr-bin-\x03";

        $this->putOneAndTrack($k1, 'v1');
        $this->putOneAndTrack($k2, 'v2');
        $this->putOneAndTrack($k3, 'v3');

        $this->testClient->deleteRange("dr-bin-\x01", "dr-bin-\x03");

        $this->assertNull($this->testClient->get($k1));
        $this->assertNull($this->testClient->get($k2));
        $this->assertEquals('v3', $this->testClient->get($k3), 'endKey is exclusive');
    }

    public function testDeleteRangeDoesNotAffectOutsideKeys(): void
    {
        $this->putOneAndTrack('dr-out-before', 'before');
        $this->putOneAndTrack('dr-out-target-a', 'ta');
        $this->putOneAndTrack('dr-out-target-b', 'tb');
        $this->putOneAndTrack('dr-out-zafter', 'after');

        $this->testClient->deleteRange('dr-out-target-', 'dr-out-target.');

        $this->assertEquals('before', $this->testClient->get('dr-out-before'));
        $this->assertNull($this->testClient->get('dr-out-target-a'));
        $this->assertNull($this->testClient->get('dr-out-target-b'));
        $this->assertEquals('after', $this->testClient->get('dr-out-zafter'));
    }

    // ========================================================================
    // DeletePrefix
    // ========================================================================

    public function testDeletePrefixBasic(): void
    {
        $pairs = [
            'dp-user:alice' => 'Alice',
            'dp-user:bob' => 'Bob',
            'dp-user:charlie' => 'Charlie',
            'dp-other:data' => 'Other',
        ];
        $this->putAndTrack($pairs);

        $this->testClient->deletePrefix('dp-user:');

        $this->assertNull($this->testClient->get('dp-user:alice'));
        $this->assertNull($this->testClient->get('dp-user:bob'));
        $this->assertNull($this->testClient->get('dp-user:charlie'));
        $this->assertEquals('Other', $this->testClient->get('dp-other:data'), 'Keys outside prefix should survive');
    }

    public function testDeletePrefixNoMatches(): void
    {
        $this->putOneAndTrack('dp-survive', 'val');

        // Delete a prefix that doesn't match anything
        $this->testClient->deletePrefix('dp-nonexistent-');

        $this->assertEquals('val', $this->testClient->get('dp-survive'));
    }

    public function testDeletePrefixVerifyWithScanPrefix(): void
    {
        $pairs = [
            'dp-scan-a' => 'va',
            'dp-scan-b' => 'vb',
            'dp-scan-c' => 'vc',
        ];
        $this->putAndTrack($pairs);

        // Verify keys exist
        $results = $this->testClient->scanPrefix('dp-scan-');
        $this->assertCount(3, $results);

        // Delete by prefix
        $this->testClient->deletePrefix('dp-scan-');

        // Verify all gone
        $results = $this->testClient->scanPrefix('dp-scan-');
        $this->assertCount(0, $results);
    }

    public function testDeletePrefixManyKeys(): void
    {
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('dp-many-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $this->testClient->deletePrefix('dp-many-');

        $results = $this->testClient->scanPrefix('dp-many-');
        $this->assertCount(0, $results);
    }

    public function testDeletePrefixEmptyStringThrows(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->testClient->deletePrefix('');
    }

    public function testDeletePrefixThenReinsert(): void
    {
        $this->putOneAndTrack('dp-reins-a', 'old-a');
        $this->putOneAndTrack('dp-reins-b', 'old-b');

        $this->testClient->deletePrefix('dp-reins-');

        $this->assertNull($this->testClient->get('dp-reins-a'));

        // Re-insert
        $this->putOneAndTrack('dp-reins-a', 'new-a');

        $this->assertEquals('new-a', $this->testClient->get('dp-reins-a'));
        $this->assertNull($this->testClient->get('dp-reins-b'));
    }

    public function testDeletePrefixDoesNotAffectSiblingPrefixes(): void
    {
        $this->putOneAndTrack('dp-sib-aaa-1', 'v1');
        $this->putOneAndTrack('dp-sib-aab-1', 'v2');
        $this->putOneAndTrack('dp-sib-aac-1', 'v3');

        $this->testClient->deletePrefix('dp-sib-aab');

        $this->assertEquals('v1', $this->testClient->get('dp-sib-aaa-1'));
        $this->assertNull($this->testClient->get('dp-sib-aab-1'));
        $this->assertEquals('v3', $this->testClient->get('dp-sib-aac-1'));
    }

    // ========================================================================
    // TTL Operations
    // ========================================================================

    public function testPutWithTtl(): void
    {
        // Put with 60 second TTL
        $this->testClient->put('ttl-basic', 'value', 60);
        $this->keysToCleanup[] = 'ttl-basic';

        $this->assertEquals('value', $this->testClient->get('ttl-basic'));
    }

    public function testPutWithTtlKeyExpires(): void
    {
        // Put with 2 second TTL
        $this->testClient->put('ttl-expire', 'temporary', 2);
        $this->keysToCleanup[] = 'ttl-expire';

        // Key should exist immediately
        $this->assertEquals('temporary', $this->testClient->get('ttl-expire'));

        // Wait for expiration
        sleep(3);

        // Key should be gone
        $this->assertNull($this->testClient->get('ttl-expire'), 'Key should expire after TTL');
    }

    public function testPutWithoutTtlDoesNotExpire(): void
    {
        // Put without TTL (default = 0 = no expiration)
        $this->putOneAndTrack('ttl-none', 'permanent');

        $this->assertEquals('permanent', $this->testClient->get('ttl-none'));

        // getKeyTTL should return null for keys without TTL
        $ttl = $this->testClient->getKeyTTL('ttl-none');
        $this->assertNull($ttl, 'Key without TTL should return null from getKeyTTL');
    }

    public function testGetKeyTtlReturnsRemainingTime(): void
    {
        // Put with 60 second TTL
        $this->testClient->put('ttl-remaining', 'value', 60);
        $this->keysToCleanup[] = 'ttl-remaining';

        $ttl = $this->testClient->getKeyTTL('ttl-remaining');

        $this->assertNotNull($ttl, 'Key with TTL should return a value');
        $this->assertGreaterThan(0, $ttl, 'Remaining TTL should be positive');
        $this->assertLessThanOrEqual(60, $ttl, 'Remaining TTL should not exceed original TTL');
    }

    public function testGetKeyTtlNonExistentKey(): void
    {
        $ttl = $this->testClient->getKeyTTL('ttl-nonexistent-' . uniqid());

        $this->assertNull($ttl, 'Non-existent key should return null');
    }

    public function testGetKeyTtlAfterExpiration(): void
    {
        $this->testClient->put('ttl-expired', 'temp', 2);
        $this->keysToCleanup[] = 'ttl-expired';

        // Wait for expiration
        sleep(3);

        $ttl = $this->testClient->getKeyTTL('ttl-expired');
        $this->assertNull($ttl, 'Expired key should return null from getKeyTTL');
    }

    public function testPutWithTtlOverwriteRefreshesTtl(): void
    {
        // Put with short TTL
        $this->testClient->put('ttl-refresh', 'old', 2);
        $this->keysToCleanup[] = 'ttl-refresh';

        // Overwrite with longer TTL
        $this->testClient->put('ttl-refresh', 'new', 60);

        // Wait past original TTL
        sleep(3);

        // Key should still exist with new value
        $this->assertEquals(
            'new',
            $this->testClient->get('ttl-refresh'),
            'Overwritten key should survive past original TTL',
        );

        $ttl = $this->testClient->getKeyTTL('ttl-refresh');
        $this->assertNotNull($ttl);
        $this->assertGreaterThan(0, $ttl);
    }

    public function testPutWithTtlZeroMeansNoExpiration(): void
    {
        // Explicit TTL=0 should behave same as no TTL
        $this->testClient->put('ttl-zero', 'permanent', 0);
        $this->keysToCleanup[] = 'ttl-zero';

        $this->assertEquals('permanent', $this->testClient->get('ttl-zero'));

        $ttl = $this->testClient->getKeyTTL('ttl-zero');
        $this->assertNull($ttl, 'TTL=0 should mean no expiration');
    }

    public function testBatchPutWithTtl(): void
    {
        $pairs = [
            'ttl-batch-a' => 'va',
            'ttl-batch-b' => 'vb',
        ];
        $this->testClient->batchPut($pairs, 60);
        $this->keysToCleanup[] = 'ttl-batch-a';
        $this->keysToCleanup[] = 'ttl-batch-b';

        $this->assertEquals('va', $this->testClient->get('ttl-batch-a'));
        $this->assertEquals('vb', $this->testClient->get('ttl-batch-b'));

        // Both should have TTL
        $ttlA = $this->testClient->getKeyTTL('ttl-batch-a');
        $ttlB = $this->testClient->getKeyTTL('ttl-batch-b');
        $this->assertNotNull($ttlA);
        $this->assertNotNull($ttlB);
        $this->assertGreaterThan(0, $ttlA);
        $this->assertGreaterThan(0, $ttlB);
    }

    public function testBatchPutWithTtlExpires(): void
    {
        $pairs = [
            'ttl-bexp-a' => 'va',
            'ttl-bexp-b' => 'vb',
        ];
        $this->testClient->batchPut($pairs, 2);
        $this->keysToCleanup[] = 'ttl-bexp-a';
        $this->keysToCleanup[] = 'ttl-bexp-b';

        // Should exist immediately
        $this->assertEquals('va', $this->testClient->get('ttl-bexp-a'));

        // Wait for expiration
        sleep(3);

        $this->assertNull($this->testClient->get('ttl-bexp-a'), 'Batch put key should expire after TTL');
        $this->assertNull($this->testClient->get('ttl-bexp-b'), 'Batch put key should expire after TTL');
    }

    public function testBatchPutWithoutTtl(): void
    {
        $pairs = ['ttl-bnone-a' => 'va', 'ttl-bnone-b' => 'vb'];
        $this->testClient->batchPut($pairs); // no TTL
        $this->keysToCleanup[] = 'ttl-bnone-a';
        $this->keysToCleanup[] = 'ttl-bnone-b';

        $ttl = $this->testClient->getKeyTTL('ttl-bnone-a');
        $this->assertNull($ttl, 'batchPut without TTL should not set expiration');
    }

    public function testBatchPutWithPerKeyTtls(): void
    {
        $pairs = [
            'ttl-pk-a' => 'short-lived',
            'ttl-pk-b' => 'long-lived',
        ];
        $this->testClient->batchPut($pairs, ['ttl-pk-a' => 2, 'ttl-pk-b' => 60]);
        $this->keysToCleanup[] = 'ttl-pk-a';
        $this->keysToCleanup[] = 'ttl-pk-b';

        // Both values readable immediately
        $this->assertEquals('short-lived', $this->testClient->get('ttl-pk-a'));
        $this->assertEquals('long-lived', $this->testClient->get('ttl-pk-b'));

        // Short-lived key expires, long-lived persists
        sleep(3);

        $this->assertNull($this->testClient->get('ttl-pk-a'), 'Per-key TTL: 2s key should expire');
        $this->assertEquals('long-lived', $this->testClient->get('ttl-pk-b'), 'Per-key TTL: 60s key should persist');
    }

    public function testBatchPutWithMixedTtlZeroAndNonZero(): void
    {
        $pairs = [
            'ttl-mix-a' => 'no-expiry',
            'ttl-mix-b' => 'with-ttl',
        ];
        $this->testClient->batchPut($pairs, ['ttl-mix-a' => 0, 'ttl-mix-b' => 2]);
        $this->keysToCleanup[] = 'ttl-mix-a';
        $this->keysToCleanup[] = 'ttl-mix-b';

        // TTL=0 key should have no expiration
        $ttlA = $this->testClient->getKeyTTL('ttl-mix-a');
        $this->assertNull($ttlA, 'TTL=0 key should have no expiration');

        // Wait for the 2s TTL key to expire
        sleep(3);

        $this->assertEquals('no-expiry', $this->testClient->get('ttl-mix-a'), 'TTL=0 key should survive');
        $this->assertNull($this->testClient->get('ttl-mix-b'), 'TTL=2 key should expire');
    }

    public function testBatchPutWithPositionalArrayTtl(): void
    {
        $pairs = [
            'ttl-pos-a' => 'short',
            'ttl-pos-b' => 'long',
        ];
        $this->testClient->batchPut($pairs, [2, 60]);
        $this->keysToCleanup[] = 'ttl-pos-a';
        $this->keysToCleanup[] = 'ttl-pos-b';

        // Both readable immediately
        $this->assertEquals('short', $this->testClient->get('ttl-pos-a'));
        $this->assertEquals('long', $this->testClient->get('ttl-pos-b'));

        // Wait for first key to expire
        sleep(3);

        $this->assertNull($this->testClient->get('ttl-pos-a'), 'Positional TTL: first key (2s) should expire');
        $this->assertEquals(
            'long',
            $this->testClient->get('ttl-pos-b'),
            'Positional TTL: second key (60s) should persist',
        );
    }

    public function testScanIncludesKeysWithTtl(): void
    {
        $this->testClient->put('ttl-scan-a', 'va', 60);
        $this->testClient->put('ttl-scan-b', 'vb', 60);
        $this->keysToCleanup[] = 'ttl-scan-a';
        $this->keysToCleanup[] = 'ttl-scan-b';

        $results = $this->testClient->scan('ttl-scan-', 'ttl-scan.');

        $this->assertCount(2, $results);
        $this->assertEquals('ttl-scan-a', $results[0]['key']);
        $this->assertEquals('va', $results[0]['value']);
    }

    public function testScanExcludesExpiredKeys(): void
    {
        $this->testClient->put('ttl-scanexp-a', 'va', 2);
        $this->putOneAndTrack('ttl-scanexp-b', 'vb'); // no TTL
        $this->keysToCleanup[] = 'ttl-scanexp-a';

        sleep(3);

        $results = $this->testClient->scan('ttl-scanexp-', 'ttl-scanexp.');
        $keys = array_column($results, 'key');

        $this->assertNotContains('ttl-scanexp-a', $keys, 'Expired key should not appear in scan');
        $this->assertContains('ttl-scanexp-b', $keys, 'Non-expired key should appear in scan');
    }

    // ========================================================================
    // CompareAndSwap (CAS)
    // ========================================================================

    public function testCasSuccessfulSwap(): void
    {
        $this->putOneAndTrack('cas-basic', 'old-value');

        $result = $this->testClient->compareAndSwap('cas-basic', 'old-value', 'new-value');

        $this->assertInstanceOf(CasResult::class, $result);
        $this->assertTrue($result->swapped, 'CAS should succeed when expected value matches');
        $this->assertEquals('old-value', $result->previousValue);

        // Verify the new value is stored
        $this->assertEquals('new-value', $this->testClient->get('cas-basic'));
    }

    public function testCasFailedSwapValueMismatch(): void
    {
        $this->putOneAndTrack('cas-fail', 'actual-value');

        $result = $this->testClient->compareAndSwap('cas-fail', 'wrong-expected', 'new-value');

        $this->assertFalse($result->swapped, 'CAS should fail when expected value does not match');
        $this->assertEquals('actual-value', $result->previousValue, 'Should return the actual current value');

        // Verify the value was NOT changed
        $this->assertEquals('actual-value', $this->testClient->get('cas-fail'));
    }

    public function testCasExpectNullKeyDoesNotExist(): void
    {
        $key = 'cas-null-' . uniqid();
        $this->keysToCleanup[] = $key;

        // CAS with expectedValue=null on a non-existent key should succeed
        $result = $this->testClient->compareAndSwap($key, null, 'created');

        $this->assertTrue($result->swapped, 'CAS with null expected should succeed when key does not exist');
        $this->assertNull($result->previousValue, 'Previous value should be null for non-existent key');

        // Verify the value was created
        $this->assertEquals('created', $this->testClient->get($key));
    }

    public function testCasExpectNullKeyExists(): void
    {
        $this->putOneAndTrack('cas-null-exists', 'existing');

        // CAS with expectedValue=null on an existing key should fail
        $result = $this->testClient->compareAndSwap('cas-null-exists', null, 'new-value');

        $this->assertFalse($result->swapped, 'CAS with null expected should fail when key exists');
        $this->assertEquals('existing', $result->previousValue, 'Should return the existing value');

        // Verify the value was NOT changed
        $this->assertEquals('existing', $this->testClient->get('cas-null-exists'));
    }

    public function testCasMultipleSwapsInSequence(): void
    {
        $this->putOneAndTrack('cas-seq', 'v1');

        // First swap: v1 → v2
        $r1 = $this->testClient->compareAndSwap('cas-seq', 'v1', 'v2');
        $this->assertTrue($r1->swapped);
        $this->assertEquals('v1', $r1->previousValue);

        // Second swap: v2 → v3
        $r2 = $this->testClient->compareAndSwap('cas-seq', 'v2', 'v3');
        $this->assertTrue($r2->swapped);
        $this->assertEquals('v2', $r2->previousValue);

        // Third swap with stale expected: should fail
        $r3 = $this->testClient->compareAndSwap('cas-seq', 'v1', 'v4');
        $this->assertFalse($r3->swapped);
        $this->assertEquals('v3', $r3->previousValue);

        // Final value should be v3
        $this->assertEquals('v3', $this->testClient->get('cas-seq'));
    }

    public function testCasWithBinaryData(): void
    {
        $key = "cas-bin-\x01\x02";
        $oldValue = "\x00\x01\x02\x03";
        $newValue = "\xff\xfe\xfd\xfc";

        $this->testClient->put($key, $oldValue);
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->compareAndSwap($key, $oldValue, $newValue);

        $this->assertTrue($result->swapped);
        $this->assertEquals($oldValue, $result->previousValue);
        $this->assertEquals($newValue, $this->testClient->get($key));
    }

    public function testCasWithEmptyStringValue(): void
    {
        $this->putOneAndTrack('cas-empty', 'non-empty');

        // Swap to empty string
        $result = $this->testClient->compareAndSwap('cas-empty', 'non-empty', '');

        $this->assertTrue($result->swapped);
        $this->assertEquals('non-empty', $result->previousValue);
        $this->assertEquals('', $this->testClient->get('cas-empty'));
    }

    public function testCasWithTtl(): void
    {
        $key = 'cas-ttl-' . uniqid();
        $this->keysToCleanup[] = $key;

        // Create key via CAS with TTL
        $result = $this->testClient->compareAndSwap($key, null, 'temp-value', 60);
        $this->assertTrue($result->swapped);

        // Verify TTL was set
        $ttl = $this->testClient->getKeyTTL($key);
        $this->assertNotNull($ttl, 'CAS with TTL should set expiration');
        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(60, $ttl);
    }

    public function testCasWithTtlExpires(): void
    {
        $key = 'cas-ttl-exp-' . uniqid();
        $this->keysToCleanup[] = $key;

        // Create key via CAS with short TTL
        $result = $this->testClient->compareAndSwap($key, null, 'temporary', 2);
        $this->assertTrue($result->swapped);
        $this->assertEquals('temporary', $this->testClient->get($key));

        // Wait for expiration
        sleep(3);

        $this->assertNull($this->testClient->get($key), 'CAS key should expire after TTL');
    }

    public function testCasSwapThenSwapAgain(): void
    {
        $this->putOneAndTrack('cas-double', 'initial');

        // First CAS succeeds
        $r1 = $this->testClient->compareAndSwap('cas-double', 'initial', 'middle');
        $this->assertTrue($r1->swapped);

        // Second CAS with the new value succeeds
        $r2 = $this->testClient->compareAndSwap('cas-double', 'middle', 'final');
        $this->assertTrue($r2->swapped);

        $this->assertEquals('final', $this->testClient->get('cas-double'));
    }

    public function testCasOnDeletedKey(): void
    {
        $this->putOneAndTrack('cas-deleted', 'value');
        $this->testClient->delete('cas-deleted');

        // CAS with null expected on a deleted key should succeed
        $result = $this->testClient->compareAndSwap('cas-deleted', null, 'resurrected');
        $this->keysToCleanup[] = 'cas-deleted';

        $this->assertTrue($result->swapped, 'CAS with null expected should succeed on deleted key');
        $this->assertEquals('resurrected', $this->testClient->get('cas-deleted'));
    }

    public function testCasOnDeletedKeyWithWrongExpected(): void
    {
        $this->putOneAndTrack('cas-del-wrong', 'value');
        $this->testClient->delete('cas-del-wrong');

        // CAS with non-null expected on a deleted key should fail
        $result = $this->testClient->compareAndSwap('cas-del-wrong', 'value', 'new');

        $this->assertFalse($result->swapped, 'CAS with non-null expected should fail on deleted key');
        $this->assertNull($result->previousValue, 'Previous value should be null for deleted key');
    }

    public function testCasReturnsPreviousValueOnFailure(): void
    {
        $this->putOneAndTrack('cas-prev', 'current-value');

        $result = $this->testClient->compareAndSwap('cas-prev', 'wrong-expected', 'new-value');

        $this->assertFalse($result->swapped);
        $this->assertEquals(
            'current-value',
            $result->previousValue,
            'Failed CAS should return the actual current value for retry logic'
        );
    }

    public function testCasAtomicCounter(): void
    {
        // Simulate an atomic counter using CAS
        $key = 'cas-counter';
        $this->putOneAndTrack($key, '0');

        // Increment: read-compare-swap loop
        $current = $this->testClient->get($key);
        $newVal = (string)((int)$current + 1);

        $result = $this->testClient->compareAndSwap($key, $current, $newVal);
        $this->assertTrue($result->swapped);
        $this->assertEquals('1', $this->testClient->get($key));

        // Increment again
        $current = $this->testClient->get($key);
        $newVal = (string)((int)$current + 1);

        $result = $this->testClient->compareAndSwap($key, $current, $newVal);
        $this->assertTrue($result->swapped);
        $this->assertEquals('2', $this->testClient->get($key));
    }

    public function testCasWithLargeValue(): void
    {
        $key = 'cas-large';
        $oldValue = str_repeat('A', 1024 * 100); // 100KB
        $newValue = str_repeat('B', 1024 * 100); // 100KB

        $this->testClient->put($key, $oldValue);
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->compareAndSwap($key, $oldValue, $newValue);

        $this->assertTrue($result->swapped);
        $this->assertEquals($newValue, $this->testClient->get($key));
    }

    // ========================================================================
    // PutIfAbsent
    // ========================================================================

    public function testPutIfAbsentNewKey(): void
    {
        $key = 'pia-new-' . uniqid();
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->putIfAbsent($key, 'first-value');

        $this->assertNull($result, 'putIfAbsent should return null when key was successfully inserted');
        $this->assertEquals('first-value', $this->testClient->get($key));
    }

    public function testPutIfAbsentExistingKey(): void
    {
        $this->putOneAndTrack('pia-exists', 'existing-value');

        $result = $this->testClient->putIfAbsent('pia-exists', 'new-value');

        $this->assertEquals('existing-value', $result, 'putIfAbsent should return existing value when key exists');

        // Verify the value was NOT changed
        $this->assertEquals('existing-value', $this->testClient->get('pia-exists'));
    }

    public function testPutIfAbsentIdempotent(): void
    {
        $key = 'pia-idem-' . uniqid();
        $this->keysToCleanup[] = $key;

        // First call — inserts
        $r1 = $this->testClient->putIfAbsent($key, 'value');
        $this->assertNull($r1);

        // Second call — returns existing value
        $r2 = $this->testClient->putIfAbsent($key, 'different-value');
        $this->assertEquals('value', $r2);

        // Third call — still returns original value
        $r3 = $this->testClient->putIfAbsent($key, 'yet-another');
        $this->assertEquals('value', $r3);

        // Value should still be the original
        $this->assertEquals('value', $this->testClient->get($key));
    }

    public function testPutIfAbsentWithBinaryData(): void
    {
        $key = "pia-bin-\x01\x02";
        $value = "\x00\xff\xfe\xfd";
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->putIfAbsent($key, $value);

        $this->assertNull($result);
        $this->assertEquals($value, $this->testClient->get($key));
    }

    public function testPutIfAbsentWithEmptyValue(): void
    {
        $key = 'pia-empty-' . uniqid();
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->putIfAbsent($key, '');

        $this->assertNull($result);
        $this->assertEquals('', $this->testClient->get($key));
    }

    public function testPutIfAbsentWithTtl(): void
    {
        $key = 'pia-ttl-' . uniqid();
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->putIfAbsent($key, 'temp-value', 60);

        $this->assertNull($result);
        $this->assertEquals('temp-value', $this->testClient->get($key));

        $ttl = $this->testClient->getKeyTTL($key);
        $this->assertNotNull($ttl);
        $this->assertGreaterThan(0, $ttl);
        $this->assertLessThanOrEqual(60, $ttl);
    }

    public function testPutIfAbsentWithTtlExpires(): void
    {
        $key = 'pia-ttl-exp-' . uniqid();
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->putIfAbsent($key, 'temporary', 2);
        $this->assertNull($result);

        sleep(3);

        $this->assertNull($this->testClient->get($key), 'putIfAbsent key should expire after TTL');

        // After expiration, putIfAbsent should succeed again
        $result2 = $this->testClient->putIfAbsent($key, 'reinserted');
        $this->assertNull($result2, 'putIfAbsent should succeed after key expires');
        $this->assertEquals('reinserted', $this->testClient->get($key));
    }

    public function testPutIfAbsentAfterDelete(): void
    {
        $key = 'pia-del-' . uniqid();
        $this->keysToCleanup[] = $key;

        // Insert
        $r1 = $this->testClient->putIfAbsent($key, 'first');
        $this->assertNull($r1);

        // Delete
        $this->testClient->delete($key);

        // putIfAbsent should succeed again
        $r2 = $this->testClient->putIfAbsent($key, 'second');
        $this->assertNull($r2);
        $this->assertEquals('second', $this->testClient->get($key));
    }

    public function testPutIfAbsentDoesNotOverwriteExistingWithTtl(): void
    {
        $this->putOneAndTrack('pia-no-ow', 'permanent');

        // Try to putIfAbsent with TTL — should fail because key exists
        $result = $this->testClient->putIfAbsent('pia-no-ow', 'temp', 60);

        $this->assertEquals('permanent', $result);

        // Original key should have no TTL (it was put without one)
        $ttl = $this->testClient->getKeyTTL('pia-no-ow');
        $this->assertNull($ttl, 'Failed putIfAbsent should not modify existing key TTL');
    }

    public function testPutIfAbsentWithLargeValue(): void
    {
        $key = 'pia-large-' . uniqid();
        $value = str_repeat('X', 1024 * 100); // 100KB
        $this->keysToCleanup[] = $key;

        $result = $this->testClient->putIfAbsent($key, $value);

        $this->assertNull($result);
        $this->assertEquals($value, $this->testClient->get($key));
    }

    // ========================================================================
    // Checksum
    // ========================================================================

    public function testChecksumEmptyRange(): void
    {
        $result = $this->testClient->checksum('chk-empty-zzz-', 'chk-empty-zzz.');

        $this->assertInstanceOf(ChecksumResult::class, $result);
        $this->assertEquals(0, $result->checksum);
        $this->assertEquals(0, $result->totalKvs);
        $this->assertEquals(0, $result->totalBytes);
    }

    public function testChecksumSingleKey(): void
    {
        $this->putOneAndTrack('chk-single', 'value');

        $result = $this->testClient->checksum('chk-single', "chk-single\x00");

        $this->assertInstanceOf(ChecksumResult::class, $result);
        $this->assertNotEquals(0, $result->checksum, 'Checksum of non-empty range should be non-zero');
        $this->assertEquals(1, $result->totalKvs);
        $this->assertGreaterThan(0, $result->totalBytes);
    }

    public function testChecksumMultipleKeys(): void
    {
        $pairs = [
            'chk-multi-a' => 'value-a',
            'chk-multi-b' => 'value-b',
            'chk-multi-c' => 'value-c',
        ];
        $this->putAndTrack($pairs);

        $result = $this->testClient->checksum('chk-multi-', 'chk-multi.');

        $this->assertEquals(3, $result->totalKvs);
        $this->assertGreaterThan(0, $result->totalBytes);
        $this->assertNotEquals(0, $result->checksum);
    }

    public function testChecksumChangesWhenDataChanges(): void
    {
        $this->putOneAndTrack('chk-change', 'original');

        $before = $this->testClient->checksum('chk-change', "chk-change\x00");

        // Overwrite with different value
        $this->testClient->put('chk-change', 'modified');

        $after = $this->testClient->checksum('chk-change', "chk-change\x00");

        $this->assertNotEquals(
            $before->checksum,
            $after->checksum,
            'Checksum should change when data changes'
        );
        $this->assertEquals(
            $before->totalKvs,
            $after->totalKvs,
            'Key count should remain the same'
        );
    }

    public function testChecksumChangesWhenKeyAdded(): void
    {
        $this->putOneAndTrack('chk-add-a', 'va');

        $before = $this->testClient->checksum('chk-add-', 'chk-add.');
        $this->assertEquals(1, $before->totalKvs);

        $this->putOneAndTrack('chk-add-b', 'vb');

        $after = $this->testClient->checksum('chk-add-', 'chk-add.');
        $this->assertEquals(2, $after->totalKvs);
        $this->assertGreaterThan($before->totalBytes, $after->totalBytes);
    }

    public function testChecksumChangesWhenKeyDeleted(): void
    {
        $this->putOneAndTrack('chk-del-a', 'va');
        $this->putOneAndTrack('chk-del-b', 'vb');

        $before = $this->testClient->checksum('chk-del-', 'chk-del.');
        $this->assertEquals(2, $before->totalKvs);

        $this->testClient->delete('chk-del-b');

        $after = $this->testClient->checksum('chk-del-', 'chk-del.');
        $this->assertEquals(1, $after->totalKvs);
        $this->assertLessThan($before->totalBytes, $after->totalBytes);
    }

    public function testChecksumDeterministic(): void
    {
        $this->putOneAndTrack('chk-det-a', 'va');
        $this->putOneAndTrack('chk-det-b', 'vb');

        $first = $this->testClient->checksum('chk-det-', 'chk-det.');
        $second = $this->testClient->checksum('chk-det-', 'chk-det.');

        $this->assertEquals(
            $first->checksum,
            $second->checksum,
            'Checksum should be deterministic for the same data'
        );
        $this->assertEquals($first->totalKvs, $second->totalKvs);
        $this->assertEquals($first->totalBytes, $second->totalBytes);
    }

    public function testChecksumWithBinaryKeys(): void
    {
        $k1 = "chk-bin-\x01";
        $k2 = "chk-bin-\x02";

        $this->testClient->put($k1, 'v1');
        $this->testClient->put($k2, 'v2');
        $this->keysToCleanup[] = $k1;
        $this->keysToCleanup[] = $k2;

        $result = $this->testClient->checksum("chk-bin-\x00", "chk-bin-\x03");

        $this->assertEquals(2, $result->totalKvs);
        $this->assertNotEquals(0, $result->checksum);
    }

    public function testChecksumPartialRange(): void
    {
        $pairs = [
            'chk-part-a' => 'va',
            'chk-part-b' => 'vb',
            'chk-part-c' => 'vc',
        ];
        $this->putAndTrack($pairs);

        // Checksum only first two keys
        $partial = $this->testClient->checksum('chk-part-a', 'chk-part-c');
        $this->assertEquals(2, $partial->totalKvs);

        // Checksum all three
        $full = $this->testClient->checksum('chk-part-', 'chk-part.');
        $this->assertEquals(3, $full->totalKvs);

        $this->assertNotEquals($partial->checksum, $full->checksum);
    }

    public function testChecksumManyKeys(): void
    {
        $pairs = [];
        for ($i = 0; $i < 50; $i++) {
            $pairs[sprintf('chk-many-%03d', $i)] = "value-$i";
        }
        $this->putAndTrack($pairs);

        $result = $this->testClient->checksum('chk-many-', 'chk-many.');

        $this->assertEquals(50, $result->totalKvs);
        $this->assertNotEquals(0, $result->checksum);
    }

    public function testChecksumTotalBytesIsAccurate(): void
    {
        $key = 'chk-bytes';
        $value = 'hello';
        $this->putOneAndTrack($key, $value);

        $result = $this->testClient->checksum($key, "$key\x00");

        $this->assertEquals(1, $result->totalKvs);
        // Total bytes should be at least key length + value length
        $this->assertGreaterThanOrEqual(
            strlen($key) + strlen($value),
            $result->totalBytes,
            'Total bytes should include at least key + value lengths'
        );
    }

    // ========================================================================
    // BatchScan
    // ========================================================================

    public function testBatchScanBasic(): void
    {
        $this->putAndTrack([
            'bs-users-alice' => 'Alice',
            'bs-users-bob' => 'Bob',
            'bs-orders-001' => 'Order1',
            'bs-orders-002' => 'Order2',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-users-', 'bs-users.'],
            ['bs-orders-', 'bs-orders.'],
        ], 100);

        $this->assertCount(2, $results, 'Should return one result set per range');

        // First range: users
        $this->assertCount(2, $results[0]);
        $userKeys = array_column($results[0], 'key');
        $this->assertContains('bs-users-alice', $userKeys);
        $this->assertContains('bs-users-bob', $userKeys);

        // Second range: orders
        $this->assertCount(2, $results[1]);
        $orderKeys = array_column($results[1], 'key');
        $this->assertContains('bs-orders-001', $orderKeys);
        $this->assertContains('bs-orders-002', $orderKeys);
    }

    public function testBatchScanWithLimit(): void
    {
        $this->putAndTrack([
            'bs-lim-a1' => 'v1',
            'bs-lim-a2' => 'v2',
            'bs-lim-a3' => 'v3',
            'bs-lim-b1' => 'v4',
            'bs-lim-b2' => 'v5',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-lim-a', 'bs-lim-b'],
            ['bs-lim-b', 'bs-lim-c'],
        ], 2);

        // Each range should be limited to 2 results
        $this->assertCount(2, $results[0], 'First range should have at most 2 results');
        $this->assertCount(2, $results[1], 'Second range should have at most 2 results');
    }

    public function testBatchScanKeyOnly(): void
    {
        $this->putAndTrack([
            'bs-ko-a' => 'secret-a',
            'bs-ko-b' => 'secret-b',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-ko-', 'bs-ko.'],
        ], 100, true);

        $this->assertCount(1, $results);
        $this->assertCount(2, $results[0]);
        foreach ($results[0] as $pair) {
            $this->assertNull($pair['value'], 'keyOnly batchScan should return null values');
        }
    }

    public function testBatchScanEmptyRanges(): void
    {
        $results = $this->testClient->batchScan([
            ['bs-empty-zzz-', 'bs-empty-zzz.'],
            ['bs-empty-yyy-', 'bs-empty-yyy.'],
        ], 100);

        $this->assertCount(2, $results);
        $this->assertCount(0, $results[0]);
        $this->assertCount(0, $results[1]);
    }

    public function testBatchScanEmptyInput(): void
    {
        $results = $this->testClient->batchScan([], 100);

        $this->assertEquals([], $results);
    }

    public function testBatchScanSingleRange(): void
    {
        $this->putAndTrack([
            'bs-single-a' => 'va',
            'bs-single-b' => 'vb',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-single-', 'bs-single.'],
        ], 100);

        $this->assertCount(1, $results);
        $this->assertCount(2, $results[0]);
    }

    public function testBatchScanMixedPopulatedAndEmpty(): void
    {
        $this->putAndTrack([
            'bs-mix-a' => 'va',
            'bs-mix-b' => 'vb',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-mix-', 'bs-mix.'],       // has data
            ['bs-mix-zzz-', 'bs-mix-zzz.'], // empty
        ], 100);

        $this->assertCount(2, $results);
        $this->assertCount(2, $results[0]);
        $this->assertCount(0, $results[1]);
    }

    public function testBatchScanWithBinaryKeys(): void
    {
        $k1 = "bs-bin-\x01";
        $k2 = "bs-bin-\x02";

        $this->testClient->put($k1, 'v1');
        $this->testClient->put($k2, 'v2');
        $this->keysToCleanup[] = $k1;
        $this->keysToCleanup[] = $k2;

        $results = $this->testClient->batchScan([
            ["bs-bin-\x00", "bs-bin-\x03"],
        ], 100);

        $this->assertCount(1, $results);
        $this->assertCount(2, $results[0]);
        $this->assertEquals($k1, $results[0][0]['key']);
        $this->assertEquals($k2, $results[0][1]['key']);
    }

    public function testBatchScanResultsInAscendingOrder(): void
    {
        $this->putOneAndTrack('bs-ord-c', 'vc');
        $this->putOneAndTrack('bs-ord-a', 'va');
        $this->putOneAndTrack('bs-ord-b', 'vb');

        $results = $this->testClient->batchScan([
            ['bs-ord-', 'bs-ord.'],
        ], 100);

        $keys = array_column($results[0], 'key');
        $this->assertEquals(['bs-ord-a', 'bs-ord-b', 'bs-ord-c'], $keys);
    }

    public function testBatchScanManyRanges(): void
    {
        // Create 5 different prefixes with 3 keys each
        for ($i = 0; $i < 5; $i++) {
            for ($j = 0; $j < 3; $j++) {
                $key = sprintf('bs-many-p%d-k%d', $i, $j);
                $this->putOneAndTrack($key, "v{$i}{$j}");
            }
        }

        $ranges = [];
        for ($i = 0; $i < 5; $i++) {
            $ranges[] = [sprintf('bs-many-p%d-', $i), sprintf('bs-many-p%d.', $i)];
        }

        $results = $this->testClient->batchScan($ranges, 100);

        $this->assertCount(5, $results);
        foreach ($results as $rangeResult) {
            $this->assertCount(3, $rangeResult);
        }
    }

    public function testBatchScanLimitOnePerRange(): void
    {
        $this->putAndTrack([
            'bs-l1-a1' => 'v1',
            'bs-l1-a2' => 'v2',
            'bs-l1-b1' => 'v3',
            'bs-l1-b2' => 'v4',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-l1-a', 'bs-l1-b'],
            ['bs-l1-b', 'bs-l1-c'],
        ], 1);

        $this->assertCount(1, $results[0], 'Limit 1 should return exactly 1 result per range');
        $this->assertCount(1, $results[1]);
        $this->assertEquals('bs-l1-a1', $results[0][0]['key']);
        $this->assertEquals('bs-l1-b1', $results[1][0]['key']);
    }

    public function testBatchScanValuesAreCorrect(): void
    {
        $this->putAndTrack([
            'bs-val-a' => 'alpha',
            'bs-val-b' => 'bravo',
        ]);

        $results = $this->testClient->batchScan([
            ['bs-val-', 'bs-val.'],
        ], 100);

        $resultMap = [];
        foreach ($results[0] as $pair) {
            $resultMap[$pair['key']] = $pair['value'];
        }

        $this->assertEquals('alpha', $resultMap['bs-val-a']);
        $this->assertEquals('bravo', $resultMap['bs-val-b']);
    }

    // ========================================================================
    // Configurable timeouts (Issue #30)
    // ========================================================================

    public function testDefaultTimeoutConfigOperationsSucceed(): void
    {
        $key = 'to-default-' . uniqid();
        $this->putOneAndTrack($key, 'value');

        $this->assertEquals('value', $this->testClient->get($key));
        $this->assertNull($this->testClient->get('to-default-nonexistent-' . uniqid()));
    }

    public function testCustomTimeoutConfigReadSucceeds(): void
    {
        $client = RawKvClient::create(
            getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'],
            options: [
                'insecure' => true,
                'timeout' => [
                    'readTimeoutMs' => 15000,
                    'writeTimeoutMs' => 15000,
                ],
            ],
        );

        $key = 'to-custom-read-' . uniqid();
        $client->put($key, 'hello');
        $this->assertEquals('hello', $client->get($key));

        $client->delete($key);
        $client->close();
    }

    public function testCustomScanTimeoutSucceeds(): void
    {
        $client = RawKvClient::create(
            getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'],
            options: [
                'insecure' => true,
                'timeout' => [
                    'scanTimeoutMs' => 30000,
                    'writeTimeoutMs' => 15000,
                ],
            ],
        );

        $prefix = 'to-scan-timeout-' . uniqid() . '-';
        $keys = [
            $prefix . 'a' => '1',
            $prefix . 'b' => '2',
            $prefix . 'c' => '3',
        ];
        $client->batchPut($keys);

        $results = $client->scan($prefix, $prefix . 'z', 10);
        $this->assertCount(3, $results);

        foreach (array_keys($keys) as $key) {
            $client->delete($key);
        }
        $client->close();
    }

    public function testCustomBatchTimeoutSucceeds(): void
    {
        $client = RawKvClient::create(
            getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'],
            options: [
                'insecure' => true,
                'timeout' => [
                    'batchReadTimeoutMs' => 30000,
                    'batchWriteTimeoutMs' => 30000,
                ],
            ],
        );

        $prefix = 'to-batch-' . uniqid() . '-';
        $keys = [
            $prefix . 'x' => '10',
            $prefix . 'y' => '20',
        ];
        $client->batchPut($keys);

        $result = $client->batchGet(array_keys($keys));
        $this->assertCount(2, $result);
        $this->assertEquals('10', $result[$prefix . 'x']);

        $client->batchDelete(array_keys($keys));
        $client->close();
    }

    public function testCustomDeleteRangeTimeoutSucceeds(): void
    {
        $client = RawKvClient::create(
            getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'],
            options: [
                'insecure' => true,
                'timeout' => [
                    'deleteRangeTimeoutMs' => 60000,
                    'writeTimeoutMs' => 15000,
                ],
            ],
        );

        $prefix = 'to-deleterange-' . uniqid() . '-';
        $client->put($prefix . 'a', '1');
        $client->put($prefix . 'b', '2');

        $client->deleteRange($prefix, $prefix . 'z');
        $result = $client->scan($prefix, $prefix . 'z', 10);
        $this->assertCount(0, $result);

        $client->close();
    }

    public function testMixedTimeoutConfig(): void
    {
        $client = RawKvClient::create(
            getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'],
            options: [
                'insecure' => true,
                'timeout' => [
                    'readTimeoutMs' => 5000,
                    'writeTimeoutMs' => 5000,
                    'batchReadTimeoutMs' => 10000,
                    'batchWriteTimeoutMs' => 10000,
                    'scanTimeoutMs' => 20000,
                    'deleteRangeTimeoutMs' => 30000,
                ],
            ],
        );

        $key = 'to-mixed-' . uniqid();
        $client->put($key, 'test');
        $this->assertEquals('test', $client->get($key));
        $client->delete($key);
        $client->close();
    }

    // ========================================================================
    // Batch auto-splitting (by count and byte size)
    // ========================================================================

    public function testBatchPutSplitByCount(): void
    {
        // Generate enough keys to exceed MAX_BATCH_LIMIT (512)
        $pairs = [];
        for ($i = 0; $i < 600; $i++) {
            $key = sprintf('split-cnt-%03d-%s', $i, uniqid());
            $pairs[$key] = 'v' . $i;
        }
        $this->putAndTrack($pairs);

        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertCount(600, $results);
        foreach ($pairs as $key => $value) {
            $this->assertArrayHasKey($key, $results);
            $this->assertSame($value, $results[$key]);
        }
    }

    public function testBatchGetSplitByCountReturnsOrdered(): void
    {
        // Put keys first
        $pairs = [];
        for ($i = 0; $i < 600; $i++) {
            $key = sprintf('bg-order-%03d-%s', $i, uniqid());
            $pairs[$key] = 'val-' . $i;
        }
        $this->putAndTrack($pairs);

        // Request in specific (non-sequential) order
        $keys = array_keys($pairs);
        $reversed = array_reverse($keys);
        $results = $this->testClient->batchGet($reversed);

        $this->assertCount(600, $results);

        // Verify input order is preserved in results
        $expected = [];
        foreach ($reversed as $k) {
            $expected[$k] = $pairs[$k];
        }
        $this->assertSame($expected, $results);
    }

    public function testBatchPutSplitBySize(): void
    {
        // Generate pairs with large keys/values so total exceeds MAX_BATCH_PUT_SIZE (16KB)
        // Each key is ~100 bytes, each value is ~200 bytes => ~300 bytes per pair
        // 60 pairs => ~18KB, forcing at least one split
        $pairs = [];
        for ($i = 0; $i < 60; $i++) {
            $key = str_repeat('k', 100) . $i . '-' . uniqid();
            $pairs[$key] = str_repeat('v', 200) . $i;
        }
        $this->putAndTrack($pairs);

        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertCount(60, $results);
        foreach ($pairs as $key => $value) {
            $this->assertArrayHasKey($key, $results);
            $this->assertSame($value, $results[$key]);
        }
    }

    public function testBatchGetSplitBySizeCorrectResults(): void
    {
        // Create keys where the keys themselves are large to exceed MAX_BATCH_GET_SIZE (16KB)
        // Each key is ~300 bytes, 60 keys => ~18KB total, forcing split
        $pairs = [];
        for ($i = 0; $i < 60; $i++) {
            $key = str_repeat('k', 300) . 'bg-size-' . $i . '-' . uniqid();
            $pairs[$key] = 'vs-' . $i;
        }
        $this->putAndTrack($pairs);

        // batchGet with same keys should return all values correctly
        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertCount(60, $results);
        foreach ($pairs as $key => $value) {
            $this->assertSame($value, $results[$key]);
        }
    }

    public function testBatchDeleteSplitByCount(): void
    {
        // Put many keys, then delete them all in one batchDelete call
        $pairs = [];
        for ($i = 0; $i < 600; $i++) {
            $key = sprintf('del-cnt-%03d-%s', $i, uniqid());
            $pairs[$key] = 'd' . $i;
        }
        $this->putAndTrack($pairs);

        // Verify they exist
        $this->assertCount(600, $this->testClient->batchGet(array_keys($pairs)));

        // Delete all at once (this will split into sub-batches)
        $this->testClient->batchDelete(array_keys($pairs));

        // Verify all deleted
        $results = $this->testClient->batchGet(array_keys($pairs));
        foreach (array_keys($pairs) as $key) {
            $this->assertNull($results[$key], "Key '$key' should be null after batchDelete");
        }

        // Remove from cleanup since they're already deleted
        $this->keysToCleanup = array_diff($this->keysToCleanup, array_keys($pairs));
    }

    public function testBatchPutSplitWithPerKeyTtl(): void
    {
        // Per-key TTL with enough keys to force splitting
        $pairs = [];
        $ttls = [];
        for ($i = 0; $i < 600; $i++) {
            $key = sprintf('ttl-split-%03d-%s', $i, uniqid());
            $pairs[$key] = 't' . $i;
            $ttls[$key] = $i % 2 === 0 ? 60 : 120;
        }
        $this->testClient->batchPut($pairs, $ttls);
        foreach (array_keys($pairs) as $key) {
            $this->keysToCleanup[] = $key;
        }

        // Verify all written correctly
        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertCount(600, $results);
        foreach ($pairs as $key => $value) {
            $this->assertSame($value, $results[$key]);
        }

        // Verify TTLs were set (keys should still exist)
        foreach (array_slice(array_keys($pairs), 0, 10) as $key) {
            $ttl = $this->testClient->getKeyTTL($key);
            $this->assertNotNull($ttl, "TTL should be set for key '$key'");
        }
    }

    public function testBatchFullCycleWithSplitting(): void
    {
        // Full write-read-delete cycle with enough data to cause splitting
        $pairs = [];
        for ($i = 0; $i < 600; $i++) {
            $key = sprintf('cycle-%03d-%s', $i, uniqid());
            $pairs[$key] = str_repeat('x', 50) . $i;
        }
        $this->putAndTrack($pairs);

        // Read all
        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertSame($pairs, $results);

        // Delete all
        $this->testClient->batchDelete(array_keys($pairs));

        // Verify gone
        $afterDelete = $this->testClient->batchGet(array_keys($pairs));
        foreach (array_keys($pairs) as $key) {
            $this->assertNull($afterDelete[$key]);
        }

        $this->keysToCleanup = array_diff($this->keysToCleanup, array_keys($pairs));
    }

    public function testBatchPutSplitWithLargeValues(): void
    {
        // Each value is ~1KB, 100 pairs => ~100KB total, well over 16KB limit
        $pairs = [];
        for ($i = 0; $i < 100; $i++) {
            $key = sprintf('big-val-%03d-%s', $i, uniqid());
            $pairs[$key] = str_repeat('v', 1000) . $i;
        }
        $this->putAndTrack($pairs);

        // Verify all written by reading each individually
        foreach ($pairs as $key => $expected) {
            $actual = $this->testClient->get($key);
            $this->assertSame($expected, $actual, "Mismatch for key '$key'");
        }
    }

    public function testBatchOperationsAcrossManyRegionsWithSplitting(): void
    {
        // Keys with different prefixes should land in different regions
        // This tests both region-level parallelism AND sub-batch splitting
        $pairs = [];
        $prefixes = ['a-', 'b-', 'c-', 'd-', 'e-', 'f-', 'g-', 'h-', 'i-', 'j-'];
        foreach ($prefixes as $prefix) {
            for ($i = 0; $i < 100; $i++) {
                $key = $prefix . sprintf('rgn-splt-%03d-%s', $i, uniqid());
                $pairs[$key] = 'r' . $i;
            }
        }
        $this->putAndTrack($pairs);

        $this->assertCount(1000, $pairs);

        // batchGet all
        $results = $this->testClient->batchGet(array_keys($pairs));
        $this->assertSame($pairs, $results);

        // batchDelete all
        $this->testClient->batchDelete(array_keys($pairs));

        $afterDelete = $this->testClient->batchGet(array_keys($pairs));
        foreach (array_keys($pairs) as $key) {
            $this->assertNull($afterDelete[$key]);
        }

        $this->keysToCleanup = array_diff($this->keysToCleanup, array_keys($pairs));
    }

    // ========================================================================
    // Client lifecycle (close behavior)
    // ========================================================================

    private function createFreshClient(): RawKvClient
    {
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        return RawKvClient::create($pdEndpoints, options: ['insecure' => true]);
    }

    public function testCloseThenGetThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->get('any-key');
    }

    public function testCloseThenPutThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->put('any-key', 'any-value');
    }

    public function testCloseThenDeleteThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->delete('any-key');
    }

    public function testCloseThenDeleteRangeThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->deleteRange('a', 'z');
    }

    public function testCloseThenScanThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->scan('a', 'z');
    }

    public function testCloseThenReverseScanThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->reverseScan('a', 'z');
    }

    public function testCloseThenBatchGetThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->batchGet(['key1', 'key2']);
    }

    public function testCloseThenBatchPutThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->batchPut(['key' => 'value']);
    }

    public function testCloseThenBatchDeleteThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->batchDelete(['key1', 'key2']);
    }

    public function testCloseThenBatchScanThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->batchScan([['a', 'z']], 10);
    }

    public function testCloseThenChecksumThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->checksum('a', 'z');
    }

    public function testCloseThenGetKeyTTLThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->getKeyTTL('any-key');
    }

    public function testCloseThenCompareAndSwapThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->compareAndSwap('any-key', 'old', 'new');
    }

    public function testCloseThenPutIfAbsentThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->putIfAbsent('any-key', 'value');
    }

    public function testCloseThenScanIteratorThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->scanIterator('a', 'z', 10);
    }

    public function testCloseThenScanPrefixThrowsClientClosedException(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        $this->expectException(ClientClosedException::class);
        $client->scanPrefix('prefix');
    }

    public function testCloseIsIdempotent(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        // Second close must not throw
        $client->close();

        // Must remain closed (still throw)
        $this->expectException(ClientClosedException::class);
        $client->get('any-key');
    }

    public function testMultiplePostCloseOperationsAllThrow(): void
    {
        $client = $this->createFreshClient();
        $client->close();

        for ($i = 0; $i < 5; $i++) {
            try {
                $client->get('key-' . $i);
                $this->fail('Expected ClientClosedException on iteration ' . $i);
            } catch (ClientClosedException) {
                // Expected
            }
        }
    }
}
