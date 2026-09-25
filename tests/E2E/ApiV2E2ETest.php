<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * API V2 smoke coverage. Run with docker-compose.apiv2.yml; it is skipped in
 * the normal V1 E2E jobs.
 *
 * scripts/test-e2e-apiv2.sh also sets TIKV_ALT_KEYSPACE to a second keyspace
 * so the suite can prove that identical user keys stay isolated per keyspace.
 */
final class ApiV2E2ETest extends TestCase
{
    private static ?RawKvClient $raw = null;
    private static ?TxnKvClient $txn = null;
    private static ?string $altKeyspace = null;

    public static function setUpBeforeClass(): void
    {
        if (getenv('TIKV_API_VERSION') !== '2') {
            return;
        }

        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'];
        $options = [
            'apiVersion' => 2,
            'keyspace' => getenv('TIKV_KEYSPACE') ?: 'DEFAULT',
        ];
        self::$raw = RawKvClient::create($pdEndpoints, options: $options);
        self::$txn = TxnKvClient::create($pdEndpoints, options: $options);
        self::$altKeyspace = (getenv('TIKV_ALT_KEYSPACE') ?: '') ?: null;
    }

    public static function tearDownAfterClass(): void
    {
        self::$raw?->close();
        self::$txn?->close();
        self::$raw = null;
        self::$txn = null;
        self::$altKeyspace = null;
    }

    protected function setUp(): void
    {
        if (
            getenv('TIKV_API_VERSION') !== '2'
            || !self::$raw instanceof RawKvClient
            || !self::$txn instanceof TxnKvClient
        ) {
            $this->markTestSkipped('API V2 TiKV cluster is not enabled');
        }
    }

    public function testRawKvRoundTripUsesDefaultColumnFamily(): void
    {
        $raw = self::$raw;
        $this->assertInstanceOf(RawKvClient::class, $raw);
        $key = 'php-api-v2-raw-' . uniqid();
        $raw->setColumnFamily('not-default');
        $raw->put($key, 'raw-value');

        $this->assertSame('raw-value', $raw->get($key));
        $raw->delete($key);
    }

    public function testRawScanStaysInsideTheKeyspace(): void
    {
        $raw = self::$raw;
        $this->assertInstanceOf(RawKvClient::class, $raw);
        $prefix = 'php-api-v2-scan-' . uniqid();
        $keys = [$prefix . '-a', $prefix . '-b', $prefix . '-c'];
        foreach ($keys as $key) {
            $raw->put($key, 'value-' . substr($key, -1));
        }

        // An unbounded upper limit has to become the keyspace sentinel on the
        // wire, otherwise TiKV would scan past the keyspace's last region.
        $this->assertSame($keys, array_column($raw->scan($prefix, '', 10, true), 'key'));
        // The half-open upper bound is exclusive, and it has to keep the same
        // keyspace prefix on both ends.
        $this->assertSame(
            [$prefix . '-a'],
            array_column($raw->scan($prefix, $prefix . '-b', 10, true), 'key'),
        );

        $raw->deleteRange($prefix, '');
        $this->assertSame([], $raw->scan($prefix, '', 10, true));
    }

    public function testKeysAreIsolatedPerKeyspace(): void
    {
        $raw = self::$raw;
        $this->assertInstanceOf(RawKvClient::class, $raw);
        $altKeyspace = self::$altKeyspace;
        if ($altKeyspace === null) {
            $this->markTestSkipped('TIKV_ALT_KEYSPACE is not set');
        }

        $key = 'php-api-v2-keyspace-' . uniqid();
        $alt = RawKvClient::create(
            getenv('PD_ENDPOINTS') ? explode(',', (string) getenv('PD_ENDPOINTS')) : ['pd:2379'],
            options: ['apiVersion' => 2, 'keyspace' => $altKeyspace],
        );
        try {
            $alt->put($key, 'alt-value');
            // Same user key, different keyspace ID: it must not resolve.
            $this->assertNull($raw->get($key));
            $this->assertSame('alt-value', $alt->get($key));
        } finally {
            $alt->delete($key);
            $alt->close();
        }
    }

    public function testRawAndTxnKeysAreIsolatedByModePrefix(): void
    {
        $raw = self::$raw;
        $txnClient = self::$txn;
        $this->assertInstanceOf(RawKvClient::class, $raw);
        $this->assertInstanceOf(TxnKvClient::class, $txnClient);
        $rawKey = 'php-api-v2-shared-' . uniqid();
        $txnKey = $rawKey . '-txn';
        $raw->put($rawKey, 'raw');
        $txn = $txnClient->begin();
        $txn->set($txnKey, 'txn');
        $txn->commit();

        $this->assertSame('raw', $raw->get($rawKey));
        $this->assertNull($raw->get($txnKey));
        $raw->delete($rawKey);

        $cleanup = $txnClient->begin();
        $cleanup->delete($txnKey);
        $cleanup->commit();
    }
}
