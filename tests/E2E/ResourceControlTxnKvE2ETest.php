<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * End-to-end acceptance for resource control (issue #427, GAP-13): a cluster
 * with a configured resource group answers TxnKV traffic that names it.
 *
 * The group is created through PD's resource-manager HTTP API (see
 * ResourceGroupAdmin) and passed as a per-transaction `begin()` option, so
 * this also exercises the per-transaction override path on the wire.
 *
 * LIMITATION: RU consumption cannot be asserted here. TiKV uses the request
 * priority from `Context.resource_control_context` and reports RU
 * consumption to PD only through a resource-manager client (TiDB); a native
 * TiKV client is not one, so PD's `ru_consumption` stays empty even though
 * the requests carry the group. This is recorded in
 * `docs/helpers/decisions.md` ("Resource-control E2E observation limit").
 * The wire-level group/tag attribution is pinned by
 * `tests/Unit/TxnKv/TransactionTest.php`, `TxnKvClientTest.php` and
 * `tests/Unit/RawKv/ResourceControlTest.php`.
 */
final class ResourceControlTxnKvE2ETest extends TestCase
{
    private static ?TxnKvClient $client = null;

    /** @var list<string> */
    private static array $pdEndpoints = ['pd:2379'];

    private static string $groupName = '';

    /** @var list<string> */
    private array $keysToCleanup = [];

    public static function setUpBeforeClass(): void
    {
        $env = getenv('PD_ENDPOINTS');
        if ($env !== false && $env !== '') {
            self::$pdEndpoints = explode(',', (string) $env);
        }

        ClusterProbe::assertPdReachable(self::$pdEndpoints);

        if (!ResourceGroupAdmin::isAvailable(self::$pdEndpoints)) {
            throw new RuntimeException(
                'PD does not expose the resource-manager API; the E2E lane '
                . 'needs PD v8.5.5 (docker-compose.yml)',
            );
        }

        self::$groupName = 'php-e2e-txn-' . bin2hex(random_bytes(4));
        ResourceGroupAdmin::create(self::$pdEndpoints, self::$groupName);

        self::$client = TxnKvClient::create(self::$pdEndpoints);
    }

    public static function tearDownAfterClass(): void
    {
        self::$client?->close();
        self::$client = null;

        if (self::$groupName !== '') {
            ResourceGroupAdmin::delete(self::$pdEndpoints, self::$groupName);
            self::$groupName = '';
        }
    }

    protected function tearDown(): void
    {
        if (!self::$client instanceof TxnKvClient) {
            return;
        }

        foreach ($this->keysToCleanup as $key) {
            try {
                $txn = self::$client->begin(['pessimistic' => false]);
                $txn->delete($key);
                $txn->commit();
            } catch (\Throwable) {
                // Cleanup must not mask the test result.
            }
        }
        $this->keysToCleanup = [];
    }

    public function testGroupedTxnKvTrafficSucceedsAndTheGroupExists(): void
    {
        $client = self::$client;
        self::assertInstanceOf(TxnKvClient::class, $client);

        $key = 'e2e:resource-control:txn:' . bin2hex(random_bytes(8));
        $this->keysToCleanup[] = $key;

        $options = [
            'pessimistic' => false,
            'resourceGroup' => self::$groupName,
            'resourceGroupTag' => 'e2e-txn',
        ];

        $txn = $client->begin($options);
        $txn->set($key, 'grouped-value');
        $txn->commit();

        $txn = $client->begin($options);
        self::assertSame('grouped-value', $txn->get($key));
        $txn->rollback();

        $group = ResourceGroupAdmin::get(self::$pdEndpoints, self::$groupName);
        self::assertSame(self::$groupName, $group['name'] ?? null);
        self::assertSame(1, $group['mode'] ?? null, 'the RU-mode group was replaced or removed');

        $txn = $client->begin($options);
        $txn->delete($key);
        $txn->commit();
        $this->keysToCleanup = [];

        $txn = $client->begin(['pessimistic' => false]);
        self::assertNull($txn->get($key));
        $txn->rollback();
    }
}
