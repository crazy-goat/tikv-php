<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * End-to-end acceptance for resource control (issue #427, GAP-13): a cluster
 * with a configured resource group answers RawKV traffic that names it.
 *
 * The group is created through PD's resource-manager HTTP API (see
 * ResourceGroupAdmin), used by the client for a put/get/delete workload, and
 * read back afterwards to prove that the group exists and was not rejected.
 *
 * LIMITATION: RU consumption cannot be asserted here. TiKV uses the request
 * priority from `Context.resource_control_context` and reports RU
 * consumption to PD only through a resource-manager client (TiDB); a native
 * TiKV client is not one, so PD's `ru_consumption` stays empty even though
 * the requests carry the group. This is recorded in
 * `docs/helpers/decisions.md` ("Resource-control E2E observation limit").
 * The wire-level group/tag attribution is pinned by
 * `tests/Unit/RawKv/ResourceControlTest.php` and the TxnKV tests.
 */
final class ResourceControlRawKvE2ETest extends TestCase
{
    private static ?RawKvClient $client = null;

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

        self::$groupName = 'php-e2e-raw-' . bin2hex(random_bytes(4));
        ResourceGroupAdmin::create(self::$pdEndpoints, self::$groupName);

        self::$client = RawKvClient::create(self::$pdEndpoints, options: [
            'resourceGroup' => self::$groupName,
            'resourceGroupTag' => 'e2e-raw',
        ]);
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
        if (!self::$client instanceof RawKvClient) {
            return;
        }

        foreach ($this->keysToCleanup as $key) {
            try {
                self::$client->delete($key);
            } catch (\Throwable) {
                // Cleanup must not mask the test result.
            }
        }
        $this->keysToCleanup = [];
    }

    public function testGroupedRawKvTrafficSucceedsAndTheGroupExists(): void
    {
        $client = self::$client;
        self::assertInstanceOf(RawKvClient::class, $client);

        $key = 'e2e:resource-control:raw:' . bin2hex(random_bytes(8));
        $this->keysToCleanup[] = $key;

        $client->put($key, 'grouped-value');
        self::assertSame('grouped-value', $client->get($key));

        $group = ResourceGroupAdmin::get(self::$pdEndpoints, self::$groupName);
        self::assertSame(self::$groupName, $group['name'] ?? null);
        self::assertSame(1, $group['mode'] ?? null, 'the RU-mode group was replaced or removed');

        // The group is still usable after traffic; a rejected/unknown group
        // would not be the one configured by this test.
        $client->delete($key);
        $this->keysToCleanup = [];
        self::assertNull($client->get($key));
    }
}
