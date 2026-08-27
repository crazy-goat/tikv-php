<?php
/**
 * TiKV PHP Client - TTL (Time-To-Live) Example
 * 
 * Demonstrates storing data with expiration.
 * 
 * NOTE: Requires TiKV to be configured with enable-ttl=true.
 * TTL mode is exclusive with TxnKV — a TTL-enabled cluster cannot serve
 * transactional requests. See README "TTL (Time-To-Live)".
 */

require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', getenv('PD_ENDPOINTS')) : ['127.0.0.1:2379'];
$client = RawKvClient::create($pdEndpoints);

try {
    echo "TiKV PHP Client - TTL Example\n";
    echo "==============================\n\n";
    
    echo "NOTE: This example requires TiKV to be configured with enable-ttl=true\n\n";
    
    // Put with TTL
    echo "1. Storing 'session:123' with 5 second TTL...\n";
    $client->put('session:123', json_encode(['user' => 'alice', 'login_time' => time()]), ttl: 5);
    echo "✓ Stored with 5 second TTL\n\n";
    
    // Check value and TTL
    echo "2. Checking value and TTL immediately...\n";
    $value = $client->get('session:123');
    $ttl = $client->getKeyTTL('session:123');
    echo "   Value: $value\n";
    echo "   TTL remaining: $ttl seconds\n\n";
    
    // Wait and check again
    echo "3. Waiting 3 seconds...\n";
    sleep(3);
    $ttl = $client->getKeyTTL('session:123');
    echo "   TTL remaining after 3s: $ttl seconds\n\n";
    
    // Wait for expiration
    echo "4. Waiting 3 more seconds for expiration...\n";
    sleep(3);
    $value = $client->get('session:123');
    $ttl = $client->getKeyTTL('session:123');
    echo "   Value after expiration: " . ($value ?? 'null') . "\n";
    echo "   TTL after expiration: " . ($ttl ?? 'null') . "\n\n";
    
    // Put without TTL (persistent)
    echo "5. Storing 'config:app' without TTL (persistent)...\n";
    $client->put('config:app', json_encode(['version' => '1.0', 'debug' => false]));
    $ttl = $client->getKeyTTL('config:app');
    echo "   TTL: " . ($ttl === null ? 'null (persistent)' : "$ttl seconds") . "\n\n";
    
    // Batch put with TTL
    echo "6. Batch put with TTL...\n";
    $client->batchPut([
        'cache:item:1' => 'cached-value-1',
        'cache:item:2' => 'cached-value-2',
        'cache:item:3' => 'cached-value-3',
    ], ttl: 10);
    echo "✓ Stored 3 items with 10 second TTL\n\n";
    
    // Verify batch TTL
    echo "7. Verifying batch TTL values...\n";
    $keys = ['cache:item:1', 'cache:item:2', 'cache:item:3'];
    foreach ($keys as $key) {
        $ttl = $client->getKeyTTL($key);
        echo "   $key TTL: $ttl seconds\n";
    }
    echo "\n";
    
    // Cleanup
    echo "8. Cleaning up...\n";
    $client->delete('config:app');
    $client->batchDelete($keys);
    echo "✓ Cleanup complete\n\n";
    
    echo "All TTL operations completed successfully!\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    if (str_contains($e->getMessage(), 'TTL')) {
        echo "\nHint: Make sure TiKV is configured with enable-ttl=true in tikv.toml\n";
    }
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
} finally {
    $client->close();
}
