<?php
/**
 * TiKV PHP Client - Atomic Operations Example
 * 
 * Demonstrates Compare-And-Swap (CAS) and Put-If-Absent operations.
 */

require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', getenv('PD_ENDPOINTS')) : ['127.0.0.1:2379'];
$client = RawKvClient::create($pdEndpoints);

try {
    // Enable atomic mode for CAS operations
    $client->setAtomicForCAS(true);

    echo "TiKV PHP Client - Atomic Operations Example\n";
    echo "==========================================\n\n";
    
    // Compare-And-Swap (CAS) Example
    echo "1. Compare-And-Swap (CAS) Example\n";
    echo "   --------------------------------\n";
    
    // Initialize counter
    $client->put('counter', '0');
    echo "   Initialized counter to '0'\n";
    
    // Successful CAS
    $result = $client->compareAndSwap('counter', '0', '1');
    echo "   CAS('counter', '0', '1'): " . ($result->swapped ? 'SUCCESS' : 'FAILED') . "\n";
    if ($result->swapped) {
        echo "   Previous value: " . ($result->previousValue ?? 'null') . "\n";
    }
    
    // Failed CAS (wrong expected value)
    $result = $client->compareAndSwap('counter', '0', '2');
    echo "   CAS('counter', '0', '2'): " . ($result->swapped ? 'SUCCESS' : 'FAILED') . "\n";
    if (!$result->swapped) {
        echo "   Current value is: {$result->previousValue} (expected '0')\n";
    }
    
    // Successful CAS with correct expected value
    $result = $client->compareAndSwap('counter', '1', '2');
    echo "   CAS('counter', '1', '2'): " . ($result->swapped ? 'SUCCESS' : 'FAILED') . "\n";
    echo "   Final counter value: " . $client->get('counter') . "\n\n";
    
    // Put-If-Absent Example (Distributed Lock Pattern)
    echo "2. Put-If-Absent (Distributed Lock Pattern)\n";
    echo "   ------------------------------------------\n";
    
    // Try to acquire lock
    $lockKey = 'lock:resource:123';
    $owner = 'process-' . getmypid();
    
    echo "   Process $owner trying to acquire lock...\n";
    $existing = $client->putIfAbsent($lockKey, $owner);
    
    if ($existing === null) {
        echo "   ✓ Lock acquired by $owner\n";
        
        // Simulate work
        echo "   Working with locked resource...\n";
        sleep(1);
        
        // Release lock
        $client->delete($lockKey);
        echo "   ✓ Lock released\n";
    } else {
        echo "   ✗ Lock already held by: $existing\n";
    }
    echo "\n";
    
    // CAS with null check (create only if not exists)
    echo "3. CAS with Null Check (Create-Only Pattern)\n";
    echo "   -------------------------------------------\n";
    
    $uniqueKey = 'unique:item:' . uniqid();
    echo "   Attempting to create $uniqueKey...\n";
    
    // Try to create (expectedValue = null means key should not exist)
    $result = $client->compareAndSwap($uniqueKey, null, 'created');
    
    if ($result->swapped) {
        echo "   ✓ Created successfully (key did not exist)\n";
    } else {
        echo "   ✗ Key already exists with value: {$result->previousValue}\n";
    }
    
    // Second attempt should fail
    echo "   Attempting to create same key again...\n";
    $result = $client->compareAndSwap($uniqueKey, null, 'created-again');
    
    if ($result->swapped) {
        echo "   ✓ Created\n";
    } else {
        echo "   ✗ Key already exists with value: {$result->previousValue}\n";
    }
    echo "\n";
    
    // CAS with TTL
    echo "4. CAS with TTL (Atomic Update with Expiration)\n";
    echo "   ----------------------------------------------\n";
    
    $sessionKey = 'session:cas-test';
    $client->put($sessionKey, 'initial', ttl: 60);
    echo "   Created session with 60s TTL\n";
    
    $result = $client->compareAndSwap($sessionKey, 'initial', 'updated', ttl: 30);
    if ($result->swapped) {
        echo "   ✓ Updated with new 30s TTL\n";
        $newTtl = $client->getKeyTTL($sessionKey);
        echo "   New TTL: $newTtl seconds\n";
    }
    echo "\n";
    
    // Cleanup
    echo "5. Cleaning up...\n";
    $client->delete('counter');
    $client->delete($uniqueKey);
    $client->delete($sessionKey);
    echo "   ✓ Cleanup complete\n\n";
    
    echo "All atomic operations completed successfully!\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
} finally {
    $client->close();
}
