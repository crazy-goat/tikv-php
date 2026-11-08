<?php

/**
 * TiKV PHP Client - Batch Operations Example
 *
 * Demonstrates batch get, put, and delete operations with parallel execution.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$pdEndpoints = explode(',', getenv('PD_ENDPOINTS') ?: '127.0.0.1:2379');
$client = RawKvClient::create($pdEndpoints);

try {
    echo "TiKV PHP Client - Batch Operations Example\n";
    echo "==========================================\n\n";

    // Prepare test data
    $testData = [];
    for ($i = 1; $i <= 10; $i++) {
        $testData["batch:key:$i"] = "value-$i-" . uniqid();
    }

    // Batch Put
    echo "Batch putting " . count($testData) . " key-value pairs...\n";
    $client->batchPut($testData);
    echo "✓ Batch put successful\n\n";

    // Batch Get
    $keys = array_keys($testData);
    echo "Batch getting " . count($keys) . " keys...\n";
    $values = $client->batchGet($keys);

    $found = 0;
    foreach ($values as $key => $value) {
        if ($value !== null) {
            $found++;
            echo "  $key => " . substr($value, 0, 20) . "...\n";
        }
    }
    echo "✓ Retrieved $found/" . count($keys) . " values\n\n";

    // Batch Delete (delete half)
    $keysToDelete = array_slice($keys, 0, 5);
    echo "Batch deleting " . count($keysToDelete) . " keys...\n";
    $client->batchDelete($keysToDelete);
    echo "✓ Batch delete successful\n\n";

    // Verify deletion
    echo "Verifying deletion...\n";
    $remaining = $client->batchGet($keys);
    $remainingCount = count(array_filter($remaining, fn(?string $v): bool => $v !== null));
    echo "✓ $remainingCount keys still exist (expected: " . (count($keys) - count($keysToDelete)) . ")\n\n";

    // Cleanup remaining keys
    $remainingKeys = array_keys(array_filter($remaining, fn(?string $v): bool => $v !== null));
    if ($remainingKeys !== []) {
        echo "Cleaning up remaining " . count($remainingKeys) . " keys...\n";
        $client->batchDelete($remainingKeys);
        echo "✓ Cleanup complete\n\n";
    }

    echo "All batch operations completed successfully!\n";
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
} finally {
    $client->close();
}
