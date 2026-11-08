<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

$pdEndpoints = explode(',', getenv('PD_ENDPOINTS') ?: '127.0.0.1:2379');
$client = RawKvClient::create($pdEndpoints);

try {
    echo "TiKV PHP Client - Basic Example\n";
    echo "================================\n\n";

    echo "Putting key 'hello' with value 'world'...\n";
    $client->put('hello', 'world');
    echo "✓ Put successful\n\n";

    echo "Getting key 'hello'...\n";
    $value = $client->get('hello');
    echo "✓ Got value: " . ($value ?? 'null') . "\n\n";

    echo "Deleting key 'hello'...\n";
    $client->delete('hello');
    echo "✓ Delete successful\n\n";

    echo "Getting key 'hello' after delete...\n";
    $value = $client->get('hello');
    echo "✓ Value after delete: " . ($value ?? 'null') . "\n\n";

    echo "All operations completed successfully!\n";
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
} finally {
    $client->close();
}
