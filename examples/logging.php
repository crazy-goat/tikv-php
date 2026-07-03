<?php
/**
 * TiKV PHP Client - PSR-3 Logging Example
 * 
 * Demonstrates how to integrate PSR-3 compatible loggers for debugging
 * and monitoring TiKV client operations.
 */

require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;

echo "TiKV PHP Client - PSR-3 Logging Example\n";
echo "=======================================\n\n";

// Setup Monolog logger
$logger = new Logger('tikv');

// Configure formatter for readable output
$formatter = new LineFormatter(
    "[%datetime%] %channel%.%level_name%: %message% %context%\n",
    'Y-m-d H:i:s'
);

// Add handler to output to stderr
$handler = new StreamHandler('php://stderr', Logger::DEBUG);
$handler->setFormatter($formatter);
$logger->pushHandler($handler);

echo "Logger configured with DEBUG level\n\n";

// Connect to TiKV with logging enabled
$pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', getenv('PD_ENDPOINTS')) : ['127.0.0.1:2379'];
echo "Connecting to TiKV at " . implode(', ', $pdEndpoints) . "...\n";

$client = RawKvClient::create($pdEndpoints, logger: $logger);

try {
    echo "\n--- Operation 1: Basic Put/Get ---\n";
    $client->put('log:test:1', 'value1');
    $value = $client->get('log:test:1');
    echo "Retrieved: $value\n";
    
    echo "\n--- Operation 2: Batch Operations ---\n";
    $client->batchPut([
        'log:test:a' => 'batch-a',
        'log:test:b' => 'batch-b',
        'log:test:c' => 'batch-c',
    ]);
    
    $values = $client->batchGet(['log:test:a', 'log:test:b', 'log:test:c']);
    echo "Batch retrieved " . count(array_filter($values)) . " values\n";
    
    echo "\n--- Operation 3: Scan Operation ---\n";
    $results = $client->scanPrefix('log:test:', limit: 10);
    echo "Scan found " . count($results) . " keys\n";
    
    echo "\n--- Operation 4: Cleanup ---\n";
    $client->batchDelete([
        'log:test:1',
        'log:test:a',
        'log:test:b',
        'log:test:c',
    ]);
    echo "Cleanup complete\n";
    
    echo "\n✓ All operations completed successfully!\n";
    echo "\nCheck the log output above to see:\n";
    echo "  - Connection attempts and region discovery\n";
    echo "  - Cache hits and misses\n";
    echo "  - Retry attempts (if any)\n";
    echo "  - Error conditions (if any)\n";
    
} catch (Exception $e) {
    echo "\n✗ Error: " . $e->getMessage() . "\n";
    echo "Stack trace:\n" . $e->getTraceAsString() . "\n";
} finally {
    echo "\nClosing client connection...\n";
    $client->close();
}

echo "\n";
echo "Logging Example - Production Usage\n";
echo "===================================\n\n";

echo "For production use, consider:\n\n";

echo "1. Different log levels for different environments:\n";
echo "   - Development: Logger::DEBUG (verbose)\n";
echo "   - Production:  Logger::WARNING (errors only)\n\n";

echo "2. Multiple handlers:\n";
echo "   \$logger->pushHandler(new StreamHandler('php://stderr', Logger::WARNING));\n";
echo "   \$logger->pushHandler(new StreamHandler('/var/log/tikv.log', Logger::INFO));\n\n";

echo "3. Structured logging with JSON format:\n";
echo "   use Monolog\\Formatter\\JsonFormatter;\n";
echo "   \$handler->setFormatter(new JsonFormatter());\n\n";

echo "4. Key redaction:\n";
echo "   - Raw keys are automatically redacted in log context using the\n";
echo "     KeyRedactor utility (first 8 bytes as hex + length).\n";
echo "   - Use KeyRedactor::setRedactor() to provide a custom redaction\n";
echo "     callable for your environment.\n";
echo "   - Avoid DEBUG level in production; keys contain tenant IDs,\n";
echo "     user IDs, session tokens, etc. and can leak via logs.\n\n";

echo "5. Integration with log aggregation (ELK, Datadog, etc.)\n\n";

echo "PSR-3 logging example completed!\n";
