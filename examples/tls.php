<?php
/**
 * TiKV PHP Client - TLS/SSL Configuration Example
 * 
 * Demonstrates how to configure TLS for secure connections.
 * 
 * NOTE: This example requires a TiKV cluster with TLS enabled.
 * For testing, you can generate test certificates with:
 *   openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
 */

require __DIR__ . '/../vendor/autoload.php';

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

echo "TiKV PHP Client - TLS/SSL Configuration Example\n";
echo "==============================================\n\n";

// Example 1: TLS with CA certificate only (server verification)
echo "Example 1: TLS with CA Certificate (Server Verification)\n";
echo "-----------------------------------------------------------\n";

$options1 = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        'caCertBaseDir' => '/path/to', // optional: restrict to base directory
    ],
];

echo "Options:\n";
echo "  caCertFile: /path/to/ca.crt\n";
echo "\nCode:\n";
echo "  \$options = ['tls' => ['caCertFile' => '/path/to/ca.crt']];\n";
echo "  \$client = RawKvClient::create(['tikv.example.com:2379'], options: \$options);\n\n";

// Example 2: Mutual TLS (mTLS) with client certificate
echo "Example 2: Mutual TLS (Client Certificate Authentication)\n";
echo "----------------------------------------------------------\n";

$options2 = [
    'tls' => [
        'caCertFile' => '/path/to/ca.crt',
        // Each *BaseDir applies only to its matching file reads:
        // caCertBaseDir → caCertFile, clientCertBaseDir → clientCertFile + clientKeyFile
        'clientCertFile' => '/path/to/client.crt',
        'clientKeyFile' => '/path/to/client.key',
        'clientCertBaseDir' => '/path/to',
    ],
];

echo "Options:\n";
echo "  caCertFile: /path/to/ca.crt\n";
echo "  clientCertFile: /path/to/client.crt\n";
echo "  clientKeyFile: /path/to/client.key\n";
echo "  clientCertBaseDir: /path/to\n";
echo "\nCode:\n";
echo "  \$options = [\n";
echo "      'tls' => [\n";
echo "          'caCertFile' => '/path/to/ca.crt',\n";
echo "          'clientCertFile' => '/path/to/client.crt',\n";
echo "          'clientKeyFile' => '/path/to/client.key',\n";
echo "          'clientCertBaseDir' => '/path/to',\n";
echo "      ],\n";
echo "  ];\n";
echo "  \$client = RawKvClient::create(['tikv.example.com:2379'], options: \$options);\n\n";

// Example 3: Using certificate content directly (inline PEM)
echo "Example 3: Using Certificate Content Directly (Inline PEM)\n";
echo "----------------------------------------------------------\n";

echo "You can also pass certificate content as strings via the *Pem variants:\n\n";
echo "  \$caCert = file_get_contents('/path/to/ca.crt');\n";
echo "  \$clientCert = file_get_contents('/path/to/client.crt');\n";
echo "  \$clientKey = file_get_contents('/path/to/client.key');\n";
echo "  \n";
echo "  \$options = [\n";
echo "      'tls' => [\n";
echo "          'caCertPem' => \$caCert,\n";
echo "          'clientCertPem' => \$clientCert,\n";
echo "          'clientKeyPem' => \$clientKey,\n";
echo "      ],\n";
echo "  ];\n\n";

// Example 4: Complete working example (if certificates exist)
echo "Example 4: Complete Working Example\n";
echo "------------------------------------\n";

$caCertPath = __DIR__ . '/../certs/ca.crt';
$clientCertPath = __DIR__ . '/../certs/client.crt';
$clientKeyPath = __DIR__ . '/../certs/client.key';

if (file_exists($caCertPath)) {
    echo "Found certificates, attempting connection...\n";
    
    try {
        $options = [
            'tls' => [
                'caCertFile' => $caCertPath,
            ],
        ];

        // Add client cert if available
        if (file_exists($clientCertPath) && file_exists($clientKeyPath)) {
            $options['tls']['clientCertFile'] = $clientCertPath;
            $options['tls']['clientKeyFile'] = $clientKeyPath;
            echo "Using mutual TLS (mTLS)\n";
        } else {
            echo "Using server verification only\n";
        }
        
        $pdEndpoints = getenv('PD_ENDPOINTS') ? explode(',', getenv('PD_ENDPOINTS')) : ['127.0.0.1:2379'];
        $client = RawKvClient::create($pdEndpoints, options: $options);
        
        // Test the connection
        $client->put('tls:test', 'secure-value');
        $value = $client->get('tls:test');
        $client->delete('tls:test');
        
        echo "✓ TLS connection successful! Retrieved value: $value\n";
        
        $client->close();
    } catch (Exception $e) {
        echo "✗ Connection failed: " . $e->getMessage() . "\n";
    }
} else {
    echo "No certificates found at $caCertPath\n";
    echo "Skipping TLS connection test.\n";
    echo "\nTo test TLS:\n";
    echo "1. Set up TiKV with TLS enabled\n";
    echo "2. Place certificates in certs/ directory:\n";
    echo "   - certs/ca.crt (CA certificate)\n";
    echo "   - certs/client.crt (client certificate, optional)\n";
    echo "   - certs/client.key (client private key, optional)\n";
}

echo "\nTLS configuration example completed!\n";
