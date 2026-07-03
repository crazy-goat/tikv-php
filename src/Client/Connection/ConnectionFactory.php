<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Observability\NoOpMetrics;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Factory that builds the shared gRPC + PD connection layer.
 *
 * Both {@see \CrazyGoat\TiKV\Client\RawKv\RawKvClient::create()} and
 * {@see \CrazyGoat\TiKV\Client\TxnKv\TxnKvClient::create()} previously
 * contained identical copies of TLS parsing, GrpcClient/StoreCache/PdClient
 * wiring, and timeout-config construction. This factory consolidates that
 * logic in one place.
 *
 * Usage:
 * <code>
 * $bundle = ConnectionFactory::create($pdEndpoints, $logger, $options);
 * $rawKv = new RawKvClient($bundle->pdClient, $bundle->grpc, …, logger: $bundle->logger, …);
 * </code>
 */
final class ConnectionFactory
{
    /**
     * Build the shared connection layer from the given endpoints and options.
     *
     * @param string[] $pdEndpoints  PD cluster addresses
     * @param array<string, mixed> $options  Client options (see OPT_ constants
     *                                       on RawKvClient / TxnKvClient)
     *
     *
     * @throws InvalidArgumentException if PD endpoints array is empty
     */
    public static function create(
        array $pdEndpoints,
        ?LoggerInterface $logger = null,
        array $options = [],
    ): ConnectionBundle {
        if ($pdEndpoints === []) {
            throw new InvalidArgumentException('PD endpoints array must not be empty');
        }

        $resolvedLogger = $logger ?? new NullLogger();

        $metrics = self::resolveMetrics($options);

        $tlsConfig = self::buildTlsConfig($options);

        $grpc = new GrpcClient($resolvedLogger, $tlsConfig, metrics: $metrics);
        $storeCache = new StoreCache(logger: $resolvedLogger);
        $pdClient = new PdClient($grpc, $pdEndpoints[0], $resolvedLogger, $storeCache);

        $timeoutConfig = self::buildTimeoutConfig($options);
        $slowLogConfig = self::buildSlowLogConfig($options);

        return new ConnectionBundle(
            grpc: $grpc,
            pdClient: $pdClient,
            storeCache: $storeCache,
            tlsConfig: $tlsConfig,
            timeoutConfig: $timeoutConfig,
            slowLogConfig: $slowLogConfig,
            logger: $resolvedLogger,
            metrics: $metrics,
        );
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function resolveMetrics(array $options): MetricsInterface
    {
        $metrics = $options['metrics'] ?? new NoOpMetrics();
        if (!$metrics instanceof MetricsInterface) {
            throw new InvalidArgumentException(
                "options['metrics'] must be an instance of MetricsInterface, "
                . 'got ' . get_debug_type($metrics)
            );
        }

        return $metrics;
    }

    /**
     * Build TLS configuration from the options array.
     *
     * @param array<string, mixed> $options
     */
    private static function buildTlsConfig(array $options): ?\CrazyGoat\TiKV\Client\Tls\TlsConfig
    {
        if (!isset($options['tls']) || !is_array($options['tls'])) {
            return null;
        }

        $tlsOptions = $options['tls'];
        $builder = new TlsConfigBuilder();

        // Explicit file-path options take priority
        if (isset($tlsOptions['caCertFile']) && is_string($tlsOptions['caCertFile'])) {
            $baseDir = isset($tlsOptions['caCertBaseDir']) && is_string($tlsOptions['caCertBaseDir'])
                ? $tlsOptions['caCertBaseDir']
                : null;
            $builder->withCaCertFile($tlsOptions['caCertFile'], $baseDir);
        } elseif (isset($tlsOptions['caCertPem']) && is_string($tlsOptions['caCertPem'])) {
            $builder->withCaCertPem($tlsOptions['caCertPem']);
        } elseif (isset($tlsOptions['caCert']) && is_string($tlsOptions['caCert'])) {
            // Backward compatibility: guess file path vs inline content
            $builder->withCaCert($tlsOptions['caCert']);
        }

        $hasClientCertFile = isset($tlsOptions['clientCertFile']) && is_string($tlsOptions['clientCertFile']);
        $hasClientKeyFile = isset($tlsOptions['clientKeyFile']) && is_string($tlsOptions['clientKeyFile']);
        $hasClientCertPem = isset($tlsOptions['clientCertPem']) && is_string($tlsOptions['clientCertPem']);
        $hasClientKeyPem = isset($tlsOptions['clientKeyPem']) && is_string($tlsOptions['clientKeyPem']);

        if ($hasClientCertFile && $hasClientKeyFile) {
            $baseDir = isset($tlsOptions['clientCertBaseDir']) && is_string($tlsOptions['clientCertBaseDir'])
                ? $tlsOptions['clientCertBaseDir']
                : null;
            $builder->withClientCertFile(
                $tlsOptions['clientCertFile'],
                $tlsOptions['clientKeyFile'],
                $baseDir,
            );
        } elseif ($hasClientCertPem && $hasClientKeyPem) {
            $builder->withClientCertPem($tlsOptions['clientCertPem'], $tlsOptions['clientKeyPem']);
        } elseif (
            isset($tlsOptions['clientCert']) && is_string($tlsOptions['clientCert']) &&
            isset($tlsOptions['clientKey']) && is_string($tlsOptions['clientKey'])
        ) {
            // Backward compatibility: guess file path vs inline content
            $builder->withClientCert($tlsOptions['clientCert'], $tlsOptions['clientKey']);
        }

        return $builder->build();
    }

    /**
     * Build TimeoutConfig from the options array.
     *
     * @param array<string, mixed> $options
     */
    private static function buildTimeoutConfig(array $options): TimeoutConfig
    {
        $timeoutConfig = new TimeoutConfig();

        if (isset($options['timeout']) && is_array($options['timeout'])) {
            $t = $options['timeout'];
            $timeoutConfig = new TimeoutConfig(
                readTimeoutMs: isset($t['readTimeoutMs']) && is_int($t['readTimeoutMs'])
                    ? $t['readTimeoutMs'] : $timeoutConfig->readTimeoutMs,
                writeTimeoutMs: isset($t['writeTimeoutMs']) && is_int($t['writeTimeoutMs'])
                    ? $t['writeTimeoutMs'] : $timeoutConfig->writeTimeoutMs,
                batchReadTimeoutMs: isset($t['batchReadTimeoutMs']) && is_int($t['batchReadTimeoutMs'])
                    ? $t['batchReadTimeoutMs'] : $timeoutConfig->batchReadTimeoutMs,
                batchWriteTimeoutMs: isset($t['batchWriteTimeoutMs']) && is_int($t['batchWriteTimeoutMs'])
                    ? $t['batchWriteTimeoutMs'] : $timeoutConfig->batchWriteTimeoutMs,
                scanTimeoutMs: isset($t['scanTimeoutMs']) && is_int($t['scanTimeoutMs'])
                    ? $t['scanTimeoutMs'] : $timeoutConfig->scanTimeoutMs,
                deleteRangeTimeoutMs: isset($t['deleteRangeTimeoutMs']) && is_int($t['deleteRangeTimeoutMs'])
                    ? $t['deleteRangeTimeoutMs'] : $timeoutConfig->deleteRangeTimeoutMs,
                checksumTimeoutMs: isset($t['checksumTimeoutMs']) && is_int($t['checksumTimeoutMs'])
                    ? $t['checksumTimeoutMs'] : $timeoutConfig->checksumTimeoutMs,
            );
        }

        return $timeoutConfig;
    }

    /**
     * Build SlowLogConfig from the options array.
     *
     * @param array<string, mixed> $options
     */
    private static function buildSlowLogConfig(array $options): ?SlowLogConfig
    {
        if (!isset($options['slowLog']) || !is_array($options['slowLog'])) {
            return null;
        }

        $s = $options['slowLog'];
        $defaults = new SlowLogConfig();

        return new SlowLogConfig(
            readThresholdMs: isset($s['readThresholdMs']) && is_int($s['readThresholdMs'])
                ? $s['readThresholdMs'] : $defaults->readThresholdMs,
            writeThresholdMs: isset($s['writeThresholdMs']) && is_int($s['writeThresholdMs'])
                ? $s['writeThresholdMs'] : $defaults->writeThresholdMs,
            batchReadThresholdMs: isset($s['batchReadThresholdMs']) && is_int($s['batchReadThresholdMs'])
                ? $s['batchReadThresholdMs'] : $defaults->batchReadThresholdMs,
            batchWriteThresholdMs: isset($s['batchWriteThresholdMs']) && is_int($s['batchWriteThresholdMs'])
                ? $s['batchWriteThresholdMs'] : $defaults->batchWriteThresholdMs,
            scanThresholdMs: isset($s['scanThresholdMs']) && is_int($s['scanThresholdMs'])
                ? $s['scanThresholdMs'] : $defaults->scanThresholdMs,
            deleteRangeThresholdMs: isset($s['deleteRangeThresholdMs']) && is_int($s['deleteRangeThresholdMs'])
                ? $s['deleteRangeThresholdMs'] : $defaults->deleteRangeThresholdMs,
            checksumThresholdMs: isset($s['checksumThresholdMs']) && is_int($s['checksumThresholdMs'])
                ? $s['checksumThresholdMs'] : $defaults->checksumThresholdMs,
        );
    }
}
