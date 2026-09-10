<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use Closure;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
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
     * TLS option keys recognised by buildTlsConfig(). Any other key inside
     * options['tls'] is rejected with an InvalidArgumentException instead of
     * being silently ignored (a silently ignored TLS key means a silently
     * plaintext connection).
     */
    private const RECOGNISED_TLS_KEYS = [
        'caCertFile',
        'caCertBaseDir',
        'caCertPem',
        'caCert',
        'clientCertFile',
        'clientCertBaseDir',
        'clientKeyFile',
        'clientCertPem',
        'clientKeyPem',
        'clientCert',
        'clientKey',
    ];

    /**
     * Build the shared connection layer from the given endpoints and options.
     *
     * @param string[] $pdEndpoints  PD cluster addresses
     * @param array<string, mixed> $options  Client options (see OPT_ constants
     *                                       on RawKvClient / TxnKvClient)
     * @param CodecInterface|null $codec  Key codec applied to PD region
     *        lookups. null builds the default V1 passthrough codec (RawKV
     *        behaviour — region keys sent to PD and region boundaries cached
     *        byte-for-byte unchanged). TxnKV callers pass CodecV1(Mode::Txn)
     *        so lookup keys are memory-comparable encoded and boundaries are
     *        decoded back to user-key space (issue #415, GAP-01).
     *
     * @throws InvalidArgumentException if PD endpoints array is empty
     */
    public static function create(
        array $pdEndpoints,
        ?LoggerInterface $logger = null,
        array $options = [],
        ?CodecInterface $codec = null,
    ): ConnectionBundle {
        if ($pdEndpoints === []) {
            throw new InvalidArgumentException('PD endpoints array must not be empty');
        }

        $resolvedLogger = $logger ?? new NullLogger();

        $metrics = self::resolveMetrics($options);

        $tlsConfig = self::buildTlsConfig($options);

        $grpcArgs = self::resolveGrpcChannelArgs($options);
        $allowInsecure = self::resolveAllowInsecure($options);

        $grpc = new GrpcClient(
            $resolvedLogger,
            $tlsConfig,
            allowInsecure: $allowInsecure,
            metrics: $metrics,
            maxReceiveMessageBytes: $grpcArgs['maxReceiveMessageBytes'],
            maxSendMessageBytes: $grpcArgs['maxSendMessageBytes'],
            keepaliveTimeMs: $grpcArgs['keepaliveTimeMs'],
            keepaliveTimeoutMs: $grpcArgs['keepaliveTimeoutMs'],
        );
        $storeCache = new StoreCache(logger: $resolvedLogger);
        $pdClient = new PdClient(
            $grpc,
            $pdEndpoints[0],
            $resolvedLogger,
            $storeCache,
            self::resolveLowResMaxStalenessMs($options),
            self::resolveTsoPoolSize($options),
            $codec ?? new CodecV1(),
        );

        $timeoutConfig = self::buildTimeoutConfig($options);
        $slowLogConfig = self::buildSlowLogConfig($options);
        $storeHostValidation = self::resolveStoreHostValidation($options);

        return new ConnectionBundle(
            grpc: $grpc,
            pdClient: $pdClient,
            storeCache: $storeCache,
            tlsConfig: $tlsConfig,
            timeoutConfig: $timeoutConfig,
            slowLogConfig: $slowLogConfig,
            logger: $resolvedLogger,
            metrics: $metrics,
            allowedStoreHosts: $storeHostValidation['allowedStoreHosts'],
            storeHostPolicy: $storeHostValidation['storeHostPolicy'],
            pdEndpoints: array_values($pdEndpoints),
            allowedStorePorts: $storeHostValidation['allowedStorePorts'],
        );
    }

    /**
     * Parse and validate the store-address host and port restriction options.
     *
     * Host restrictions are opt-in; when neither is configured, the default
     * host policy derived from the configured PD endpoints applies (see
     * RegionResolver::matchesDefaultPolicy()). The port option is always
     * enforced on the default policy path (privileged-port guard) and on
     * the explicit allowlist path when set.
     *
     * @param array<string, mixed> $options
     * @return array{allowedStoreHosts: list<string>, storeHostPolicy: ?Closure, allowedStorePorts: ?list<int>}
     */
    private static function resolveStoreHostValidation(array $options): array
    {
        $allowedStoreHosts = [];
        if (isset($options['allowedStoreHosts'])) {
            if (!is_array($options['allowedStoreHosts'])) {
                throw new InvalidArgumentException(
                    "options['allowedStoreHosts'] must be an array of hostnames, DNS suffixes or CIDR ranges",
                );
            }

            foreach ($options['allowedStoreHosts'] as $entry) {
                if (!is_string($entry) || $entry === '') {
                    throw new InvalidArgumentException(
                        "options['allowedStoreHosts'] entries must be non-empty strings",
                    );
                }
                $allowedStoreHosts[] = $entry;
            }
        }

        $storeHostPolicy = null;
        if (isset($options['storeHostPolicy'])) {
            if (!is_callable($options['storeHostPolicy'])) {
                throw new InvalidArgumentException(
                    'options[\'storeHostPolicy\'] must be callable',
                );
            }
            $storeHostPolicy = Closure::fromCallable($options['storeHostPolicy']);
        }

        $allowedStorePorts = null;
        if (array_key_exists('allowedStorePorts', $options)) {
            $ports = $options['allowedStorePorts'];
            if ($ports !== null && !is_array($ports)) {
                throw new InvalidArgumentException(
                    "options['allowedStorePorts'] must be a list of ports (ints 1-65535) or null",
                );
            }

            if (is_array($ports)) {
                $validatedPorts = [];
                foreach ($ports as $port) {
                    if (!is_int($port) || $port < 1 || $port > 65535) {
                        throw new InvalidArgumentException(
                            "options['allowedStorePorts'] entries must be ints in the range 1-65535",
                        );
                    }
                    $validatedPorts[] = $port;
                }
                $allowedStorePorts = $validatedPorts;
            }
        }

        return [
            'allowedStoreHosts' => $allowedStoreHosts,
            'storeHostPolicy' => $storeHostPolicy,
            'allowedStorePorts' => $allowedStorePorts,
        ];
    }

    /**
     * Resolve gRPC channel options (issue #265): options['grpc'] carries
     * maxReceiveMessageBytes / maxSendMessageBytes / keepaliveTimeMs /
     * keepaliveTimeoutMs. Each entry is optional; defaults come from the
     * GrpcClient constants.
     *
     * @param array<string, mixed> $options
     * @return array{maxReceiveMessageBytes: int, maxSendMessageBytes: int,
     *               keepaliveTimeMs: int, keepaliveTimeoutMs: int}
     */
    private static function resolveGrpcChannelArgs(array $options): array
    {
        $defaults = [
            'maxReceiveMessageBytes' => GrpcClient::DEFAULT_MAX_RECEIVE_MESSAGE_BYTES,
            'maxSendMessageBytes' => GrpcClient::DEFAULT_MAX_SEND_MESSAGE_BYTES,
            'keepaliveTimeMs' => GrpcClient::DEFAULT_KEEPALIVE_TIME_MS,
            'keepaliveTimeoutMs' => GrpcClient::DEFAULT_KEEPALIVE_TIMEOUT_MS,
        ];

        if (!isset($options['grpc']) || !is_array($options['grpc'])) {
            return $defaults;
        }

        $args = $defaults;
        foreach ($defaults as $key => $default) {
            if (!array_key_exists($key, $options['grpc'])) {
                continue;
            }
            $value = $options['grpc'][$key];
            if (!is_int($value) || $value < 1) {
                throw new InvalidArgumentException(
                    "options['grpc'][{$key}] must be an int >= 1, "
                    . 'got ' . get_debug_type($value)
                    . (is_int($value) ? " ({$value})" : ''),
                );
            }
            $args[$key] = $value;
        }

        return $args;
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function resolveLowResMaxStalenessMs(array $options): ?int
    {
        if (!array_key_exists('lowResTimestampMaxStalenessMs', $options)) {
            return null;
        }

        $stalenessMs = $options['lowResTimestampMaxStalenessMs'];
        if (!is_int($stalenessMs)) {
            throw new InvalidArgumentException(sprintf(
                "options['lowResTimestampMaxStalenessMs'] must be an int (milliseconds), %s given",
                get_debug_type($stalenessMs),
            ));
        }
        if ($stalenessMs < 0) {
            throw new InvalidArgumentException("options['lowResTimestampMaxStalenessMs'] must be >= 0");
        }

        return $stalenessMs;
    }

    /**
     * Resolve options['tsoPoolSize'] (issue #292): the number of timestamps
     * requested per pooled `Tso` RPC. null (absent) = TimestampOracle's
     * default; 1 disables pooling. Must be an int
     * `>= 1` and `<= TimestampOracle::MAX_TIMESTAMP_POOL_SIZE`.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveTsoPoolSize(array $options): ?int
    {
        if (!array_key_exists('tsoPoolSize', $options)) {
            return null;
        }

        $poolSize = $options['tsoPoolSize'];
        if (!is_int($poolSize)) {
            throw new InvalidArgumentException(sprintf(
                "options['tsoPoolSize'] must be an int (timestamp count), %s given",
                get_debug_type($poolSize),
            ));
        }
        if ($poolSize < 1) {
            throw new InvalidArgumentException("options['tsoPoolSize'] must be >= 1");
        }
        if ($poolSize > TimestampOracle::MAX_TIMESTAMP_POOL_SIZE) {
            throw new InvalidArgumentException(sprintf(
                "options['tsoPoolSize'] must be <= %d",
                TimestampOracle::MAX_TIMESTAMP_POOL_SIZE,
            ));
        }

        return $poolSize;
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
     * Resolve options['allowInsecure'] (default true) and forward it to the
     * GrpcClient. When set to false, the GrpcClient throws
     * InvalidStateException on any attempt to open a channel without usable
     * TLS credentials — TLS is then enforced through the public factory path
     * (issue #304, SEC-01).
     *
     * @param array<string, mixed> $options
     */
    private static function resolveAllowInsecure(array $options): bool
    {
        if (!array_key_exists('allowInsecure', $options)) {
            return true;
        }

        $allowInsecure = $options['allowInsecure'];
        if (!is_bool($allowInsecure)) {
            throw new InvalidArgumentException(
                "options['allowInsecure'] must be a boolean, got "
                . get_debug_type($allowInsecure)
            );
        }

        return $allowInsecure;
    }

    /**
     * Build TLS configuration from the options array.
     *
     * @param array<string, mixed> $options
     * @throws InvalidArgumentException when options['tls'] is present but not
     *                                  an array (or null), or when it contains
     *                                  unrecognised keys — both previously
     *                                  degraded silently to plaintext
     */
    private static function buildTlsConfig(array $options): ?\CrazyGoat\TiKV\Client\Tls\TlsConfig
    {
        if (!array_key_exists('tls', $options) || $options['tls'] === null) {
            return null;
        }

        if (!is_array($options['tls'])) {
            throw new InvalidArgumentException(
                "options['tls'] must be an array, got "
                . get_debug_type($options['tls'])
            );
        }

        $tlsOptions = $options['tls'];
        $unknown = array_diff(array_keys($tlsOptions), self::RECOGNISED_TLS_KEYS);
        if ($unknown !== []) {
            throw new InvalidArgumentException(
                "Unrecognised TLS option(s) in options['tls']: "
                . implode(', ', $unknown)
            );
        }

        // A recognised key with a non-string value would be silently dropped
        // by the guards below (the key would simply not be set) — the
        // connection would then go out in plaintext while the caller
        // believes TLS is configured. Fail closed instead (issue #304).
        // After this loop every value in $tlsOptions is a string.
        foreach ($tlsOptions as $key => $value) {
            if (!is_string($value)) {
                throw new InvalidArgumentException(
                    "options['tls'][{$key}] must be a string, got "
                    . get_debug_type($value)
                );
            }
        }

        $builder = new TlsConfigBuilder();

        // Explicit file-path options take priority
        if (isset($tlsOptions['caCertFile'])) {
            $baseDir = $tlsOptions['caCertBaseDir'] ?? null;
            $builder->withCaCertFile($tlsOptions['caCertFile'], $baseDir);
        } elseif (isset($tlsOptions['caCertPem'])) {
            $builder->withCaCertPem($tlsOptions['caCertPem']);
        } elseif (isset($tlsOptions['caCert'])) {
            // Backward compatibility: guess file path vs inline content
            $builder->withCaCert($tlsOptions['caCert']);
        }

        $hasClientCertFile = isset($tlsOptions['clientCertFile']);
        $hasClientKeyFile = isset($tlsOptions['clientKeyFile']);
        $hasClientCertPem = isset($tlsOptions['clientCertPem']);
        $hasClientKeyPem = isset($tlsOptions['clientKeyPem']);

        $clientPairConfigured = false;
        if ($hasClientCertFile && $hasClientKeyFile) {
            $baseDir = $tlsOptions['clientCertBaseDir'] ?? null;
            $builder->withClientCertFile(
                $tlsOptions['clientCertFile'],
                $tlsOptions['clientKeyFile'],
                $baseDir,
            );
            $clientPairConfigured = true;
        } elseif ($hasClientCertPem && $hasClientKeyPem) {
            $builder->withClientCertPem($tlsOptions['clientCertPem'], $tlsOptions['clientKeyPem']);
            $clientPairConfigured = true;
        } elseif (
            isset($tlsOptions['clientCert']) &&
            isset($tlsOptions['clientKey'])
        ) {
            // Backward compatibility: guess file path vs inline content
            $builder->withClientCert($tlsOptions['clientCert'], $tlsOptions['clientKey']);
            $clientPairConfigured = true;
        }

        // A half-specified client pair (e.g. clientCertFile without
        // clientKeyFile) falls through every branch above and would silently
        // connect in plaintext while the caller believes a client certificate
        // is configured. Fail closed instead (issue #304).
        if (
            !$clientPairConfigured && ($hasClientCertFile || $hasClientKeyFile
            || $hasClientCertPem || $hasClientKeyPem
            || isset($tlsOptions['clientCert']) || isset($tlsOptions['clientKey']))
        ) {
            throw new InvalidArgumentException(
                "Incomplete client certificate pair in options['tls']: client "
                . 'cert/key options must be given as a complete pair '
                . '(clientCertFile + clientKeyFile, clientCertPem + clientKeyPem, '
                . 'or clientCert + clientKey)',
            );
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
