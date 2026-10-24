<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use Closure;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
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
     * Options that configure the gRPC transport the factory builds. They
     * cannot be combined with a caller-supplied transport
     * (options['grpcClient']) — that transport was configured by the
     * application, so these options would be silently ignored.
     */
    private const TRANSPORT_OPTION_KEYS = [
        'tls',
        'allowInsecure',
        'maxChannels',
        'idleTtlMs',
        'grpc',
    ];

    /**
     * Build the shared connection layer from the given endpoints and options.
     *
     * @param string[] $pdEndpoints  PD cluster addresses
     * @param array<string, mixed> $options  Client options (see OPT_ constants
     *                                       on RawKvClient / TxnKvClient).
     *                                       Transport options: 'tls',
     *                                       'allowInsecure', 'maxChannels',
     *                                       'idleTtlMs' and 'grpc'. A
     *                                       caller-supplied transport is
     *                                       passed as 'grpcClient' and
     *                                       replaces every other transport
     *                                       option.
     * @param CodecInterface|null $codec  Key codec applied to PD region
     *        lookups. null builds the default V1 passthrough codec (RawKV
     *        behaviour — region keys sent to PD and region boundaries cached
     *        byte-for-byte unchanged). TxnKV callers pass CodecV1(Mode::Txn)
     *        so lookup keys are memory-comparable encoded and boundaries are
     *        decoded back to user-key space (issue #415, GAP-01).
     * @param Mode|null $mode RawKV or TxnKV mode used when apiVersion=2.
     *        API V2 resolves options['keyspace'] through PD before creating
     *        the final codec. An omitted/empty keyspace selects DEFAULT.
     *
     * @throws InvalidArgumentException if PD endpoints array is empty
     */
    public static function create(
        array $pdEndpoints,
        ?LoggerInterface $logger = null,
        array $options = [],
        ?CodecInterface $codec = null,
        ?Mode $mode = null,
    ): ConnectionBundle {
        if ($pdEndpoints === []) {
            throw new InvalidArgumentException('PD endpoints array must not be empty');
        }

        $resolvedLogger = $logger ?? new NullLogger();

        $metrics = self::resolveMetrics($options);

        $suppliedGrpc = self::resolveSuppliedGrpcClient($options);

        if ($suppliedGrpc instanceof GrpcClientInterface) {
            // The application owns the transport: it configured it (TLS,
            // channel pool) and it closes it. The transport options would be
            // silently ignored here, so they are rejected instead.
            self::rejectTransportOptions($options);
            $grpc = $suppliedGrpc;
            $tlsConfig = null;
            $ownsGrpcClient = false;

            // options['metrics'] is a client-side option: it always backs
            // the client-side counters (region cache, retries). The
            // transport's own RPC counters follow the transport, which was
            // built with its own backend — the factory cannot change it.
            // Announced so the split is never silent, but only when the
            // option was actually given (issue #283).
            if (isset($options['metrics'])) {
                $resolvedLogger->debug(
                    "options['metrics'] backs the client-side counters only: the injected "
                    . 'gRPC transport keeps its own metrics backend',
                );
            }
        } else {
            $tlsConfig = self::buildTlsConfig($options);

            $grpcArgs = self::resolveGrpcChannelArgs($options);
            $allowInsecure = self::resolveAllowInsecure($options);
            $maxChannels = self::resolveMaxChannels($options);
            $idleTtlMs = self::resolveIdleTtlMs($options);

            $grpc = new GrpcClient(
                $resolvedLogger,
                $tlsConfig,
                maxChannels: $maxChannels,
                idleTtlMs: $idleTtlMs,
                allowInsecure: $allowInsecure,
                metrics: $metrics,
                maxReceiveMessageBytes: $grpcArgs['maxReceiveMessageBytes'],
                maxSendMessageBytes: $grpcArgs['maxSendMessageBytes'],
                keepaliveTimeMs: $grpcArgs['keepaliveTimeMs'],
                keepaliveTimeoutMs: $grpcArgs['keepaliveTimeoutMs'],
            );
            $ownsGrpcClient = true;
        }
        $storeCache = new StoreCache(logger: $resolvedLogger);
        $resolvedMode = $mode ?? Mode::Raw;
        $resolvedCodec = $codec ?? new CodecV1($resolvedMode);
        $apiVersion = self::resolveApiVersion($options);
        // Built before the PdClients below: PD and TSO deadlines are
        // configured on TimeoutConfig and read from the client (issue #260).
        $timeoutConfig = self::buildTimeoutConfig($options);
        $resolvedClusterId = null;
        if ($apiVersion === 2 && !$resolvedCodec instanceof CodecV2) {
            $keyspaceName = self::resolveKeyspaceName($options);
            $probePdClient = new PdClient(
                $grpc,
                array_values($pdEndpoints),
                $resolvedLogger,
                $storeCache,
                self::resolveLowResMaxStalenessMs($options),
                self::resolveTsoPoolSize($options),
                new CodecV1($resolvedMode),
                $timeoutConfig,
            );
            try {
                $keyspaceId = (new KeyspaceResolver($probePdClient))->resolve($keyspaceName);
                $resolvedCodec = new CodecV2($resolvedMode, $keyspaceId, $keyspaceName);
                $resolvedClusterId = $probePdClient->getClusterId();
            } catch (\Throwable $e) {
                if ($ownsGrpcClient) {
                    $grpc->close();
                }
                throw $e;
            }
        }
        $pdClient = new PdClient(
            $grpc,
            array_values($pdEndpoints),
            $resolvedLogger,
            $storeCache,
            self::resolveLowResMaxStalenessMs($options),
            self::resolveTsoPoolSize($options),
            $resolvedCodec,
            $timeoutConfig,
        );
        if ($resolvedClusterId !== null) {
            $pdClient->setClusterId($resolvedClusterId);
        }

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
            codec: $resolvedCodec,
            ownsGrpcClient: $ownsGrpcClient,
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
    private static function resolveApiVersion(array $options): int
    {
        $version = $options['apiVersion'] ?? 1;
        if (!is_int($version) || !in_array($version, [0, 1, 2], true)) {
            throw new InvalidArgumentException("options['apiVersion'] must be 0 (V1), 1 (V1), or 2 (V2)");
        }

        return $version === 2 ? 2 : 1;
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function resolveKeyspaceName(array $options): string
    {
        $name = $options['keyspace'] ?? KeyspaceResolver::DEFAULT_NAME;
        if (!is_string($name)) {
            throw new InvalidArgumentException(
                "options['keyspace'] must be a string when apiVersion is 2",
            );
        }

        return $name === '' ? KeyspaceResolver::DEFAULT_NAME : $name;
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
     * Resolve options['maxChannels'] (default {@see GrpcClient::DEFAULT_MAX_CHANNELS}):
     * the maximum number of gRPC channels the transport keeps open at the
     * same time. The least recently used channel is evicted when the cap is
     * reached. Must be an int >= 1.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveMaxChannels(array $options): int
    {
        return self::resolvePositiveIntOption($options, 'maxChannels', GrpcClient::DEFAULT_MAX_CHANNELS);
    }

    /**
     * Resolve options['idleTtlMs'] (default {@see GrpcClient::DEFAULT_IDLE_TTL_MS}):
     * the idle time in milliseconds after which an unused channel is evicted.
     * Must be an int >= 1.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveIdleTtlMs(array $options): int
    {
        return self::resolvePositiveIntOption($options, 'idleTtlMs', GrpcClient::DEFAULT_IDLE_TTL_MS);
    }

    /**
     * Read an int >= 1 from the options, falling back to the given default.
     *
     * @param array<string, mixed> $options
     */
    private static function resolvePositiveIntOption(array $options, string $key, int $default): int
    {
        if (!array_key_exists($key, $options)) {
            return $default;
        }

        $value = $options[$key];
        if (!is_int($value)) {
            throw new InvalidArgumentException(sprintf(
                "options['%s'] must be an int, %s given",
                $key,
                get_debug_type($value),
            ));
        }
        if ($value < 1) {
            throw new InvalidArgumentException("options['{$key}'] must be >= 1");
        }

        return $value;
    }

    /**
     * Resolve the caller-supplied gRPC transport (options['grpcClient']).
     *
     * A transport built by the application is used as it is: it is
     * long-lived and shared (one transport can serve a RawKV and a
     * transactional client), and the application stays its owner — the
     * client never closes it.
     *
     * @param array<string, mixed> $options
     */
    private static function resolveSuppliedGrpcClient(array $options): ?GrpcClientInterface
    {
        if (!array_key_exists('grpcClient', $options)) {
            return null;
        }

        $grpc = $options['grpcClient'];
        if ($grpc === null) {
            return null;
        }
        if (!$grpc instanceof GrpcClientInterface) {
            throw new InvalidArgumentException(
                "options['grpcClient'] must be an instance of GrpcClientInterface, got "
                . get_debug_type($grpc)
            );
        }

        return $grpc;
    }

    /**
     * Reject the transport options when the application supplies its own
     * transport: they configure a transport the factory does not build, so
     * honouring them is impossible and ignoring them silently would leave
     * the caller believing (for example) that TLS is enforced.
     *
     * @param array<string, mixed> $options
     */
    private static function rejectTransportOptions(array $options): void
    {
        $conflicts = array_values(array_intersect(self::TRANSPORT_OPTION_KEYS, array_keys($options)));
        if ($conflicts === []) {
            return;
        }

        $names = array_map(
            static fn (string $key): string => "'{$key}'",
            $conflicts,
        );

        throw new InvalidArgumentException(sprintf(
            "options['grpcClient'] cannot be combined with the transport option(s) %s — "
            . 'configure them on the GrpcClient you pass in',
            implode(', ', $names),
        ));
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
     * The option keys a config class understands.
     *
     * Derived from the class's constructor parameters, so the list cannot
     * drift from the class that carries the values: a new timeout field is
     * configurable — and validated — as soon as it exists (issue #272,
     * GRPC-13).
     *
     * @param class-string $class
     * @return list<string>
     */
    private static function recognisedOptionKeys(string $class): array
    {
        $parameters = (new \ReflectionMethod($class, '__construct'))->getParameters();

        $keys = [];
        foreach ($parameters as $parameter) {
            $keys[] = $parameter->getName();
        }

        return $keys;
    }

    /**
     * Reject an option key the config class does not know.
     *
     * A silently dropped key is a misconfiguration nobody sees: the option
     * has no effect and the default applies, with no warning anywhere. The
     * TLS options already fail that way (see
     * {@see self::RECOGNISED_TLS_KEYS}); the timeouts and the slow-log
     * thresholds now do too (issue #272, GRPC-13).
     *
     * @param array<array-key, mixed> $options
     * @param list<string> $recognised
     * @param string $optionPath the option path as the user wrote it, e.g.
     *                           "options['timeout']"
     */
    private static function assertKnownOptionKeys(array $options, array $recognised, string $optionPath): void
    {
        $unknown = array_diff(
            array_map(strval(...), array_keys($options)),
            $recognised,
        );

        if ($unknown === []) {
            return;
        }

        sort($unknown);

        throw new InvalidArgumentException(sprintf(
            'Unrecognised option(s) in %s: %s',
            $optionPath,
            implode(', ', $unknown),
        ));
    }

    /**
     * Reject non-positive timeout options before they reach
     * {@see TimeoutConfig} (issue #276, GRPC-17).
     *
     * The check names the option key the user wrote, so the error points at
     * the offending entry of the configuration array instead of at an
     * internal field name. `batchDeadlineMs` keeps `0` as the documented
     * "no overall batch deadline" value.
     *
     * @param array<array-key, mixed> $timeouts
     * @param list<string> $recognised the keys the config class knows; every
     *                                 key has already been checked against it
     */
    private static function assertPositiveTimeoutOptions(array $timeouts, array $recognised): void
    {
        $optionPath = "options['timeout']";
        foreach ($timeouts as $key => $value) {
            if (!is_string($key) || !in_array($key, $recognised, true)) {
                continue;
            }
            if (!is_int($value)) {
                // The type is checked by assertIntOptions() first.
                continue;
            }
            if ($key === 'batchDeadlineMs') {
                if ($value < 0) {
                    throw new InvalidArgumentException(
                        sprintf('%s[%s] must be greater than or equal to 0, got %d', $optionPath, $key, $value),
                    );
                }
                continue;
            }
            if ($value <= 0) {
                throw new InvalidArgumentException(
                    sprintf('%s[%s] must be greater than 0, got %d', $optionPath, $key, $value),
                );
            }
        }
    }

    /**
     * Reject an option whose value is not a number.
     *
     * A value of the wrong type was silently dropped before (issue #272,
     * GRPC-13): the option had no effect and the default applied, with no
     * warning anywhere — the same failure mode an unknown key has. Every
     * other option group in the factory already fails closed on a mistyped
     * value.
     *
     * @param array<array-key, mixed> $options
     * @param list<string> $recognised
     * @param string $optionPath the option path as the user wrote it, e.g.
     *                           "options['timeout']"
     */
    private static function assertIntOptions(array $options, array $recognised, string $optionPath): void
    {
        foreach ($options as $key => $value) {
            if (!is_string($key) || !in_array($key, $recognised, true)) {
                continue;
            }
            if (!is_int($value)) {
                throw new InvalidArgumentException(sprintf(
                    '%s[%s] must be an int (milliseconds), %s given',
                    $optionPath,
                    $key,
                    get_debug_type($value),
                ));
            }
        }
    }

    /**
     * Reject a slow-log threshold that is negative, naming the option path
     * the user wrote (issue #272, GRPC-13).
     *
     * @param array<array-key, mixed> $thresholds
     * @param list<string> $recognised
     */
    private static function assertNonNegativeSlowLogOptions(array $thresholds, array $recognised): void
    {
        foreach ($thresholds as $key => $value) {
            if (!is_string($key) || !in_array($key, $recognised, true)) {
                continue;
            }
            if (!is_int($value)) {
                // The type is checked by assertIntOptions() first.
                continue;
            }
            if ($value < 0) {
                throw new InvalidArgumentException(sprintf(
                    "options['slowLog'][%s] must be greater than or equal to 0, got %d",
                    $key,
                    $value,
                ));
            }
        }
    }

    /**
     * Build TimeoutConfig from the options array.
     *
     * The mapping is driven by {@see TimeoutConfig}'s constructor signature
     * (issue #272, GRPC-13): every field is configurable under its own name,
     * and a field that is added to the class is configurable the moment it
     * exists. The hand-written list this replaces had already dropped the
     * `ingestTimeoutMs` field once — it was re-added by #580, but nothing
     * kept the two in step, so the next field would have gone the same way.
     * A dropped field is invisible: the operator configured a five-minute
     * ingest deadline and got the 60-second default, with no warning.
     *
     * @param array<string, mixed> $options
     */
    private static function buildTimeoutConfig(array $options): TimeoutConfig
    {
        $defaults = new TimeoutConfig();

        if (!array_key_exists('timeout', $options) || $options['timeout'] === null) {
            return $defaults;
        }

        if (!is_array($options['timeout'])) {
            throw new InvalidArgumentException(
                "options['timeout'] must be an array, got " . get_debug_type($options['timeout'])
            );
        }

        $t = $options['timeout'];

        $recognised = self::recognisedOptionKeys(TimeoutConfig::class);

        self::assertKnownOptionKeys($t, $recognised, "options['timeout']");
        self::assertIntOptions($t, $recognised, "options['timeout']");
        self::assertPositiveTimeoutOptions($t, $recognised);

        /** @var array<string, int> $values */
        $values = [];
        foreach ($recognised as $key) {
            $value = $t[$key] ?? null;
            if (is_int($value)) {
                $values[$key] = $value;
            }
        }

        return new TimeoutConfig(...$values);
    }

    /**
     * Build SlowLogConfig from the options array.
     *
     * Driven by {@see SlowLogConfig}'s constructor signature, like
     * {@see self::buildTimeoutConfig()} (issue #272, GRPC-13): a threshold
     * that is added to the class is configurable the moment it exists, and
     * an unknown key is reported instead of silently dropped.
     *
     * @param array<string, mixed> $options
     */
    private static function buildSlowLogConfig(array $options): ?SlowLogConfig
    {
        if (!array_key_exists('slowLog', $options) || $options['slowLog'] === null) {
            return null;
        }

        if (!is_array($options['slowLog'])) {
            throw new InvalidArgumentException(
                "options['slowLog'] must be an array, got " . get_debug_type($options['slowLog'])
            );
        }

        $s = $options['slowLog'];

        $recognised = self::recognisedOptionKeys(SlowLogConfig::class);

        self::assertKnownOptionKeys($s, $recognised, "options['slowLog']");
        self::assertIntOptions($s, $recognised, "options['slowLog']");
        self::assertNonNegativeSlowLogOptions($s, $recognised);

        /** @var array<string, int> $values */
        $values = [];
        foreach ($recognised as $key) {
            $value = $s[$key] ?? null;
            if (is_int($value)) {
                $values[$key] = $value;
            }
        }

        return new SlowLogConfig(...$values);
    }
}
