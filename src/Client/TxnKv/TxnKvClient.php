<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\TxnKv;

use CrazyGoat\TiKV\Client\Cache\RegionCache;
use CrazyGoat\TiKV\Client\Cache\RegionCacheInterface;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\ClientClosedException;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Region\RegionResolver;
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class TxnKvClient
{
    public const OPT_TIMEOUT = 'timeout';

    private bool $closed = false;
    private readonly RegionResolver $regionResolver;

    /**
     * @param string[] $pdEndpoints PD addresses (currently only the first is used)
     * @param array<string, mixed> $options Client options, including 'tls' for TLS
     *                                      configuration and 'timeout' for timeout config
     */
    public static function create(array $pdEndpoints, ?LoggerInterface $logger = null, array $options = []): self
    {
        if ($pdEndpoints === []) {
            throw new InvalidArgumentException('PD endpoints array must not be empty');
        }

        $resolvedLogger = $logger ?? new NullLogger();

        $tlsConfig = null;
        if (isset($options['tls']) && is_array($options['tls'])) {
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

            $tlsConfig = $builder->build();
        }

        $grpc = new GrpcClient($resolvedLogger, $tlsConfig);
        $pdAddress = $pdEndpoints[0];
        $storeCache = new StoreCache(logger: $resolvedLogger);
        $pdClient = new PdClient($grpc, $pdAddress, $resolvedLogger, $storeCache);

        $timeoutConfig = new TimeoutConfig();

        if (isset($options[self::OPT_TIMEOUT]) && is_array($options[self::OPT_TIMEOUT])) {
            $t = $options[self::OPT_TIMEOUT];
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
            );
        }

        return new self($pdClient, $grpc, logger: $resolvedLogger, timeoutConfig: $timeoutConfig);
    }

    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache = new RegionCache(),
        ?RegionResolver $regionResolver = null,
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
        private readonly TimeoutConfig $timeoutConfig = new TimeoutConfig(),
    ) {
        $this->regionResolver = $regionResolver ?? new RegionResolver($this->pdClient, $this->regionCache);
    }

    /**
     * @param array{pessimistic?: bool, priority?: int} $options
     */
    public function begin(array $options = []): Transaction
    {
        $this->ensureOpen();

        $pessimistic = (bool) ($options['pessimistic'] ?? true);
        $priority = (int) ($options['priority'] ?? 0);

        $startTs = $this->pdClient->getTimestamp();

        $txnId = uniqid('txn-', true);

        $this->logger->info('Transaction started', [
            'txnId' => $txnId,
            'startTs' => $startTs,
            'pessimistic' => $pessimistic,
        ]);

        $lockResolver = new LockResolver(
            $this->grpc,
            $this->regionResolver,
            $this->regionCache,
            $this->maxBackoffMs,
            $this->logger,
        );

        return new Transaction(
            txnId: $txnId,
            startTs: $startTs,
            pessimistic: $pessimistic,
            priority: $priority,
            pdClient: $this->pdClient,
            grpc: $this->grpc,
            regionCache: $this->regionCache,
            lockResolver: $lockResolver,
            regionResolver: $this->regionResolver,
            maxBackoffMs: $this->maxBackoffMs,
            logger: $this->logger,
            timeoutConfig: $this->timeoutConfig,
        );
    }

    /**
     * Get the learned cluster ID, or null if not yet discovered.
     */
    public function getClusterId(): ?int
    {
        return $this->pdClient->getClusterId();
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;

        try {
            $this->grpc->close();
        } catch (\Throwable $e) {
            $this->logger->error('Failed to close gRPC client', ['exception' => $e]);
        }

        try {
            $this->pdClient->close();
        } catch (\Throwable $e) {
            $this->logger->error('Failed to close PD client', ['exception' => $e]);
        }
    }

    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new ClientClosedException();
        }
    }
}
