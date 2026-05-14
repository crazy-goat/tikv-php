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
use CrazyGoat\TiKV\Client\Tls\TlsConfigBuilder;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class TxnKvClient
{
    private bool $closed = false;

    /**
     * @param string[] $pdEndpoints PD addresses (currently only the first is used)
     * @param array<string, mixed> $options Client options, including 'tls' for TLS configuration
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

            if (isset($tlsOptions['caCert']) && is_string($tlsOptions['caCert'])) {
                $builder->withCaCert($tlsOptions['caCert']);
            }

            if (
                isset($tlsOptions['clientCert']) && is_string($tlsOptions['clientCert']) &&
                isset($tlsOptions['clientKey']) && is_string($tlsOptions['clientKey'])
            ) {
                $builder->withClientCert($tlsOptions['clientCert'], $tlsOptions['clientKey']);
            }

            $tlsConfig = $builder->build();
        }

        $grpc = new GrpcClient($resolvedLogger, $tlsConfig);
        $pdAddress = $pdEndpoints[0];
        $storeCache = new StoreCache(logger: $resolvedLogger);
        $pdClient = new PdClient($grpc, $pdAddress, $resolvedLogger, $storeCache);

        return new self($pdClient, $grpc, logger: $resolvedLogger);
    }

    public function __construct(
        private readonly PdClientInterface $pdClient,
        private readonly GrpcClientInterface $grpc,
        private readonly RegionCacheInterface $regionCache = new RegionCache(),
        private readonly int $maxBackoffMs = 20000,
        private readonly LoggerInterface $logger = new NullLogger(),
    ) {
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
            $this->pdClient,
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
            maxBackoffMs: $this->maxBackoffMs,
            logger: $this->logger,
        );
    }

    public function close(): void
    {
        if (!$this->closed) {
            $this->grpc->close();
            $this->pdClient->close();
            $this->closed = true;
        }
    }

    private function ensureOpen(): void
    {
        if ($this->closed) {
            throw new ClientClosedException();
        }
    }
}
