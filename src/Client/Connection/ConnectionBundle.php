<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use CrazyGoat\TiKV\Client\Observability\MetricsInterface;
use CrazyGoat\TiKV\Client\Tls\TlsConfig;
use Psr\Log\LoggerInterface;

/**
 * Value object holding the shared connections and configuration produced
 * by {@see ConnectionFactory}.
 *
 * Carries all objects that are built identically for both RawKvClient
 * and TxnKvClient, eliminating the copy-paste bootstrap between the two
 * create() methods.
 */
final readonly class ConnectionBundle
{
    public function __construct(
        public GrpcClientInterface $grpc,
        public PdClientInterface $pdClient,
        public StoreCache $storeCache,
        public ?TlsConfig $tlsConfig,
        public TimeoutConfig $timeoutConfig,
        public ?SlowLogConfig $slowLogConfig,
        public LoggerInterface $logger,
        public MetricsInterface $metrics,
    ) {
    }
}
