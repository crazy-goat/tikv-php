<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use Closure;
use CrazyGoat\TiKV\Client\Cache\StoreCache;
use CrazyGoat\TiKV\Client\Codec\CodecInterface;
use CrazyGoat\TiKV\Client\Codec\CodecV1;
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
    /**
     * @param list<string> $allowedStoreHosts
     * @param (Closure(string): bool)|null $storeHostPolicy
     * @param list<string> $pdEndpoints
     * @param list<int>|null $allowedStorePorts null = unrestricted
     * @param TlsConfig|null $tlsConfig null when the transport was injected
     *        through `options['grpcClient']` — the TLS configuration of an
     *        injected transport is its own, and the factory builds none
     *        (issue #283).
     * @param MetricsInterface $metrics the client-side metrics backend
     *        (region cache, retries). With an injected transport the
     *        transport's own RPC counters follow that transport — the factory
     *        cannot change them (issue #283).
     * @param bool $ownsGrpcClient true when the factory built the transport
     *        and the client may close it; false when the application supplied
     *        it (options['grpcClient']) and stays its owner
     */
    public function __construct(
        public GrpcClientInterface $grpc,
        public PdClientInterface $pdClient,
        public StoreCache $storeCache,
        public ?TlsConfig $tlsConfig,
        public TimeoutConfig $timeoutConfig,
        public ?SlowLogConfig $slowLogConfig,
        public LoggerInterface $logger,
        public MetricsInterface $metrics,
        public array $allowedStoreHosts = [],
        public ?Closure $storeHostPolicy = null,
        public array $pdEndpoints = [],
        public ?array $allowedStorePorts = null,
        public CodecInterface $codec = new CodecV1(),
        public bool $ownsGrpcClient = true,
    ) {
    }
}
