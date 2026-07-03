<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Tls;

final class TlsConfig
{
    public function __construct(
        public ?string $caCert = null,
        public ?string $clientCert = null,
        public ?string $clientKey = null,
    ) {
    }

    public function isEnabled(): bool
    {
        return $this->caCert !== null || $this->clientCert !== null || $this->clientKey !== null;
    }

    /**
     * Zero-out sensitive credential data.
     * Call this after credentials have been consumed by gRPC channel creation.
     */
    public function close(): void
    {
        if ($this->clientKey !== null) {
            $this->clientKey = str_repeat("\0", strlen($this->clientKey));
        }
    }
}
