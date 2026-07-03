<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Tls;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;

final class TlsConfig
{
    /**
     * @param string|null $caCert     CA certificate content (PEM)
     * @param string|null $clientCert Client certificate content (PEM)
     * @param string|null $clientKey  Client private key content (PEM)
     *
     * @throws InvalidArgumentException if the configuration is partial:
     *         - clientCert is set without clientKey, or vice versa
     *         - clientCert and clientKey are set without caCert
     */
    public function __construct(
        public ?string $caCert = null,
        public ?string $clientCert = null,
        public ?string $clientKey = null,
    ) {
        // If one of clientCert/clientKey is set but not the other
        if (($this->clientCert !== null) xor ($this->clientKey !== null)) {
            throw new InvalidArgumentException(
                'Both clientCert and clientKey must be provided together; '
                . 'got clientCert=' . var_export($this->clientCert !== null, true)
                . ', clientKey=' . var_export($this->clientKey !== null, true)
            );
        }

        // If client cert/key are provided without CA, the configuration is incomplete
        // and would silently downgrade to plaintext — reject it.
        if ($this->clientCert !== null && $this->clientKey !== null && $this->caCert === null) {
            throw new InvalidArgumentException(
                'Partial TLS configuration: client certificate and key are provided '
                . 'but no CA certificate is set. A CA certificate is required to '
                . 'verify the server certificate. Either provide a CA certificate '
                . 'via caCert, or remove the client certificate and key if you '
                . 'intend to use an insecure connection.'
            );
        }
    }

    /**
     * Returns true when any TLS material is present in this configuration.
     *
     * Previously this only checked for caCert, which meant a partial mTLS
     * configuration (clientCert/clientKey without caCert) was reported as
     * "not enabled" and the connection silently fell back to plaintext.
     * Now it returns true if either the CA certificate or the client
     * certificate+key pair is present.
     *
     * Note: if isEnabled() returns true, the configuration is guaranteed
     * to be complete (see constructor validation).
     */
    public function isEnabled(): bool
    {
        return $this->caCert !== null
            || ($this->clientCert !== null && $this->clientKey !== null);
    }

    /**
     * Returns true when the TLS configuration is complete for a secure connection.
     *
     * A complete configuration requires at least a CA certificate. If client
     * certificate and key are provided, they are also required (enforced by
     * the constructor).
     */
    public function isComplete(): bool
    {
        return $this->caCert !== null;
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
