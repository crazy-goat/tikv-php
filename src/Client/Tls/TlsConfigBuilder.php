<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Tls;

use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;

final class TlsConfigBuilder
{
    /** @var string[] Allowed file extensions for TLS credential files */
    private const ALLOWED_EXTENSIONS = ['pem', 'crt', 'key'];

    private ?string $caCert = null;
    private ?string $clientCert = null;
    private ?string $clientKey = null;

    public function withCaCert(string $caCert): self
    {
        $this->caCert = $this->resolveContent($caCert);
        return $this;
    }

    public function withClientCert(string $cert, string $key): self
    {
        $this->clientCert = $this->resolveContent($cert);
        $this->clientKey = $this->resolveContent($key);
        return $this;
    }

    public function build(): TlsConfig
    {
        return new TlsConfig($this->caCert, $this->clientCert, $this->clientKey);
    }

    private function resolveContent(string $value): string
    {
        if (file_exists($value) && is_readable($value)) {
            $extension = strtolower(pathinfo($value, PATHINFO_EXTENSION));
            if (!in_array($extension, self::ALLOWED_EXTENSIONS, true)) {
                throw new InvalidArgumentException(
                    sprintf(
                        'TLS file "%s" has disallowed extension "%s". Allowed: %s',
                        $value,
                        $extension,
                        implode(', ', self::ALLOWED_EXTENSIONS),
                    ),
                );
            }
            $content = file_get_contents($value);
            if ($content === false) {
                throw new InvalidArgumentException("Cannot read file: {$value}");
            }
            return $content;
        }
        return $value;
    }
}
