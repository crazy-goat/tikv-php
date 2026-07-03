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

    /**
     * Set CA certificate from a file path.
     *
     * The path is canonicalized with realpath() and validated against
     * an optional allowed base directory to prevent path traversal.
     *
     * @param string      $path       Path to the CA certificate file (.pem, .crt, or .key)
     * @param string|null $baseDir    Optional allowed base directory (resolved via realpath)
     *
     * @throws InvalidArgumentException if the file cannot be read or path traversal detected
     */
    public function withCaCertFile(string $path, ?string $baseDir = null): self
    {
        $this->caCert = $this->readFile($path, $baseDir);
        return $this;
    }

    /**
     * Set CA certificate from inline PEM content.
     */
    public function withCaCertPem(string $pem): self
    {
        $this->caCert = $pem;
        return $this;
    }

    /**
     * Set client certificate and key from file paths.
     *
     * Both paths are canonicalized with realpath() and validated against
     * an optional allowed base directory to prevent path traversal.
     *
     * @param string      $certPath   Path to the client certificate file (.pem, .crt)
     * @param string      $keyPath    Path to the client key file (.pem, .key)
     * @param string|null $baseDir    Optional allowed base directory (resolved via realpath)
     *
     * @throws InvalidArgumentException if any file cannot be read or path traversal detected
     */
    public function withClientCertFile(string $certPath, string $keyPath, ?string $baseDir = null): self
    {
        $this->clientCert = $this->readFile($certPath, $baseDir);
        $this->clientKey = $this->readFile($keyPath, $baseDir);
        return $this;
    }

    /**
     * Set client certificate and key from inline PEM content.
     */
    public function withClientCertPem(string $certPem, string $keyPem): self
    {
        $this->clientCert = $certPem;
        $this->clientKey = $keyPem;
        return $this;
    }

    /**
     * Set CA certificate.
     *
     * If the value is a path to an existing file with a .pem, .crt, or .key extension,
     * the file is read with path traversal protection. Otherwise the value is treated
     * as inline PEM content.
     *
     * @deprecated Use withCaCertFile() for file paths or withCaCertPem() for inline content.
     *             This method uses file_exists() to guess the input type, which is
     *             ambiguous and less secure.
     */
    public function withCaCert(string $caCert): self
    {
        return $this->resolveWithGuess($caCert) ? $this->withCaCertFile($caCert) : $this->withCaCertPem($caCert);
    }

    /**
     * Set client certificate and key.
     *
     * If the values are paths to existing files with allowed extensions,
     * they are read with path traversal protection. Otherwise they are treated
     * as inline PEM content.
     *
     * @deprecated Use withClientCertFile() for file paths or withClientCertPem() for inline content.
     *             This method uses file_exists() to guess the input type, which is
     *             ambiguous and less secure.
     */
    public function withClientCert(string $cert, string $key): self
    {
        return $this->resolveWithGuess($cert) || $this->resolveWithGuess($key)
            ? $this->withClientCertFile($cert, $key)
            : $this->withClientCertPem($cert, $key);
    }

    public function build(): TlsConfig
    {
        return new TlsConfig($this->caCert, $this->clientCert, $this->clientKey);
    }

    /**
     * Check whether a string looks like a file path (exists, readable).
     * This is used for backward compatibility with the deprecated guessing methods.
     * Extension validation is done later in readFile().
     */
    private function resolveWithGuess(string $value): bool
    {
        return file_exists($value) && is_readable($value);
    }

    /**
     * Read a TLS credential file with path traversal protection.
     *
     * @param string      $path    File path to read
     * @param string|null $baseDir Optional allowed base directory
     *
     * @return string File contents
     *
     * @throws InvalidArgumentException If the file cannot be read or path traversal is detected
     */
    private function readFile(string $path, ?string $baseDir = null): string
    {
        // Validate file extension
        $extension = strtolower(pathinfo($path, PATHINFO_EXTENSION));
        if (!in_array($extension, self::ALLOWED_EXTENSIONS, true)) {
            throw new InvalidArgumentException(
                sprintf(
                    'TLS file has disallowed extension "%s". Allowed: %s',
                    $extension,
                    implode(', ', self::ALLOWED_EXTENSIONS),
                ),
            );
        }

        // Canonicalize the path to resolve symlinks and ../ segments
        $resolved = realpath($path);
        if ($resolved === false) {
            throw new InvalidArgumentException('Cannot read TLS file: file does not exist or is not readable');
        }

        // If a base directory is specified, ensure the resolved path is inside it
        if ($baseDir !== null) {
            $resolvedBase = realpath($baseDir);
            if ($resolvedBase === false) {
                throw new InvalidArgumentException('Invalid TLS base directory: directory does not exist');
            }
            // Ensure the resolved path starts with the resolved base directory
            if (!str_starts_with($resolved, $resolvedBase)) {
                throw new InvalidArgumentException('TLS file path is outside the allowed base directory');
            }
        }

        $content = file_get_contents($resolved);
        if ($content === false) {
            throw new InvalidArgumentException('Cannot read TLS file: read failed');
        }

        return $content;
    }
}
