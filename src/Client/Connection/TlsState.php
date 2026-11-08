<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Connection;

use CrazyGoat\TiKV\Client\Tls\TlsConfig;

/**
 * TLS state of the transport carried by a {@see ConnectionBundle}.
 *
 * {@see ConnectionBundle::$tlsConfig} is `null` both for a transport the
 * factory built without TLS and for a transport the application supplied
 * through `options['grpcClient']` — the factory does not inspect the latter.
 * This enum removes the ambiguity: a supplied transport is
 * {@see self::Unknown}, never silently reported as plaintext (issue #695).
 */
enum TlsState
{
    /** The factory did not build the transport, so its TLS state is unknown. */
    case Unknown;

    /** The factory built a transport without TLS configuration. */
    case Plaintext;

    /** The factory built a transport with TLS configuration. */
    case Tls;

    /**
     * State of a transport the factory built from the given TLS configuration.
     */
    public static function forFactoryBuiltTransport(?TlsConfig $tlsConfig): self
    {
        return $tlsConfig instanceof \CrazyGoat\TiKV\Client\Tls\TlsConfig ? self::Tls : self::Plaintext;
    }
}
