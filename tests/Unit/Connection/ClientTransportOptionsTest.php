<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * The transport options reach the transport through the public client
 * factories (issue #407).
 *
 * `RawKvClient::create()` and `TxnKvClient::create()` hand their options to
 * the shared connection factory, so an option that works there has to work
 * here too — the fail-closed TLS control is only worth anything when the
 * documented `create()` path really enforces it.
 */
class ClientTransportOptionsTest extends TestCase
{
    public function testRawKvClientRefusesAPlaintextChannelWhenTlsIsNotConfigured(): void
    {
        $client = RawKvClient::create(
            ['127.0.0.1:2379'],
            options: ['allowInsecure' => false],
        );

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('TLS is not configured');

        $client->get('key');
    }

    public function testTransactionalClientRefusesAPlaintextChannelWhenTlsIsNotConfigured(): void
    {
        $client = TxnKvClient::create(
            ['127.0.0.1:2379'],
            options: ['allowInsecure' => false],
        );

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('TLS is not configured');

        $client->begin();
    }
}
