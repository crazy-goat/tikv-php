<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Connection\ConnectionBundle;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    public function testAllowedStorePortsDefaultsToNull(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertNull($bundle->allowedStorePorts);
    }

    // ========================================================================
    // options['grpc'] — gRPC channel arguments (issue #265)
    // ========================================================================

    public function testGrpcOptionsDefaultToGrpcClientDefaults(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379']);

        $this->assertSame(
            GrpcClient::DEFAULT_MAX_RECEIVE_MESSAGE_BYTES,
            $this->grpcProperty($grpc, 'maxReceiveMessageBytes'),
        );
        $this->assertSame(
            GrpcClient::DEFAULT_MAX_SEND_MESSAGE_BYTES,
            $this->grpcProperty($grpc, 'maxSendMessageBytes'),
        );
        $this->assertSame(
            GrpcClient::DEFAULT_KEEPALIVE_TIME_MS,
            $this->grpcProperty($grpc, 'keepaliveTimeMs'),
        );
        $this->assertSame(
            GrpcClient::DEFAULT_KEEPALIVE_TIMEOUT_MS,
            $this->grpcProperty($grpc, 'keepaliveTimeoutMs'),
        );
    }

    public function testGrpcOptionsThreadedThroughToGrpcClient(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379'], [
            'grpc' => [
                'maxReceiveMessageBytes' => 128 * 1024 * 1024,
                'maxSendMessageBytes' => 8 * 1024 * 1024,
                'keepaliveTimeMs' => 5000,
                'keepaliveTimeoutMs' => 1000,
            ],
        ]);

        $this->assertSame(128 * 1024 * 1024, $this->grpcProperty($grpc, 'maxReceiveMessageBytes'));
        $this->assertSame(8 * 1024 * 1024, $this->grpcProperty($grpc, 'maxSendMessageBytes'));
        $this->assertSame(5000, $this->grpcProperty($grpc, 'keepaliveTimeMs'));
        $this->assertSame(1000, $this->grpcProperty($grpc, 'keepaliveTimeoutMs'));
    }

    public function testGrpcOptionsRejectNonIntValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['grpc'][maxReceiveMessageBytes] must be an int >= 1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpc' => ['maxReceiveMessageBytes' => '64MB']],
        );
    }

    public function testGrpcOptionsRejectValueBelowOne(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['grpc'][keepaliveTimeMs] must be an int >= 1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpc' => ['keepaliveTimeMs' => 0]],
        );
    }

    // ========================================================================
    // options['tsoPoolSize'] — PD TSO timestamp pool (issue #292)
    // ========================================================================

    public function testTsoPoolSizeIsThreadedThroughToPdClient(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tsoPoolSize' => 32],
        );

        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);
        /** @var int|null $poolSize */
        $poolSize = (new \ReflectionProperty(PdClient::class, 'tsoPoolSize'))->getValue($pdClient);
        $this->assertSame(32, $poolSize);
    }

    public function testTsoPoolSizeRejectsNonIntValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['tsoPoolSize'] must be an int (timestamp count)");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tsoPoolSize' => '64'],
        );
    }

    public function testTsoPoolSizeRejectsValueBelowOne(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['tsoPoolSize'] must be >= 1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tsoPoolSize' => 0],
        );
    }

    /**
     * @param string[] $pdEndpoints
     * @param array<string, mixed> $options
     */
    private function grpcFromFactory(
        array $pdEndpoints,
        array $options = [],
    ): GrpcClient {
        $grpc = ConnectionFactory::create($pdEndpoints, options: $options)->grpc;
        assert($grpc instanceof GrpcClient);

        return $grpc;
    }

    private function grpcProperty(GrpcClient $grpc, string $property): int
    {
        /** @var int */
        return (new \ReflectionProperty(GrpcClient::class, $property))->getValue($grpc);
    }

    public function testAllowedStorePortsThreadedThroughBundle(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => [20160, 20161]],
        );

        $this->assertSame([20160, 20161], $bundle->allowedStorePorts);
    }

    public function testAllowedStorePortsExplicitNullIsAccepted(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => null],
        );

        $this->assertNull($bundle->allowedStorePorts);
    }

    public function testAllowedStorePortsRejectsNonArrayValue(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => '20160'],
        );
    }

    public function testAllowedStorePortsRejectsNonIntEntries(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => ['20160']],
        );
    }

    public function testAllowedStorePortsRejectsOutOfRangeEntries(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowedStorePorts' => [0]],
        );
    }

    private function grpcAllowInsecure(ConnectionBundle $bundle): bool
    {
        $ref = new \ReflectionProperty($bundle->grpc, 'allowInsecure');
        $value = $ref->getValue($bundle->grpc);
        assert(is_bool($value));

        return $value;
    }

    public function testAllowInsecureDefaultsToTrue(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertTrue($this->grpcAllowInsecure($bundle));
    }

    public function testAllowInsecureFalseIsForwardedToGrpcClient(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowInsecure' => false],
        );

        $this->assertFalse($this->grpcAllowInsecure($bundle));
    }

    public function testAllowInsecureRejectsNonBoolValue(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowInsecure' => 'false'],
        );
    }

    public function testTlsNonArrayStringThrowsInsteadOfSilentPlaintext(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['tls'] must be an array, got string");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => '/etc/tikv/ca.pem'],
        );
    }

    public function testTlsNonArrayBoolThrowsInsteadOfSilentPlaintext(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => true],
        );
    }

    public function testTlsExplicitNullMeansNoTls(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => null],
        );

        $this->assertNull($bundle->tlsConfig);
    }

    public function testUnknownTlsKeyThrowsNamingTheKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unrecognised TLS option(s) in options[\'tls\']: ca_cert');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['ca_cert' => '/etc/tikv/ca.pem']],
        );
    }

    public function testTlsKeyWithNonStringValueThrowsInsteadOfSilentPlaintext(): void
    {
        // A recognised key with a mistyped value would be silently dropped by
        // the is_string() guards, producing a plaintext connection while the
        // caller believes TLS is configured.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['tls'][caCertFile] must be a string, got int");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['caCertFile' => 123]],
        );
    }

    public function testIncompleteClientCertPairThrowsInsteadOfSilentPlaintext(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Incomplete client certificate pair');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['clientCertFile' => '/etc/tikv/client.crt']],
        );
    }
}
