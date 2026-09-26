<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\ConnectionBundle;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    public function testAllowedStorePortsDefaultsToNull(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertNull($bundle->allowedStorePorts);
    }

    public function testApiVersionRejectsUnsupportedValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['apiVersion'] must be 0 (V1), 1 (V1), or 2 (V2)");

        ConnectionFactory::create(['127.0.0.1:2379'], options: ['apiVersion' => 3]);
    }

    public function testApiV2KeyspaceMustBeString(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['keyspace'] must be a string when apiVersion is 2");

        ConnectionFactory::create(['127.0.0.1:2379'], options: [
            'apiVersion' => 2,
            'keyspace' => 42,
        ]);
    }

    public function testPreconfiguredApiV2CodecIsRetained(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['apiVersion' => 2],
            codec: $codec,
            mode: Mode::Raw,
        );

        $this->assertSame($codec, $bundle->codec);
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

    public function testTsoPoolSizeRejectsValueAboveMaximum(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['tsoPoolSize'] must be <= 1000");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tsoPoolSize' => TimestampOracle::MAX_TIMESTAMP_POOL_SIZE + 1],
        );
    }

    // ========================================================================
    // options['timeout']['ingestTimeoutMs'] — SST ingest deadline (issue #185 row 5)
    // ========================================================================

    public function testTimeoutIngestTimeoutMsDefaultsToTimeoutConfigDefault(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertSame(60000, $bundle->timeoutConfig->ingestTimeoutMs);
    }

    public function testTimeoutIngestTimeoutMsIsThreadedThrough(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['ingestTimeoutMs' => 45000]],
        );

        $this->assertSame(45000, $bundle->timeoutConfig->ingestTimeoutMs);
    }

    public function testTimeoutIngestTimeoutMsNonIntFallsBackToDefault(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['ingestTimeoutMs' => '45000']],
        );

        $this->assertSame(60000, $bundle->timeoutConfig->ingestTimeoutMs);
    }

    // ========================================================================
    // options['timeout']['batchDeadlineMs'] — batch fan-out deadline (issue #185 row 6)
    // ========================================================================

    public function testTimeoutBatchDeadlineMsDefaultsToZeroDisabled(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertSame(0, $bundle->timeoutConfig->batchDeadlineMs);
    }

    public function testTimeoutBatchDeadlineMsIsThreadedThrough(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => 2500]],
        );

        $this->assertSame(2500, $bundle->timeoutConfig->batchDeadlineMs);
    }

    public function testTimeoutBatchDeadlineMsNonIntFallsBackToDefault(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => '2500']],
        );

        $this->assertSame(0, $bundle->timeoutConfig->batchDeadlineMs);
    }

    // ========================================================================
    // options['timeout']['pdTimeoutMs'|'tsoTimeoutMs'|'lockResolveTimeoutMs']
    // — metadata-plane deadlines (issue #260)
    // ========================================================================

    /**
     * @return array<string, array{string, int, int}>
     */
    public static function metadataDeadlineProvider(): array
    {
        return [
            'pd' => ['pdTimeoutMs', 3000, 4321],
            'tso' => ['tsoTimeoutMs', 3000, 5432],
            'lockResolve' => ['lockResolveTimeoutMs', 5000, 6543],
        ];
    }

    #[DataProvider('metadataDeadlineProvider')]
    public function testTimeoutMetadataDeadlineDefaultsToTheFiniteDefault(string $field, int $default): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertSame($default, $bundle->timeoutConfig->{$field});
    }

    #[DataProvider('metadataDeadlineProvider')]
    public function testTimeoutMetadataDeadlineIsThreadedThrough(string $field, int $default, int $configured): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => [$field => $configured]],
        );

        $this->assertSame($configured, $bundle->timeoutConfig->{$field});
        // …and the other two keep their own defaults: they are configured
        // separately because a hung PD and a slow store are different
        // failures.
        foreach (['pdTimeoutMs', 'tsoTimeoutMs', 'lockResolveTimeoutMs'] as $other) {
            if ($other !== $field) {
                $this->assertSame(
                    (new TimeoutConfig())->{$other},
                    $bundle->timeoutConfig->{$other},
                    "{$other} must not inherit {$field}'s value",
                );
            }
        }
    }

    #[DataProvider('metadataDeadlineProvider')]
    public function testTimeoutMetadataDeadlineNonIntFallsBackToDefault(
        string $field,
        int $default,
    ): void {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => [$field => '3000']],
        );

        $this->assertSame($default, $bundle->timeoutConfig->{$field});
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
