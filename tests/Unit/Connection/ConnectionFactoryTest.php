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
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
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

    public function testTimeoutIngestTimeoutMsNonIntIsRejected(): void
    {
        // A mistyped value was silently dropped before (issue #272): the
        // option had no effect and the default applied, with no warning.
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['timeout'][ingestTimeoutMs] must be an int (milliseconds), string given",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['ingestTimeoutMs' => '45000']],
        );
    }

    // ========================================================================
    // options['timeout']['batchDeadlineMs'] — batch fan-out deadline (issue #185 row 6)
    // ========================================================================

    public function testTimeoutBatchDeadlineMsDefaultsToAFiniteValue(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertSame(TimeoutConfig::DEFAULT_BATCH_DEADLINE_MS, $bundle->timeoutConfig->batchDeadlineMs);
    }

    public function testTimeoutBatchDeadlineMsZeroDisablesTheDeadline(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => 0]],
        );

        $this->assertSame(0, $bundle->timeoutConfig->batchDeadlineMs);
    }

    public function testTimeoutBatchDeadlineMsBelowAPerCallTimeoutIsRejectedByName(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('batchDeadlineMs (2500) must not be less than writeTimeoutMs (5000)');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => 2500]],
        );
    }

    public function testTimeoutBatchDeadlineMsIsThreadedThrough(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => 90000]],
        );

        $this->assertSame(90000, $bundle->timeoutConfig->batchDeadlineMs);
    }

    public function testTimeoutBatchDeadlineMsNonIntIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['timeout'][batchDeadlineMs] must be an int (milliseconds), string given",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => '2500']],
        );
    }

    // ========================================================================
    // options['timeout'] — a non-positive timeout is a configuration error
    // (issue #276, GRPC-17). The message names the option key the user
    // wrote, not an internal field name.
    // ========================================================================

    /**
     * @return array<string, array{string, int}>
     */
    public static function nonPositiveTimeoutOptionProvider(): array
    {
        return [
            'readTimeoutMs' => ['readTimeoutMs', -1],
            'writeTimeoutMs' => ['writeTimeoutMs', 0],
            'batchReadTimeoutMs' => ['batchReadTimeoutMs', 0],
            'batchWriteTimeoutMs' => ['batchWriteTimeoutMs', 0],
            'scanTimeoutMs' => ['scanTimeoutMs', 0],
            'deleteRangeTimeoutMs' => ['deleteRangeTimeoutMs', 0],
            'checksumTimeoutMs' => ['checksumTimeoutMs', 0],
            'ingestTimeoutMs' => ['ingestTimeoutMs', 0],
            'pdTimeoutMs' => ['pdTimeoutMs', 0],
            'tsoTimeoutMs' => ['tsoTimeoutMs', 0],
            'lockResolveTimeoutMs' => ['lockResolveTimeoutMs', 0],
        ];
    }

    #[DataProvider('nonPositiveTimeoutOptionProvider')]
    public function testANonPositiveTimeoutOptionIsRejected(string $key, int $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf("options['timeout'][%s] must be greater than 0", $key));

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => [$key => $value]],
        );
    }

    /**
     * A key the factory does not know is a misconfiguration, not a no-op:
     * a silently dropped option has no effect and the default applies, with
     * no warning anywhere (issue #272, GRPC-13). The TLS options already
     * fail this way.
     */
    public function testAnUnknownTimeoutOptionIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Unrecognised option(s) in options['timeout']: notATimeout");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['notATimeout' => 0]],
        );
    }

    /**
     * A group that is not a map at all is a misconfiguration of the same
     * class as an unknown key — `options['tls']` already fails closed on it
     * (issue #272, GRPC-13).
     */
    public function testANonArrayTimeoutOptionIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['timeout'] must be an array, got string");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => 'readTimeoutMs=1000'],
        );
    }

    /**
     * A group that is explicitly `null` means "not configured" — the same
     * as leaving it out — not "wrong type".
     */
    public function testANullTimeoutOptionIsTreatedAsAbsent(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => null],
        );

        $this->assertSame(5000, $bundle->timeoutConfig->readTimeoutMs);
    }

    public function testANullSlowLogOptionIsTreatedAsAbsent(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['slowLog' => null],
        );

        $this->assertNull($bundle->slowLogConfig);
    }

    public function testANonArraySlowLogOptionIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['slowLog'] must be an array, got string");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['slowLog' => 'readThresholdMs=100'],
        );
    }

    /**
     * A slow-log threshold of the wrong type was silently dropped before
     * (issue #272): the option had no effect and the default applied.
     */
    public function testANonIntSlowLogThresholdIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['slowLog'][readThresholdMs] must be an int (milliseconds), string given",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['slowLog' => ['readThresholdMs' => '250']],
        );
    }

    /**
     * The slow-log error names the option the user wrote, not the internal
     * field — like the timeout group does.
     */
    public function testANegativeSlowLogThresholdNamesTheOption(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['slowLog'][readThresholdMs] must be greater than or equal to 0, got -1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['slowLog' => ['readThresholdMs' => -1]],
        );
    }

    public function testAnUnknownSlowLogOptionIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Unrecognised option(s) in options['slowLog']: notAThreshold");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['slowLog' => ['notAThreshold' => 0]],
        );
    }

    /**
     * Every {@see TimeoutConfig} field is configurable through
     * `options['timeout']` under its own name — the factory derives the
     * list of keys it validates from that class, so a new field is
     * validated as soon as it exists and no key can drift.
     */
    public function testEveryTimeoutConfigFieldIsConfigurableByItsOwnName(): void
    {
        $parameters = (new \ReflectionMethod(TimeoutConfig::class, '__construct'))->getParameters();

        $fields = [];
        foreach ($parameters as $parameter) {
            $fields[] = $parameter->getName();
        }

        self::assertNotEmpty($fields, 'TimeoutConfig must have timeout fields');

        foreach ($fields as $field) {
            // A batch deadline below a per-call timeout is rejected (#275).
            $configured = $field === 'batchDeadlineMs' ? 90000 : 4321;

            $bundle = ConnectionFactory::create(
                ['127.0.0.1:2379'],
                options: ['timeout' => [$field => $configured]],
            );

            self::assertSame(
                $configured,
                $bundle->timeoutConfig->{$field},
                "options['timeout']['{$field}'] must reach TimeoutConfig::\${$field}",
            );
        }
    }

    /**
     * The same for the slow-log thresholds: every
     * {@see SlowLogConfig} field is configurable through `options['slowLog']`
     * under its own name (issue #272, GRPC-13).
     */
    public function testEverySlowLogConfigFieldIsConfigurableByItsOwnName(): void
    {
        $parameters = (new \ReflectionMethod(SlowLogConfig::class, '__construct'))->getParameters();

        $fields = [];
        foreach ($parameters as $parameter) {
            $fields[] = $parameter->getName();
        }

        self::assertNotEmpty($fields, 'SlowLogConfig must have threshold fields');

        foreach ($fields as $field) {
            $configured = 4321;

            $bundle = ConnectionFactory::create(
                ['127.0.0.1:2379'],
                options: ['slowLog' => [$field => $configured]],
            );

            self::assertNotNull($bundle->slowLogConfig, 'options[slowLog] must build a SlowLogConfig');
            self::assertSame(
                $configured,
                $bundle->slowLogConfig->{$field},
                "options['slowLog']['{$field}'] must reach SlowLogConfig::\${$field}",
            );
        }
    }

    public function testANegativeBatchDeadlineOptionIsRejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['timeout'][batchDeadlineMs] must be greater than or equal to 0");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => ['batchDeadlineMs' => -1]],
        );
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
    public function testTimeoutMetadataDeadlineNonIntIsRejected(
        string $field,
    ): void {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            "options['timeout'][%s] must be an int (milliseconds), string given",
            $field,
        ));

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => [$field => '3000']],
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

    /**
     * The fail-closed contract of issue #407: with `allowInsecure` false and
     * no TLS configuration, the transport refuses to open a plaintext channel
     * instead of silently downgrading.
     */
    public function testAllowInsecureFalseWithoutTlsThrowsWhenAChannelIsOpened(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['allowInsecure' => false],
        );

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('TLS is not configured');

        $bundle->grpc->getChannel('127.0.0.1:20160');
    }

    // ========================================================================
    // options['maxChannels'] / options['idleTtlMs'] — channel pool
    // ========================================================================

    public function testMaxChannelsDefaultsToGrpcClientDefault(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379']);

        $this->assertSame(GrpcClient::DEFAULT_MAX_CHANNELS, $this->grpcProperty($grpc, 'maxChannels'));
    }

    public function testMaxChannelsIsThreadedThroughToGrpcClient(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379'], ['maxChannels' => 8]);

        $this->assertSame(8, $this->grpcProperty($grpc, 'maxChannels'));
    }

    public function testMaxChannelsRejectsNonIntValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['maxChannels'] must be an int, string given");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['maxChannels' => '8'],
        );
    }

    public function testMaxChannelsRejectsValueBelowOne(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['maxChannels'] must be >= 1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['maxChannels' => 0],
        );
    }

    public function testIdleTtlMsDefaultsToGrpcClientDefault(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379']);

        $this->assertSame(GrpcClient::DEFAULT_IDLE_TTL_MS, $this->grpcProperty($grpc, 'idleTtlMs'));
    }

    public function testIdleTtlMsIsThreadedThroughToGrpcClient(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379'], ['idleTtlMs' => 30000]);

        $this->assertSame(30000, $this->grpcProperty($grpc, 'idleTtlMs'));
    }

    public function testIdleTtlMsRejectsNonIntValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['idleTtlMs'] must be an int, string given");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['idleTtlMs' => '30000'],
        );
    }

    public function testIdleTtlMsRejectsValueBelowOne(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['idleTtlMs'] must be >= 1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['idleTtlMs' => 0],
        );
    }

    // ========================================================================
    // options['grpcClient'] — a transport supplied by the application
    // ========================================================================

    public function testFactoryBuiltTransportIsOwnedByTheClient(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertTrue($bundle->ownsGrpcClient);
    }

    public function testSuppliedGrpcClientIsUsedAsTheTransport(): void
    {
        $grpc = new GrpcClient();

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => $grpc],
        );

        $this->assertSame($grpc, $bundle->grpc);
        $this->assertFalse($bundle->ownsGrpcClient);
    }

    public function testSuppliedGrpcClientRejectsNonGrpcClientValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['grpcClient'] must be an instance of GrpcClientInterface, got string");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => 'not-a-grpc-client'],
        );
    }

    /**
     * @return array<string, array{array<string, mixed>}>
     */
    public static function transportOptionProvider(): array
    {
        return [
            'tls' => [['tls' => ['caCertFile' => '/etc/tikv/ca.pem']]],
            'allowInsecure' => [['allowInsecure' => false]],
            'maxChannels' => [['maxChannels' => 8]],
            'idleTtlMs' => [['idleTtlMs' => 30000]],
            'grpc' => [['grpc' => ['keepaliveTimeMs' => 5000]]],
        ];
    }

    /**
     * A supplied transport was configured by the application, so the
     * transport options would be silently ignored — they are rejected
     * instead.
     */
    /**
     * @param array<string, mixed> $option
     */
    #[DataProvider('transportOptionProvider')]
    public function testSuppliedGrpcClientRejectsTransportOptions(array $option): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['grpcClient'] cannot be combined with the transport option(s)");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => new GrpcClient()] + $option,
        );
    }

    public function testSuppliedGrpcClientAcceptsNull(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => null],
        );

        $this->assertTrue($bundle->ownsGrpcClient);
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
