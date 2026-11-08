<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Connection;

use CrazyGoat\Proto\Keyspacepb\LoadKeyspaceResponse;
use CrazyGoat\Proto\Pdpb\ResponseHeader;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Connection\ConnectionBundle;
use CrazyGoat\TiKV\Client\Connection\ConnectionFactory;
use CrazyGoat\TiKV\Client\Connection\PdClient;
use CrazyGoat\TiKV\Client\Connection\TimestampOracle;
use CrazyGoat\TiKV\Client\Connection\TlsState;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\InvalidStateException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClient;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\SlowLogOperation;
use CrazyGoat\TiKV\Client\Grpc\TimeoutConfig;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class ConnectionFactoryTest extends TestCase
{
    private string $tempDir;

    protected function setUp(): void
    {
        $this->tempDir = sys_get_temp_dir() . '/tikv-connection-factory-' . uniqid();
        mkdir($this->tempDir, 0777, true);
    }

    protected function tearDown(): void
    {
        $this->removeTree($this->tempDir);
    }

    private function removeTree(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }

        foreach (scandir($dir) ?: [] as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }
            $path = $dir . '/' . $entry;
            if (is_dir($path)) {
                $this->removeTree($path);
                continue;
            }
            unlink($path);
        }
        rmdir($dir);
    }

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

    public function testApiV2ProbeRejectsMismatchingExpectedClusterId(): void
    {
        // The API V2 probe is a separate PdClient built before the final one;
        // it must enforce the option before the keyspace is resolved.
        $header = new ResponseHeader();
        $header->setClusterId(999);

        $response = new LoadKeyspaceResponse();
        $response->setHeader($header);

        $grpc = $this->createMock(GrpcClientInterface::class);
        $grpc->expects($this->once())
            ->method('call')
            ->with(
                '127.0.0.1:2379',
                'keyspacepb.Keyspace',
                'LoadKeyspace',
                $this->anything(),
                LoadKeyspaceResponse::class,
            )
            ->willReturn($response);

        $this->expectException(TiKvException::class);
        $this->expectExceptionMessage('Cluster ID mismatch: expected 12345, server reported 999');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: [
                'apiVersion' => 2,
                'keyspace' => 'tenant-a',
                'grpcClient' => $grpc,
                'expectedClusterId' => 12345,
            ],
        );
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

    public function testGrpcReceiveLimitReachesTheChannelArguments(): void
    {
        // SEC-16 (issue #319): the receive bound has to be enforced by
        // grpc-core in the channel arguments, before the response payload is
        // materialised as a PHP string — not by the post-allocation guard in
        // GrpcResponseParser.
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379'], [
            'grpc' => ['maxReceiveMessageBytes' => 8 * 1024 * 1024],
        ]);

        $args = $this->channelArgsOf($grpc);

        $this->assertArrayHasKey('grpc.max_receive_message_length', $args);
        $this->assertSame(8 * 1024 * 1024, $args['grpc.max_receive_message_length']);
    }

    public function testGrpcReceiveLimitDefaultsToTheExplicitFiniteValue(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379']);

        $args = $this->channelArgsOf($grpc);

        $this->assertArrayHasKey('grpc.max_receive_message_length', $args);
        $this->assertSame(
            GrpcClient::DEFAULT_MAX_RECEIVE_MESSAGE_BYTES,
            $args['grpc.max_receive_message_length'],
        );
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

    public function testGrpcKeepaliveWithoutCallsDefaultsToTrue(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379']);

        $this->assertTrue($this->grpcBoolProperty($grpc, 'keepalivePermitWithoutCalls'));
    }

    public function testGrpcCompressionAndWindowOptionsThreadedThroughToGrpcClient(): void
    {
        $grpc = $this->grpcFromFactory(['127.0.0.1:2379'], [
            'grpc' => [
                'keepalivePermitWithoutCalls' => false,
                'compression' => GrpcClient::COMPRESSION_GZIP,
                'initialWindowSizeBytes' => 1 << 27,
            ],
        ]);

        $this->assertFalse($this->grpcBoolProperty($grpc, 'keepalivePermitWithoutCalls'));
        $this->assertSame(GrpcClient::COMPRESSION_GZIP, $this->grpcStringProperty($grpc, 'compression'));
        $this->assertSame(1 << 27, $this->grpcProperty($grpc, 'initialWindowSizeBytes'));
    }

    public function testGrpcOptionsRejectNonBoolKeepaliveWithoutCalls(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['grpc'][keepalivePermitWithoutCalls] must be a bool, got int",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpc' => ['keepalivePermitWithoutCalls' => 1]],
        );
    }

    public function testGrpcOptionsRejectUnknownCompressionAlgorithm(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['grpc'][compression] must be one of 'none', 'gzip', 'deflate', got 'snappy'",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpc' => ['compression' => 'snappy']],
        );
    }

    public function testGrpcOptionsRejectNegativeInitialWindowSize(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['grpc'][initialWindowSizeBytes] must be an int >= 0",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpc' => ['initialWindowSizeBytes' => -1]],
        );
    }

    public function testGrpcOptionsRejectNonIntInitialWindowSize(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['grpc'][initialWindowSizeBytes] must be an int >= 0, got string",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpc' => ['initialWindowSizeBytes' => '1024']],
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
    // options['expectedClusterId'] — cluster identity verification (issue #308)
    // ========================================================================

    public function testExpectedClusterIdDefaultsToNull(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);
        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);

        /** @var string|null $expected */
        $expected = (new \ReflectionProperty(PdClient::class, 'expectedClusterId'))->getValue($pdClient);
        $this->assertNull($expected);
    }

    public function testExpectedClusterIdIsThreadedThroughToPdClient(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => 12345],
        );
        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);

        /** @var string|null $expected */
        $expected = (new \ReflectionProperty(PdClient::class, 'expectedClusterId'))->getValue($pdClient);
        $this->assertSame('12345', $expected);
    }

    public function testExpectedClusterIdIsThreadedThroughToTimestampOracle(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => 12345],
        );
        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);

        $oracle = (new \ReflectionMethod(PdClient::class, 'oracle'))->invoke($pdClient);
        $this->assertInstanceOf(TimestampOracle::class, $oracle);

        /** @var string|null $expected */
        $expected = (new \ReflectionProperty(TimestampOracle::class, 'expectedClusterId'))->getValue($oracle);
        $this->assertSame('12345', $expected);
    }

    public function testExpectedClusterIdAcceptsCanonicalDecimalStringAbovePhpIntMax(): void
    {
        // PD cluster IDs are uint64; a canonical decimal string must survive
        // the factory unchanged instead of being rejected as a non-int
        // (issue #818).
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => '18446744073709551615'],
        );
        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);

        /** @var string|null $expected */
        $expected = (new \ReflectionProperty(PdClient::class, 'expectedClusterId'))->getValue($pdClient);
        $this->assertSame('18446744073709551615', $expected);
    }

    public function testExpectedClusterIdNormalisesLeadingZeros(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => '00012345'],
        );
        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);

        /** @var string|null $expected */
        $expected = (new \ReflectionProperty(PdClient::class, 'expectedClusterId'))->getValue($pdClient);
        $this->assertSame('12345', $expected);
    }

    public function testExpectedClusterIdRejectsNonNumericString(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['expectedClusterId'] must be a decimal string (uint64 cluster ID)");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => '12ab'],
        );
    }

    public function testExpectedClusterIdRejectsNonIntNonStringValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['expectedClusterId'] must be an int or a decimal string (uint64 cluster ID), bool given",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => true],
        );
    }

    public function testExpectedClusterIdRejectsValueAboveUint64(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "options['expectedClusterId'] must not exceed 18446744073709551615 (uint64)",
        );

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => '18446744073709551616'],
        );
    }

    public function testExpectedClusterIdRejectsZero(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['expectedClusterId'] must be greater than 0, got 0");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => 0],
        );
    }

    public function testExpectedClusterIdRejectsNegative(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['expectedClusterId'] must be greater than 0, got -1");

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => -1],
        );
    }

    public function testExplicitNullExpectedClusterIdIsTreatedAsAbsent(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['expectedClusterId' => null],
        );
        $pdClient = $bundle->pdClient;
        $this->assertInstanceOf(PdClient::class, $pdClient);

        /** @var int|null $expected */
        $expected = (new \ReflectionProperty(PdClient::class, 'expectedClusterId'))->getValue($pdClient);
        $this->assertNull($expected);
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
     * @return array<string, array{string}>
     */
    public static function timeoutFieldProvider(): array
    {
        $fields = [];
        foreach ((new \ReflectionMethod(TimeoutConfig::class, '__construct'))->getParameters() as $parameter) {
            $fields[$parameter->getName()] = [$parameter->getName()];
        }

        return $fields;
    }

    /**
     * A numeric string — the shape an environment variable produces — is a
     * type error, not a value to coerce or silently ignore (issue #339,
     * TEST-19). Every timeout field fails the same way.
     */
    #[DataProvider('timeoutFieldProvider')]
    public function testNumericStringTimeoutIsRejectedNotSilentlyIgnored(string $field): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(sprintf(
            "options['timeout'][%s] must be an int (milliseconds), string given",
            $field,
        ));

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['timeout' => [$field => '5000']],
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

    /**
     * A threshold of `0` is the documented "disable slow logging for this
     * operation" value, unlike a zero timeout, which is a configuration
     * error.
     */
    public function testZeroSlowLogThresholdDisablesLoggingForThatOperation(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['slowLog' => ['readThresholdMs' => 0]],
        );

        $this->assertNotNull($bundle->slowLogConfig);
        $this->assertSame(0, $bundle->slowLogConfig->readThresholdMs);
        $this->assertSame(0, $bundle->slowLogConfig->getThreshold(SlowLogOperation::Read));
        // The other operations keep their own defaults.
        $this->assertSame(200, $bundle->slowLogConfig->writeThresholdMs);
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

    /**
     * @return array<string, int|string|bool>
     */
    private function channelArgsOf(GrpcClient $grpc): array
    {
        /** @var array<string, int|string|bool> */
        return (new \ReflectionMethod(GrpcClient::class, 'channelArgs'))->invoke($grpc);
    }

    private function grpcBoolProperty(GrpcClient $grpc, string $property): bool
    {
        /** @var bool */
        return (new \ReflectionProperty(GrpcClient::class, $property))->getValue($grpc);
    }

    private function grpcStringProperty(GrpcClient $grpc, string $property): string
    {
        /** @var string */
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

    /**
     * `'tls' => null` means "no TLS configuration" everywhere else
     * ({@see ConnectionFactory::buildTlsConfig()}), so it is not a transport
     * option that conflicts with a supplied transport (issue #695).
     */
    public function testSuppliedGrpcClientAcceptsExplicitNullTlsOption(): void
    {
        $grpc = new GrpcClient();

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => $grpc, 'tls' => null],
        );

        $this->assertSame($grpc, $bundle->grpc);
        $this->assertFalse($bundle->ownsGrpcClient);
    }

    /**
     * A supplied transport may well use TLS, so the bundle must not report
     * its state as plaintext (issue #695).
     */
    public function testSuppliedGrpcClientReportsUnknownTlsState(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['grpcClient' => new GrpcClient()],
        );

        $this->assertSame(TlsState::Unknown, $bundle->tlsState);
        $this->assertNull($bundle->tlsConfig);
    }

    public function testFactoryBuiltPlaintextTransportReportsPlaintextTlsState(): void
    {
        $bundle = ConnectionFactory::create(['127.0.0.1:2379']);

        $this->assertSame(TlsState::Plaintext, $bundle->tlsState);
        $this->assertNull($bundle->tlsConfig);
    }

    public function testFactoryBuiltTlsTransportReportsTlsState(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['caCertPem' => self::CA_PEM]],
        );

        $this->assertSame(TlsState::Tls, $bundle->tlsState);
        $this->assertNotNull($bundle->tlsConfig);
    }

    /**
     * `'tls' => []` and a base-dir-only config build a `TlsConfig` with no
     * material, and `GrpcClient::createChannel()` opens an insecure channel
     * for it. Reporting `TlsState::Tls` on object presence alone would be
     * the same fail-open signal #304 removed (issue #695).
     */
    public function testEmptyTlsConfigReportsPlaintextTlsState(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => []],
        );

        $this->assertSame(TlsState::Plaintext, $bundle->tlsState);
        $this->assertNotNull($bundle->tlsConfig);
        $this->assertFalse($bundle->tlsConfig->isEnabled());
    }

    public function testBaseDirOnlyTlsConfigReportsPlaintextTlsState(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['caCertBaseDir' => $this->tempDir]],
        );

        $this->assertSame(TlsState::Plaintext, $bundle->tlsState);
        $this->assertNotNull($bundle->tlsConfig);
        $this->assertFalse($bundle->tlsConfig->isEnabled());
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

    // ========================================================================
    // options['tls'] — parsing and the documented precedence rules
    // (issue #339, TEST-19)
    // ========================================================================

    private const CA_PEM = "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----";

    private const CLIENT_CERT_PEM = "-----BEGIN CERTIFICATE-----\nclient-cert\n-----END CERTIFICATE-----";

    private const CLIENT_KEY_PEM = "-----BEGIN PRIVATE KEY-----\nclient-key\n-----END PRIVATE KEY-----";

    /**
     * An explicit file path wins over inline PEM content when both are
     * given (documented precedence in buildTlsConfig()).
     */
    public function testTlsFilePathTakesPriorityOverPem(): void
    {
        $filePem = "-----BEGIN CERTIFICATE-----\nfrom-file\n-----END CERTIFICATE-----";
        $caPath = $this->writeTlsFile('ca.crt', $filePem);

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCertFile' => $caPath,
                'caCertPem' => self::CA_PEM,
            ]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame($filePem, $bundle->tlsConfig->caCert);
    }

    /**
     * The same precedence applies to the client certificate pair: a
     * complete file pair wins over a complete PEM pair.
     */
    public function testClientCertFilePathTakesPriorityOverPem(): void
    {
        $caPath = $this->writeTlsFile('ca.crt', self::CA_PEM);
        $certPath = $this->writeTlsFile('client.crt', self::CLIENT_CERT_PEM);
        $keyPath = $this->writeTlsFile('client.key', self::CLIENT_KEY_PEM);

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCertFile' => $caPath,
                'clientCertFile' => $certPath,
                'clientKeyFile' => $keyPath,
                'clientCertPem' => "-----BEGIN CERTIFICATE-----\ninline-cert\n-----END CERTIFICATE-----",
                'clientKeyPem' => "-----BEGIN PRIVATE KEY-----\ninline-key\n-----END PRIVATE KEY-----",
            ]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CLIENT_CERT_PEM, $bundle->tlsConfig->clientCert);
        $this->assertSame(self::CLIENT_KEY_PEM, $bundle->tlsConfig->clientKey);
    }

    public function testCaCertPemIsUsedWhenNoFilePathIsGiven(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['caCertPem' => self::CA_PEM]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CA_PEM, $bundle->tlsConfig->caCert);
    }

    public function testCaCertFileIsRead(): void
    {
        $caPath = $this->writeTlsFile('ca.crt', self::CA_PEM);

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['caCertFile' => $caPath]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CA_PEM, $bundle->tlsConfig->caCert);
    }

    /**
     * The base directory is actually forwarded: a CA file outside it is
     * rejected. Without the forwarding this test would build a TlsConfig.
     */
    public function testCaCertBaseDirRejectsAFileOutsideTheAllowedDirectory(): void
    {
        $caPath = $this->writeTlsFile('ca.crt', self::CA_PEM);
        $allowedDir = $this->tempDir . '/allowed';
        mkdir($allowedDir);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TLS file path is outside the allowed base directory');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCertFile' => $caPath,
                'caCertBaseDir' => $allowedDir,
            ]],
        );
    }

    /**
     * Same for the client certificate pair: clientCertBaseDir is forwarded
     * and restricts both files it covers.
     */
    public function testClientCertBaseDirRejectsAFileOutsideTheAllowedDirectory(): void
    {
        $caPath = $this->writeTlsFile('ca.crt', self::CA_PEM);
        $certPath = $this->writeTlsFile('client.crt', self::CLIENT_CERT_PEM);
        $keyPath = $this->writeTlsFile('client.key', self::CLIENT_KEY_PEM);
        $allowedDir = $this->tempDir . '/allowed';
        mkdir($allowedDir);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TLS file path is outside the allowed base directory');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCertFile' => $caPath,
                'clientCertFile' => $certPath,
                'clientKeyFile' => $keyPath,
                'clientCertBaseDir' => $allowedDir,
            ]],
        );
    }

    /**
     * The deprecated `caCert` key guesses file vs inline content; inline
     * content is the documented use.
     */
    public function testLegacyCaCertOptionAcceptsInlinePem(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => ['caCert' => self::CA_PEM]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CA_PEM, $bundle->tlsConfig->caCert);
    }

    public function testClientCertFilePairIsApplied(): void
    {
        $caPath = $this->writeTlsFile('ca.crt', self::CA_PEM);
        $certPath = $this->writeTlsFile('client.crt', self::CLIENT_CERT_PEM);
        $keyPath = $this->writeTlsFile('client.key', self::CLIENT_KEY_PEM);

        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCertFile' => $caPath,
                'clientCertFile' => $certPath,
                'clientKeyFile' => $keyPath,
                'clientCertBaseDir' => $this->tempDir,
            ]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CLIENT_CERT_PEM, $bundle->tlsConfig->clientCert);
        $this->assertSame(self::CLIENT_KEY_PEM, $bundle->tlsConfig->clientKey);
    }

    public function testClientCertPemPairIsApplied(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCertPem' => self::CA_PEM,
                'clientCertPem' => self::CLIENT_CERT_PEM,
                'clientKeyPem' => self::CLIENT_KEY_PEM,
            ]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CLIENT_CERT_PEM, $bundle->tlsConfig->clientCert);
        $this->assertSame(self::CLIENT_KEY_PEM, $bundle->tlsConfig->clientKey);
    }

    public function testLegacyClientCertPairAcceptsInlinePem(): void
    {
        $bundle = ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCert' => self::CA_PEM,
                'clientCert' => self::CLIENT_CERT_PEM,
                'clientKey' => self::CLIENT_KEY_PEM,
            ]],
        );

        $this->assertNotNull($bundle->tlsConfig);
        $this->assertSame(self::CLIENT_CERT_PEM, $bundle->tlsConfig->clientCert);
        $this->assertSame(self::CLIENT_KEY_PEM, $bundle->tlsConfig->clientKey);
    }

    public function testLegacyMixedClientPairThrowsOneMessage(): void
    {
        $certPath = $this->writeTlsFile('client.crt', self::CLIENT_CERT_PEM);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TLS client certificate and key must be given in the same form');

        ConnectionFactory::create(
            ['127.0.0.1:2379'],
            options: ['tls' => [
                'caCert' => self::CA_PEM,
                'clientCert' => $certPath,
                'clientKey' => self::CLIENT_KEY_PEM,
            ]],
        );
    }

    private function writeTlsFile(string $name, string $content): string
    {
        $path = $this->tempDir . '/' . $name;
        file_put_contents($path, $content);

        return $path;
    }
}
