<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Grpc\GrpcResponseParser;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class GrpcResponseParserTest extends TestCase
{
    /**
     * @return array<string, array{event: mixed, expected: array{code: int, details: string}}>
     */
    public static function extractStatusDataProvider(): array
    {
        return [
            'status OK from array' => [
                'event' => ['status' => ['code' => 0, 'details' => 'OK']],
                'expected' => ['code' => 0, 'details' => 'OK'],
            ],
            'status error from array' => [
                'event' => ['status' => ['code' => 2, 'details' => 'Unavailable']],
                'expected' => ['code' => 2, 'details' => 'Unavailable'],
            ],
            'status as object' => [
                'event' => (object) ['status' => (object) ['code' => 5, 'details' => 'Not found']],
                'expected' => ['code' => 5, 'details' => 'Not found'],
            ],
            'status without details defaults to empty string' => [
                'event' => ['status' => ['code' => 3]],
                'expected' => ['code' => 3, 'details' => ''],
            ],
            'details as int is cast to string' => [
                'event' => ['status' => ['code' => 1, 'details' => 42]],
                'expected' => ['code' => 1, 'details' => '42'],
            ],
            'event as object with object status' => [
                'event' => (object) [
                    'status' => (object) ['code' => 10, 'details' => 'Aborted'],
                    'message' => 'some data',
                ],
                'expected' => ['code' => 10, 'details' => 'Aborted'],
            ],
        ];
    }

    /**
     * @param array{code: int, details: string} $expected
     */
    #[DataProvider('extractStatusDataProvider')]
    public function testExtractStatus(mixed $event, array $expected): void
    {
        $result = GrpcResponseParser::extractStatus($event);
        $this->assertSame($expected['code'], $result['code']);
        $this->assertSame($expected['details'], $result['details']);
    }

    public function testExtractStatusPreservesOtherEventKeys(): void
    {
        $event = [
            'status' => ['code' => 0, 'details' => 'OK'],
            'message' => 'some data',
        ];
        $result = GrpcResponseParser::extractStatus($event);
        $this->assertSame(0, $result['code']);
        $this->assertSame('OK', $result['details']);
    }

    public function testDeserializeWithValidMessage(): void
    {
        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('test-key');
        $serialized = $request->serializeToString();

        $event = ['message' => $serialized];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

        $this->assertInstanceOf(Message::class, $result);
        $this->assertSame('test-key', $result->getKey());
    }

    public function testDeserializeWithNullMessageThrowsWhenRequired(): void
    {
        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('no response body');

        GrpcResponseParser::deserialize(['message' => null], \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class);
    }

    public function testDeserializeWithNullMessageWhenNotRequired(): void
    {
        $result = GrpcResponseParser::deserialize(
            ['message' => null],
            \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class,
            requireMessage: false,
        );

        $this->assertInstanceOf(Message::class, $result);
    }

    public function testDeserializeWithEmptyStringMessage(): void
    {
        $event = ['message' => ''];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

        $this->assertInstanceOf(Message::class, $result);
    }

    public function testDeserializeWithObjectEvent(): void
    {
        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('obj-key');
        $serialized = $request->serializeToString();

        $event = (object) ['message' => $serialized];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

        $this->assertInstanceOf(Message::class, $result);
        $this->assertSame('obj-key', $result->getKey());
    }

    public function testDeserializeWithMissingMessageThrowsWhenRequired(): void
    {
        $this->expectException(GrpcException::class);

        GrpcResponseParser::deserialize(['status' => ['code' => 0]], \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class);
    }

    public function testDeserializeWithMissingMessageWhenNotRequired(): void
    {
        $result = GrpcResponseParser::deserialize(
            ['status' => ['code' => 0]],
            \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class,
            requireMessage: false,
        );

        $this->assertInstanceOf(Message::class, $result);
    }

    public function testExtractStatusThrowsWhenStatusIsMissing(): void
    {
        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('no status');

        GrpcResponseParser::extractStatus(['message' => 'x']);
    }

    public function testExtractStatusThrowsWhenStatusIsNull(): void
    {
        $this->expectException(GrpcException::class);

        GrpcResponseParser::extractStatus(['status' => null]);
    }

    public function testExtractStatusThrowsWhenCodeIsMissing(): void
    {
        $this->expectException(GrpcException::class);

        GrpcResponseParser::extractStatus(['status' => ['details' => 'msg']]);
    }

    /**
     * @return array<string, array{code: mixed}>
     */
    public static function nonIntCodeProvider(): array
    {
        return [
            'string' => ['code' => '4'],
            'array' => ['code' => ['nested']],
            'float' => ['code' => 1.5],
            'null' => ['code' => null],
        ];
    }

    #[DataProvider('nonIntCodeProvider')]
    public function testExtractStatusThrowsWhenCodeIsNotAnInteger(mixed $code): void
    {
        $this->expectException(GrpcException::class);
        $this->expectExceptionMessage('unexpected type');

        GrpcResponseParser::extractStatus(['status' => ['code' => $code, 'details' => 'x']]);
    }

    public function testDeserializeDifferentResponseTypes(): void
    {
        $response = new \CrazyGoat\Proto\Kvrpcpb\RawGetResponse();
        $response->setValue('value-data');
        $serialized = $response->serializeToString();

        $event = ['message' => $serialized];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class);

        $this->assertInstanceOf(\CrazyGoat\Proto\Kvrpcpb\RawGetResponse::class, $result);
        $this->assertSame('value-data', $result->getValue());
    }

    public function testMaxMessageSizeAcceptsMessageWithinLimit(): void
    {
        GrpcResponseParser::setMaxMessageSize(1024);

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('within-limit');
        $serialized = $request->serializeToString();

        $event = ['message' => $serialized];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

        $this->assertSame('within-limit', $result->getKey());
    }

    public function testMaxMessageSizeRejectsOversizedMessage(): void
    {
        GrpcResponseParser::setMaxMessageSize(1);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('exceeds maximum allowed');

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('too-large-key');
        $serialized = $request->serializeToString();

        $event = ['message' => $serialized];
        GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);
    }

    public function testMaxMessageSizeDisabledByDefault(): void
    {
        $this->assertSame(0, GrpcResponseParser::getMaxMessageSize());
    }

    public function testMaxMessageSizeWithNullMessageDoesNotThrow(): void
    {
        GrpcResponseParser::setMaxMessageSize(1);

        $event = ['message' => null];
        $result = GrpcResponseParser::deserialize(
            $event,
            \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class,
            requireMessage: false,
        );

        $this->assertInstanceOf(Message::class, $result);
    }

    public function testMaxMessageSizeZeroDisablesLimit(): void
    {
        GrpcResponseParser::setMaxMessageSize(0);

        $request = new \CrazyGoat\Proto\Kvrpcpb\RawGetRequest();
        $request->setKey('any-size');
        $serialized = $request->serializeToString();

        $event = ['message' => $serialized];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

        $this->assertSame('any-size', $result->getKey());
    }

    protected function tearDown(): void
    {
        GrpcResponseParser::setMaxMessageSize(0);
    }
}
