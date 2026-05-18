<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\GrpcResponseParser;
use Google\Protobuf\Internal\Message;
use PHPUnit\Framework\TestCase;

class GrpcResponseParserTest extends TestCase
{
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
            'missing status defaults to zero' => [
                'event' => ['message' => 'data'],
                'expected' => ['code' => 0, 'details' => ''],
            ],
            'null status becomes empty array' => [
                'event' => ['status' => null],
                'expected' => ['code' => 0, 'details' => ''],
            ],
            'status without code defaults to zero' => [
                'event' => ['status' => ['details' => 'msg']],
                'expected' => ['code' => 0, 'details' => 'msg'],
            ],
            'status without details defaults to empty string' => [
                'event' => ['status' => ['code' => 3]],
                'expected' => ['code' => 3, 'details' => ''],
            ],
            'code as string is cast to int' => [
                'event' => ['status' => ['code' => '4', 'details' => 'test']],
                'expected' => ['code' => 4, 'details' => 'test'],
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
            'non-array code defaults to zero' => [
                'event' => ['status' => ['code' => ['nested'], 'details' => 'x']],
                'expected' => ['code' => 0, 'details' => 'x'],
            ],
        ];
    }

    /** @dataProvider extractStatusDataProvider */
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

    public function testDeserializeWithNullMessage(): void
    {
        $event = ['message' => null];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

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

    public function testDeserializeWithMissingMessage(): void
    {
        $event = ['status' => ['code' => 0]];
        $result = GrpcResponseParser::deserialize($event, \CrazyGoat\Proto\Kvrpcpb\RawGetRequest::class);

        $this->assertInstanceOf(Message::class, $result);
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
}
