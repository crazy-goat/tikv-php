<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\KeyNotInRegion;
use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\KvPair;
use CrazyGoat\Proto\Kvrpcpb\Mutation;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetRequest;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutRequest;
use CrazyGoat\Proto\Kvrpcpb\RawScanRequest;
use CrazyGoat\Proto\Kvrpcpb\ScanResponse;
use CrazyGoat\Proto\Tikvpb\BatchCommandsRequest;
use CrazyGoat\Proto\Tikvpb\BatchCommandsResponse;
use CrazyGoat\TiKV\Client\Codec\CodecV2;
use CrazyGoat\TiKV\Client\Codec\Mode;
use CrazyGoat\TiKV\Client\Exception\KeyOutOfBoundsException;
use CrazyGoat\TiKV\Client\Grpc\ApiV2MessageTransformer;
use PHPUnit\Framework\TestCase;

final class ApiV2MessageTransformerTest extends TestCase
{
    public function testRawRequestEncodesKeyContextAndForcesDefaultCf(): void
    {
        $codec = new CodecV2(Mode::Raw, 0x010203, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Raw);
        $request = new RawPutRequest();
        $request->setKey("\x00\xFFkey");
        $request->setCf('custom');

        $encoded = $transformer->transformRequest($request, 'tikvpb.Tikv');

        $this->assertNotSame($request, $encoded);
        $this->assertSame("\x00\xFFkey", $request->getKey());
        $this->assertSame($codec->encodeKey("\x00\xFFkey"), $encoded->getKey());
        $this->assertSame('', $encoded->getCf());
        $context = $encoded->getContext();
        $this->assertInstanceOf(Context::class, $context);
        $this->assertSame(2, $context->getApiVersion());
        $this->assertSame('tenant-a', $context->getKeyspaceName());
        $this->assertSame(0x010203, $context->getKeyspaceId());
    }

    public function testTransactionNestedKeysAreEncoded(): void
    {
        $codec = new CodecV2(Mode::Txn, 42, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Txn);
        $mutation = new Mutation();
        $mutation->setKey("\x00\xFF");
        $request = new PrewriteRequest();
        $request->setMutations([$mutation]);
        $request->setPrimaryLock('primary');

        $encoded = $transformer->transformRequest($request, 'tikvpb.Tikv');

        $mutations = $encoded->getMutations();
        $this->assertInstanceOf(Mutation::class, $mutations[0]);
        $this->assertSame($codec->encodeKey("\x00\xFF"), $mutations[0]->getKey());
        $this->assertSame($codec->encodeKey('primary'), $encoded->getPrimaryLock());
        $context = $encoded->getContext();
        $this->assertInstanceOf(Context::class, $context);
        $this->assertSame(2, $context->getApiVersion());
    }

    public function testRequestRangesUseKeyspaceSentinelAndReverseBounds(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Raw);
        $request = new RawScanRequest();
        $request->setStartKey('a');
        $request->setEndKey('');

        $encoded = $transformer->transformRequest($request, 'tikvpb.Tikv');

        $this->assertSame($codec->encodeKey('a'), $encoded->getStartKey());
        $this->assertSame($codec->encodeRequestRange('a', '')[1], $encoded->getEndKey());
        $this->assertNotSame('', $encoded->getEndKey());

        $reverse = new RawScanRequest();
        $reverse->setStartKey('z');
        $reverse->setEndKey('a');
        $reverse->setReverse(true);
        $encodedReverse = $transformer->transformRequest($reverse, 'tikvpb.Tikv');
        $this->assertSame($codec->encodeKey('a'), $encodedReverse->getStartKey());
        $this->assertSame($codec->encodeKey('z'), $encodedReverse->getEndKey());
    }

    public function testResponseDecodesPairsAndRegionBoundaries(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Raw);
        $pair = new KvPair();
        $pair->setKey($codec->encodeKey("\x00\xFF"));
        $response = new ScanResponse();
        $response->setPairs([$pair]);

        $regionError = new Error();
        $notInRegion = new KeyNotInRegion();
        $notInRegion->setKey($codec->encodeKey('key'));
        $notInRegion->setStartKey($codec->encodeRegionKey('start'));
        $notInRegion->setEndKey($codec->encodeRegionKey('end'));
        $regionError->setKeyNotInRegion($notInRegion);
        $rawResponse = new RawGetResponse();
        $rawResponse->setRegionError($regionError);

        $decoded = $transformer->transformResponse($response, 'tikvpb.Tikv');
        $this->assertSame("\x00\xFF", $decoded->getPairs()[0]->getKey());

        $decodedError = $transformer->transformResponse($rawResponse, 'tikvpb.Tikv');
        $decodedRegionError = $decodedError->getRegionError();
        $this->assertInstanceOf(Error::class, $decodedRegionError);
        $decodedNotInRegion = $decodedRegionError->getKeyNotInRegion();
        $this->assertInstanceOf(KeyNotInRegion::class, $decodedNotInRegion);
        $this->assertSame('key', $decodedNotInRegion->getKey());
        $this->assertSame('start', $decodedNotInRegion->getStartKey());
        $this->assertSame('end', $decodedNotInRegion->getEndKey());
    }

    public function testOutOfKeyspaceResponseRaisesTypedException(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Raw);
        $other = new CodecV2(Mode::Raw, 43, 'tenant-b');
        $pair = new KvPair();
        $pair->setKey($other->encodeKey('secret'));
        $response = new ScanResponse();
        $response->setPairs([$pair]);

        $this->expectException(KeyOutOfBoundsException::class);
        $transformer->transformResponse($response, 'tikvpb.Tikv');
    }

    public function testBatchCommandsOneofMessagesAreTranslated(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Raw);

        $inner = new RawGetRequest();
        $inner->setKey('key');
        $wrapper = new BatchCommandsRequest();
        $wrapper->setRequests([(new BatchCommandsRequest\Request())->setRawGet($inner)]);
        $encoded = $transformer->transformRequest($wrapper, 'tikvpb.BatchCommands');

        /** @var BatchCommandsRequest\Request $encodedInner */
        $encodedInner = $encoded->getRequests()[0];
        $encodedGet = $encodedInner->getRawGet();
        $this->assertInstanceOf(RawGetRequest::class, $encodedGet);
        $this->assertSame($codec->encodeKey('key'), $encodedGet->getKey());
        // The caller's own message must stay untouched for retry.
        $this->assertSame('key', $inner->getKey());

        $responseInner = new RawGetResponse();
        $responseInner->setValue('value');
        $response = new BatchCommandsResponse();
        $response->setResponses([
            (new BatchCommandsResponse\Response())->setRawGet($responseInner),
        ]);
        $decoded = $transformer->transformResponse($response, 'tikvpb.BatchCommands');
        $decodedGet = $decoded->getResponses()[0]->getRawGet();
        $this->assertInstanceOf(RawGetResponse::class, $decodedGet);
        $this->assertSame('value', $decodedGet->getValue());
    }

    public function testPdMessagesAreNotTransformed(): void
    {
        $codec = new CodecV2(Mode::Raw, 42, 'tenant-a');
        $transformer = new ApiV2MessageTransformer($codec, Mode::Raw);
        $request = new RawPutRequest();
        $request->setKey('key');

        $this->assertSame($request, $transformer->transformRequest($request, 'pdpb.PD'));
        $this->assertSame($request, $transformer->transformResponse($request, 'pdpb.PD'));
    }
}
