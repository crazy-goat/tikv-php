<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use CrazyGoat\Proto\Errorpb\EpochNotMatch;
use CrazyGoat\Proto\Errorpb\Error;
use CrazyGoat\Proto\Errorpb\NotLeader;
use CrazyGoat\Proto\Kvrpcpb\RawGetResponse;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\Proto\Metapb\Peer;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use PHPUnit\Framework\TestCase;

class RegionErrorHandlerTest extends TestCase
{
    public function testNoExceptionWhenResponseHasNoRegionError(): void
    {
        $response = new RawPutResponse();

        RegionErrorHandler::check($response);

        $this->expectNotToPerformAssertions();
    }

    public function testNoExceptionWhenRegionErrorIsNull(): void
    {
        $response = new RawGetResponse();

        RegionErrorHandler::check($response);

        $this->expectNotToPerformAssertions();
    }

    public function testThrowsRegionExceptionOnNotLeaderWithHint(): void
    {
        $leader = new Peer();
        $leader->setId(20);
        $leader->setStoreId(3);

        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);
        $notLeader->setLeader($leader);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            $this->assertNotNull($e->notLeader);
            $this->assertNotNull($e->notLeader->getLeader());
            $this->assertSame(3, (int) $e->notLeader->getLeader()->getStoreId());
            $this->assertSame(42, (int) $e->notLeader->getRegionId());
        }
    }

    public function testThrowsRegionExceptionOnNotLeaderWithoutHint(): void
    {
        $notLeader = new NotLeader();
        $notLeader->setRegionId(42);

        $error = new Error();
        $error->setMessage('not leader');
        $error->setNotLeader($notLeader);

        $response = new RawGetResponse();
        $response->setRegionError($error);

        try {
            RegionErrorHandler::check($response);
            $this->fail('Expected RegionException');
        } catch (RegionException $e) {
            $this->assertNotNull($e->notLeader);
            $this->assertNull($e->notLeader->getLeader());
        }
    }

    public function testThrowsRegionExceptionOnOtherRegionError(): void
    {
        $error = new Error();
        $error->setMessage('epoch not match');
        $error->setEpochNotMatch(new EpochNotMatch());

        $response = new RawGetResponse();
        $response->setRegionError($error);

        $this->expectException(RegionException::class);
        $this->expectExceptionMessage('RegionError failed: epoch not match');

        RegionErrorHandler::check($response);
    }

    public function testNoExceptionForObjectWithoutGetRegionErrorMethod(): void
    {
        $response = new \stdClass();

        RegionErrorHandler::check($response);

        $this->expectNotToPerformAssertions();
    }
}
