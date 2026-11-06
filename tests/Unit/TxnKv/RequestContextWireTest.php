<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Kvrpcpb\CommitRequest;
use CrazyGoat\Proto\Kvrpcpb\CommitResponse;
use CrazyGoat\Proto\Kvrpcpb\GetRequest;
use CrazyGoat\Proto\Kvrpcpb\GetResponse;
use CrazyGoat\Proto\Kvrpcpb\PrewriteRequest;
use CrazyGoat\Proto\Kvrpcpb\PrewriteResponse;
use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Region\RequestContextOptions;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\TestCase;

/**
 * Issue #426: the optional Context control fields reach the wire on the
 * transactional RPCs, and a begin() option overrides the client-level value.
 */
class RequestContextWireTest extends TestCase
{
    use MockGrpcCallAsyncShim;

    /**
     * @param array{pessimistic?: bool, requestContext?: RequestContextOptions|null} $beginOptions
     * @return array<string, GetRequest|PrewriteRequest|CommitRequest>
     */
    private function runTransaction(
        ?RequestContextOptions $clientRequestContext,
        array $beginOptions = [],
    ): array {
        $pdClient = $this->createMock(PdClientInterface::class);
        $pdClient->method('getTimestamp')->willReturn(4000);
        $pdClient->method('getClusterId')->willReturn(null);
        $pdClient->method('getRegion')->willReturn($this->testRegion());
        $pdClient->method('getStore')->willReturn($this->testStore());

        /** @var array<string, GetRequest|PrewriteRequest|CommitRequest> $capturedRequests */
        $capturedRequests = [];
        $grpc = $this->createMock(GrpcClientInterface::class);
        $this->shimCallAsync($grpc);
        $grpc->method('call')->willReturnCallback(
            function (string $addr, string $service, string $method, mixed $request) use (&$capturedRequests): object {
                if (
                    $request instanceof GetRequest
                    || $request instanceof PrewriteRequest
                    || $request instanceof CommitRequest
                ) {
                    $capturedRequests[$method] = $request;
                }

                return match ($method) {
                    'KvGet' => new GetResponse(),
                    'KvPrewrite' => new PrewriteResponse(),
                    'KvCommit' => new CommitResponse(),
                    default => throw new \RuntimeException("Unexpected method: $method"),
                };
            },
        );

        $client = new TxnKvClient($pdClient, $grpc, requestContext: $clientRequestContext);
        $beginOptions['pessimistic'] = false;
        $txn = $client->begin($beginOptions);
        $txn->get('k1');
        $txn->set('k1', 'v1');
        $txn->commit();

        return $capturedRequests;
    }

    public function testClientLevelOptionsReachGetPrewriteAndCommit(): void
    {
        $captured = $this->runTransaction(new RequestContextOptions(
            notFillCache: true,
            maxExecutionDurationMs: 2500,
            requestSource: 'tikv-php-test',
        ));

        foreach (['KvGet', 'KvPrewrite', 'KvCommit'] as $method) {
            $context = $captured[$method]->getContext();
            $this->assertNotNull($context, "missing Context on $method");
            $this->assertTrue($context->getNotFillCache(), "not_fill_cache missing on $method");
            $this->assertSame(
                2500,
                $context->getMaxExecutionDurationMs(),
                "max_execution_duration_ms missing on $method",
            );
            $this->assertSame('tikv-php-test', $context->getRequestSource(), "request_source missing on $method");
        }
    }

    public function testTransactionLevelOptionOverridesTheClientLevelOne(): void
    {
        $captured = $this->runTransaction(
            new RequestContextOptions(notFillCache: true, requestSource: 'client'),
            ['requestContext' => new RequestContextOptions(maxExecutionDurationMs: 100, requestSource: 'txn')],
        );

        foreach (['KvGet', 'KvPrewrite', 'KvCommit'] as $method) {
            $context = $captured[$method]->getContext();
            $this->assertNotNull($context);
            $this->assertFalse($context->getNotFillCache(), "client-level not_fill_cache leaked into $method");
            $this->assertSame(
                'txn',
                $context->getRequestSource(),
                "transaction-level request_source missing on $method",
            );
            $this->assertSame(100, $context->getMaxExecutionDurationMs());
        }
    }

    public function testNullBeginOptionDisablesTheClientLevelControls(): void
    {
        $captured = $this->runTransaction(
            new RequestContextOptions(notFillCache: true, requestSource: 'client'),
            ['requestContext' => null],
        );

        $context = $captured['KvGet']->getContext();
        $this->assertNotNull($context);
        $this->assertFalse($context->getNotFillCache());
        $this->assertSame('', $context->getRequestSource());
    }

    public function testBeginRejectsANonOptionsRequestContext(): void
    {
        $client = new TxnKvClient(
            $this->createMock(PdClientInterface::class),
            $this->createMock(GrpcClientInterface::class),
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("begin() option 'requestContext'");
        $client->begin(['requestContext' => 'tikv-php']); // @phpstan-ignore argument.type
    }

    private function testRegion(): RegionInfo
    {
        return new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
            startKey: '',
            endKey: '',
            peers: [],
        );
    }

    private function testStore(): Store
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('127.0.0.1:20160');

        return $store;
    }
}
