<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\TxnKv;

use CrazyGoat\Proto\Metapb\Store;
use CrazyGoat\TiKV\Client\Connection\PdClientInterface;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;
use CrazyGoat\TiKV\Client\Exception\TiKvException;
use CrazyGoat\TiKV\Client\Grpc\GrpcClientInterface;
use CrazyGoat\TiKV\Client\Region\Dto\RegionInfo;
use CrazyGoat\TiKV\Client\Retry\RetryVerdict;
use CrazyGoat\TiKV\Client\TxnKv\TxnKvClient;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * Issue #719: TxnKvClient hands its retry classifier to every transaction,
 * through the constructor and through `options['retryClassifier']`.
 */
class TxnKvClientRetryClassifierTest extends TestCase
{
    private PdClientInterface&MockObject $pdClient;

    private GrpcClientInterface&MockObject $grpc;

    protected function setUp(): void
    {
        $store = new Store();
        $store->setId(1);
        $store->setAddress('tikv1:20160');

        $this->pdClient = $this->createMock(PdClientInterface::class);
        $this->pdClient->method('getTimestamp')->willReturn(1000);
        $this->pdClient->method('getLowResolutionTimestamp')->willReturn(1000);
        $this->pdClient->method('getRegion')->willReturn(new RegionInfo(
            regionId: 1,
            leaderPeerId: 1,
            leaderStoreId: 1,
            epochConfVer: 1,
            epochVersion: 1,
        ));
        $this->pdClient->method('getStore')->willReturn($store);

        $this->grpc = $this->createMock(GrpcClientInterface::class);
    }

    public function testConstructorClassifierReachesTheTransactionRetryLoop(): void
    {
        // DataIsNotReady is retried by the built-in classifier; the custom
        // classifier makes it fatal, so the transaction must issue one RPC
        // only instead of retrying up to maxAttempts times.
        $this->grpc->expects($this->once())
            ->method('call')
            ->willThrowException(new TiKvException('DataIsNotReady'));

        $client = new TxnKvClient(
            $this->pdClient,
            $this->grpc,
            retryClassifier: static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
        );

        $txn = $client->begin(['pessimistic' => false]);

        try {
            $this->expectException(TiKvException::class);
            $txn->get('key');
        } finally {
            // Read-only transaction: rollback closes it without an RPC.
            $txn->rollback();
        }
    }

    public function testCreateRejectsANonCallableRetryClassifier(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("options['retryClassifier'] must be callable, int given");

        TxnKvClient::create(['127.0.0.1:2379'], options: ['retryClassifier' => 42]);
    }

    public function testCreateResolvesTheClassifierOption(): void
    {
        $client = TxnKvClient::create(
            ['127.0.0.1:2379'],
            options: [
                'retryClassifier' => static fn (TiKvException $e): RetryVerdict => RetryVerdict::fatal(),
            ],
        );

        $property = new \ReflectionProperty(TxnKvClient::class, 'retryClassifier');
        $classifier = $property->getValue($client);

        self::assertInstanceOf(\Closure::class, $classifier);
        self::assertTrue($classifier(new TiKvException('anything'))->isFatal());

        $client->close();
    }

    public function testCreateDefaultsToNoClassifier(): void
    {
        $client = TxnKvClient::create(['127.0.0.1:2379']);

        $property = new \ReflectionProperty(TxnKvClient::class, 'retryClassifier');

        self::assertNull($property->getValue($client));

        $client->close();
    }
}
