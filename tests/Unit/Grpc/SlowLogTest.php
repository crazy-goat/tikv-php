<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Grpc;

use CrazyGoat\TiKV\Client\Grpc\SlowLog;
use CrazyGoat\TiKV\Client\Grpc\SlowLogConfig;
use CrazyGoat\TiKV\Client\Grpc\SlowLogOperation;
use CrazyGoat\TiKV\Client\Util\KeyRedactor;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use Monolog\LogRecord;
use PHPUnit\Framework\TestCase;

/**
 * The slow-log helper is the single place the warning is timed and written
 * (issue #701): the four `measure()` copies and the batch `wrapWithLogging()`
 * now share this message and this context.
 */
class SlowLogTest extends TestCase
{
    public function testMeasureReturnsTheCallableResultAndStaysSilentBelowTheThreshold(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);

        $result = SlowLog::measure(
            new SlowLogConfig(readThresholdMs: 60_000),
            $logger,
            SlowLogOperation::Read,
            'user:1',
            static fn(): string => 'value',
        );

        self::assertSame('value', $result);
        self::assertSame([], $this->slowRecords($handler));
    }

    public function testMeasureLogsTheDocumentedWarningAboveTheThreshold(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);
        $key = 'user:1';

        SlowLog::measure(
            new SlowLogConfig(readThresholdMs: 1),
            $logger,
            SlowLogOperation::Read,
            $key,
            static function (): void {
                usleep(20_000);
            },
        );

        $records = $this->slowRecords($handler);
        self::assertCount(1, $records);

        $record = $records[0];
        self::assertSame(Level::Warning, $record->level);
        self::assertSame('Slow TiKV operation', $record->message);
        self::assertSame(
            ['operation', 'key', 'duration_ms', 'threshold_ms'],
            array_keys($record->context),
        );
        self::assertSame('read', $record->context['operation']);
        self::assertSame(KeyRedactor::redact($key), $record->context['key']);
        self::assertGreaterThan(1, $record->context['duration_ms']);
        self::assertSame(1, $record->context['threshold_ms']);
    }

    public function testMeasureLogsTheWarningWhenTheCallableThrows(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);

        $thrown = null;
        try {
            SlowLog::measure(
                new SlowLogConfig(writeThresholdMs: 1),
                $logger,
                SlowLogOperation::Write,
                'user:1',
                static function (): void {
                    usleep(20_000);
                    throw new \RuntimeException('boom');
                },
            );
        } catch (\RuntimeException $e) {
            $thrown = $e;
        }

        self::assertSame('boom', $thrown->getMessage());
        self::assertCount(1, $this->slowRecords($handler));
    }

    public function testANullConfigDisablesSlowLogging(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);

        $result = SlowLog::measure(
            null,
            $logger,
            SlowLogOperation::Read,
            'user:1',
            static fn(): string => 'value',
        );

        self::assertSame('value', $result);
        self::assertNull(SlowLog::threshold(null, SlowLogOperation::Read));
        self::assertSame([], $this->slowRecords($handler));
    }

    public function testAZeroThresholdDisablesThatOperation(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);
        $config = new SlowLogConfig(readThresholdMs: 0);

        self::assertNull(SlowLog::threshold($config, SlowLogOperation::Read));
        self::assertSame(500, SlowLog::threshold($config, SlowLogOperation::BatchRead));

        SlowLog::measure(
            $config,
            $logger,
            SlowLogOperation::Read,
            'user:1',
            static function (): void {
                usleep(20_000);
            },
        );

        self::assertSame([], $this->slowRecords($handler));
    }

    public function testWarningWritesTheMessageAndContextWithoutTiming(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('tikv-test', [$handler]);

        SlowLog::warning($logger, SlowLogOperation::BatchWrite, 'user:1', 12.3456, 5);

        $records = $this->slowRecords($handler);
        self::assertCount(1, $records);
        self::assertSame('batch_write', $records[0]->context['operation']);
        self::assertSame(12.35, $records[0]->context['duration_ms']);
        self::assertSame(5, $records[0]->context['threshold_ms']);
        self::assertSame(KeyRedactor::redact('user:1'), $records[0]->context['key']);
    }

    /**
     * @return list<LogRecord>
     */
    private function slowRecords(TestHandler $handler): array
    {
        return array_values(array_filter(
            $handler->getRecords(),
            static fn(LogRecord $record): bool => $record->message === SlowLog::MESSAGE,
        ));
    }
}
