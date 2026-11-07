<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Security;

use CrazyGoat\Proto\Kvrpcpb\KeyError;
use CrazyGoat\Proto\Kvrpcpb\RawPutResponse;
use CrazyGoat\TiKV\Client\Exception\GrpcException;
use CrazyGoat\TiKV\Client\Exception\RegionException;
use CrazyGoat\TiKV\Client\Region\KeyErrorDescriber;
use CrazyGoat\TiKV\Client\Region\RegionErrorHandler;
use CrazyGoat\TiKV\Client\Retry\BackoffType;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TransactionConflictException;
use CrazyGoat\TiKV\Client\TxnKv\Exception\TxnRetryableException;
use CrazyGoat\TiKV\Client\Util\ServerText;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for SEC-11 (issue #314): server-supplied text must not
 * reach an exception message with control characters, ANSI escapes or an
 * unbounded length.
 *
 * A compromised TiKV or PD server controls these strings; interpolating them
 * verbatim lets it forge log lines and inject terminal escapes into the log
 * sink used to investigate the compromise.
 */
class ServerTextSanitisationTest extends TestCase
{
    /** Newline, ANSI colour codes, carriage return, bell and a tab. */
    private const HOSTILE = "injected\nforged log line \x1b[31mred\x1b[0m\r\x07\ttabbed";

    public function testGrpcExceptionStripsControlCharactersAndAnsiEscapes(): void
    {
        $exception = new GrpcException(self::HOSTILE, 13);
        $message = $exception->getMessage();

        self::assertStringContainsString('injected', $message);
        self::assertStringContainsString('forged log line', $message);
        self::assertStringContainsString('red', $message);
        self::assertStringNotContainsString("\n", $message);
        self::assertStringNotContainsString("\r", $message);
        self::assertStringNotContainsString("\t", $message);
        self::assertStringNotContainsString("\x1b", $message);
        self::assertStringNotContainsString('[31m', $message);
        self::assertStringNotContainsString('[0m', $message);
        self::assertStringNotContainsString("\x07", $message);
    }

    public function testRegionExceptionStripsControlCharactersFromServerMessage(): void
    {
        $exception = new RegionException('RegionError', self::HOSTILE);
        $message = $exception->getMessage();

        self::assertStringContainsString('RegionError failed:', $message);
        self::assertStringContainsString('injected', $message);
        self::assertStringNotContainsString("\n", $message);
        self::assertStringNotContainsString("\r", $message);
        self::assertStringNotContainsString("\x1b", $message);
        self::assertStringNotContainsString('[31m', $message);
    }

    public function testRegionErrorHandlerSanitisesTopLevelResponseErrorString(): void
    {
        $response = new RawPutResponse();
        $response->setError(self::HOSTILE);

        try {
            RegionErrorHandler::check($response);
            self::fail('A top-level response error must throw a RegionException');
        } catch (RegionException $exception) {
            $message = $exception->getMessage();
            self::assertStringContainsString('BatchRequest failed:', $message);
            self::assertStringContainsString('injected', $message);
            self::assertStringNotContainsString("\n", $message);
            self::assertStringNotContainsString("\x1b", $message);
            self::assertStringNotContainsString('[31m', $message);
        }
    }

    public function testKeyErrorDescriberSanitisesRetryableAndAbortText(): void
    {
        $error = new KeyError();
        $error->setRetryable("retry\nme \x1b[31m");
        $error->setAbort("abort\rme");

        $description = KeyErrorDescriber::describe($error);

        self::assertStringContainsString('retryable: retryme', $description);
        self::assertStringContainsString('abort: abortme', $description);
        self::assertStringNotContainsString("\n", $description);
        self::assertStringNotContainsString("\r", $description);
        self::assertStringNotContainsString("\x1b", $description);
    }

    public function testTransactionConflictExceptionSanitisesServerText(): void
    {
        $exception = new TransactionConflictException("conflict\nforged \x1b[31m");

        self::assertStringNotContainsString("\n", $exception->getMessage());
        self::assertStringNotContainsString("\x1b", $exception->getMessage());
        self::assertStringContainsString('conflictforged', $exception->getMessage());
    }

    public function testTxnRetryableExceptionSanitisesServerText(): void
    {
        $exception = new TxnRetryableException("retry\nforged", BackoffType::TxnLock);

        self::assertStringNotContainsString("\n", $exception->getMessage());
        self::assertStringContainsString('retryforged', $exception->getMessage());
    }

    public function testLongServerTextIsCappedAndMarkedAsTruncated(): void
    {
        $text = str_repeat('a', ServerText::DEFAULT_MAX_LENGTH + 50) . 'TAIL';

        $clean = ServerText::sanitise($text);

        self::assertSame(ServerText::DEFAULT_MAX_LENGTH + 3, strlen($clean));
        self::assertStringEndsWith('...', $clean);
        self::assertStringNotContainsString('TAIL', $clean);
    }

    public function testShortCleanTextIsReturnedUnchanged(): void
    {
        self::assertSame('server is busy', ServerText::sanitise('server is busy'));
    }

    public function testInvalidUtf8IsNotDiscarded(): void
    {
        self::assertSame("a\xFFb", ServerText::sanitise("a\xFFb"));
    }
}
