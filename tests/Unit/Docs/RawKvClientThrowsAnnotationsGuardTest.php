<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\Unit\Docs;

use CrazyGoat\TiKV\Client\RawKv\RawKvClient;
use CrazyGoat\TiKV\Client\RawKv\ScanIterator;
use PHPUnit\Framework\TestCase;

/**
 * Regression guard for issue #385 [DOC-19]: the public `RawKvClient` methods
 * that execute a retrying operation have to document the exceptions that
 * retry loop can raise, and the argument-validating methods have to document
 * `InvalidArgumentException`.
 *
 * The defect this guards against is an *absent* annotation: a method added
 * tomorrow, or an existing method moved onto a retrying path, would simply
 * have no `@throws` line and no behavioural test could notice. The list of
 * methods is therefore derived from the source instead of restated here:
 *
 *  - `retryKind()` follows `$this->method()` calls inside the class and
 *    `$this->property->method()` calls into the `RawKv` components, and
 *    classifies the path from what the target body actually does:
 *    `->execute()` is a direct retry loop, `executeParallelCapped()` is the
 *    fan-out wrapper that reports failures through
 *    `BatchPartialFailureException`.
 *  - `canThrowInvalidArgument()` follows the same call graph and looks for a
 *    validator (inline or in a `RawKv` component) that throws the type.
 *
 * Two exclusions are deliberate, not omissions:
 *
 *  - the lazy iterator factories return a `ScanIterator`; their RPCs run
 *    during iteration, so the retry exceptions belong on
 *    `ScanIterator::next()` / `valid()`, not on the factory;
 *  - `ingest()` is classified as retrying since issue #197: `SstIngestor`
 *    runs each region group through a `RetryExecutor`, so both the retry
 *    budget and `StoreNotFoundException` are part of its contract.
 */
class RawKvClientThrowsAnnotationsGuardTest extends TestCase
{
    private const RETRY_BUDGET = 'RetryBudgetExhaustedException';

    private const STORE_NOT_FOUND = 'StoreNotFoundException';

    private const INVALID_ARGUMENT = 'InvalidArgumentException';

    private const BATCH_PARTIAL_FAILURE = 'BatchPartialFailureException';

    /**
     * @var array<string, array{doc: string, body: string, returnType: ?string}>
     */
    private array $methodSources = [];

    public function testDirectRetryMethodsDocumentTheRetryBudgetAndMissingStoreExceptions(): void
    {
        $direct = [];

        foreach ($this->publicMethodsOf(RawKvClient::class) as $name => $method) {
            if ($this->retryKind(RawKvClient::class, $name) !== 'direct') {
                continue;
            }

            $direct[] = $name;
            $this->assertDocblockDocuments(
                'RawKvClient::' . $name . '()',
                $method['doc'],
                self::RETRY_BUDGET,
            );
            $this->assertDocblockDocuments(
                'RawKvClient::' . $name . '()',
                $method['doc'],
                self::STORE_NOT_FOUND,
            );
        }

        self::assertNotEmpty(
            $direct,
            'No direct retry method was derived from RawKvClient; the guard would be vacuous.',
        );
    }

    public function testFannedOutRetryMethodsDocumentThePartialFailureWrapper(): void
    {
        $fannedOut = [];

        foreach ($this->publicMethodsOf(RawKvClient::class) as $name => $method) {
            if ($this->retryKind(RawKvClient::class, $name) !== 'fanout') {
                continue;
            }

            $fannedOut[] = $name;
            $this->assertDocblockDocuments(
                'RawKvClient::' . $name . '()',
                $method['doc'],
                self::BATCH_PARTIAL_FAILURE,
            );
        }

        self::assertNotEmpty(
            $fannedOut,
            'No fanned-out retry method was derived from RawKvClient; the guard would be vacuous.',
        );
    }

    public function testMethodsThatCanThrowInvalidArgumentExceptionDocumentIt(): void
    {
        $validating = [];

        foreach ($this->publicMethodsOf(RawKvClient::class) as $name => $method) {
            if (!$this->canThrowInvalidArgument(RawKvClient::class, $name)) {
                continue;
            }

            $validating[] = $name;
            $this->assertDocblockDocuments(
                'RawKvClient::' . $name . '()',
                $method['doc'],
                self::INVALID_ARGUMENT,
            );
        }

        self::assertNotEmpty(
            $validating,
            'No argument-validating method was derived from RawKvClient; the guard would be vacuous.',
        );
    }

    public function testConstructorsDocumentInvalidArgumentException(): void
    {
        foreach ([RawKvClient::class, ScanIterator::class] as $class) {
            $doc = $this->methodSource($class, '__construct')['doc'];

            $this->assertDocblockDocuments(
                $class . '::__construct()',
                $doc,
                self::INVALID_ARGUMENT,
            );
        }
    }

    public function testScanIteratorIterationDocumentsTheRetryExceptions(): void
    {
        $fetchers = 0;

        foreach (['next', 'valid'] as $name) {
            $method = $this->methodSource(ScanIterator::class, $name);
            self::assertStringContainsString(
                'fetchNextBatch(',
                $method['body'],
                sprintf('ScanIterator::%s() no longer fetches a page; re-check this guard.', $name),
            );

            $fetchers++;
            $this->assertDocblockDocuments(
                'ScanIterator::' . $name . '()',
                $method['doc'],
                self::RETRY_BUDGET,
            );
            $this->assertDocblockDocuments(
                'ScanIterator::' . $name . '()',
                $method['doc'],
                self::STORE_NOT_FOUND,
            );
        }

        self::assertSame(2, $fetchers);
    }

    public function testLazyScanFactoriesDoNotClaimTheIterationExceptions(): void
    {
        foreach (['scanIterator', 'scanPrefixIterator'] as $name) {
            $doc = $this->methodSource(RawKvClient::class, $name)['doc'];

            self::assertStringNotContainsString(
                '@throws ' . self::RETRY_BUDGET,
                $doc,
                sprintf(
                    'RawKvClient::%s() only returns an iterator; the retry exceptions are '
                    . 'raised during iteration and belong on ScanIterator::next()/valid().',
                    $name,
                ),
            );
            self::assertStringNotContainsString(
                '@throws ' . self::STORE_NOT_FOUND,
                $doc,
                sprintf(
                    'RawKvClient::%s() only returns an iterator; the store exception is '
                    . 'raised during iteration and belongs on ScanIterator::next()/valid().',
                    $name,
                ),
            );
        }
    }

    public function testIngestDocumentsItsRetryExecutorExceptions(): void
    {
        $doc = $this->methodSource(RawKvClient::class, 'ingest')['doc'];

        $this->assertDocblockDocuments('RawKvClient::ingest()', $doc, self::RETRY_BUDGET);
        $this->assertDocblockDocuments('RawKvClient::ingest()', $doc, self::STORE_NOT_FOUND);
    }

    /**
     * Classify how a method reaches a retry loop.
     *
     * `executeParallelCapped()` is checked before recursing because every
     * call made from its callbacks is awaited by `BatchAsyncExecutor`, which
     * wraps a TiKvException into `BatchPartialFailureException`. A body that
     * also calls `->execute()` directly keeps the `direct` verdict: that path
     * can surface the exception without the wrapper.
     *
     * @param class-string $class
     * @param array<string, true> $visited
     * @return 'direct'|'fanout'|'none'
     */
    private function retryKind(string $class, string $method, array $visited = []): string
    {
        $key = $class . '::' . $method;
        if (isset($visited[$key])) {
            return 'none';
        }
        $visited[$key] = true;

        // These factories build an executor for a caller; they do not run an
        // operation themselves.
        if ($method === 'createRetryExecutor' || $method === 'createBatchExecutor') {
            return 'none';
        }

        $body = $this->methodSource($class, $method)['body'];

        if (str_contains($body, '->execute(') || str_contains($body, 'new RetryExecutor(')) {
            return 'direct';
        }

        if (str_contains($body, 'executeParallelCapped(')) {
            return 'fanout';
        }

        $kinds = [];

        foreach ($this->thisMethodCalls($body) as $called) {
            $kinds[] = $this->retryKind($class, $called, $visited);
        }

        $properties = $this->propertyTypes($class);
        foreach ($this->propertyMethodCalls($body) as [$property, $called]) {
            $target = $properties[$property] ?? null;
            if (
                $target === null
                || !str_starts_with($target, 'CrazyGoat\\TiKV\\Client\\RawKv\\')
                || !class_exists($target)
            ) {
                continue;
            }
            if (!$this->sourceContains($target, 'RetryExecutor')) {
                continue;
            }

            $targetMethod = $this->methodSource($target, $called);
            if ($targetMethod['returnType'] === ScanIterator::class) {
                // Lazy iterator factory: the RPCs run during iteration.
                continue;
            }

            $kinds[] = $this->retryKind($target, $called, $visited);
        }

        if (in_array('direct', $kinds, true)) {
            return 'direct';
        }
        if (in_array('fanout', $kinds, true)) {
            return 'fanout';
        }

        return 'none';
    }

    /**
     * Whether a method can raise the client's `InvalidArgumentException`.
     *
     * The component check is deliberately coarse: a `RawKv` collaborator that
     * contains any argument validation at all makes the calling method a
     * candidate. That can over-report (a method that only calls a validated
     * collaborator on a path that cannot fail), but never under-reports the
     * four methods issue #385 named.
     *
     * @param class-string $class
     * @param array<string, true> $visited
     */
    private function canThrowInvalidArgument(string $class, string $method, array $visited = []): bool
    {
        $key = $class . '::' . $method;
        if (isset($visited[$key])) {
            return false;
        }
        $visited[$key] = true;

        $body = $this->methodSource($class, $method)['body'];

        if (str_contains($body, 'InvalidArgumentException')) {
            return true;
        }

        foreach ($this->thisMethodCalls($body) as $called) {
            if ($this->canThrowInvalidArgument($class, $called, $visited)) {
                return true;
            }
        }

        foreach ($this->staticMethodCalls($body) as $called) {
            if ($this->canThrowInvalidArgument($class, $called, $visited)) {
                return true;
            }
        }

        $properties = $this->propertyTypes($class);
        foreach ($this->propertyMethodCalls($body) as [$property]) {
            $target = $properties[$property] ?? null;
            if (
                $target === null
                || !str_starts_with($target, 'CrazyGoat\\TiKV\\Client\\RawKv\\')
                || !class_exists($target)
            ) {
                continue;
            }
            if ($this->sourceContains($target, 'InvalidArgumentException')) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param class-string $class
     * @return array<string, array{doc: string, body: string, returnType: ?string}>
     */
    private function publicMethodsOf(string $class): array
    {
        $methods = [];
        $reflection = new \ReflectionClass($class);

        foreach ($reflection->getMethods(\ReflectionMethod::IS_PUBLIC) as $method) {
            if ($method->getDeclaringClass()->getName() !== $class) {
                continue;
            }
            if (str_starts_with($method->getName(), '__')) {
                continue;
            }

            $methods[$method->getName()] = $this->methodSource($class, $method->getName());
        }

        return $methods;
    }

    /**
     * @param class-string $class
     * @return array{doc: string, body: string, returnType: ?string}
     */
    private function methodSource(string $class, string $method): array
    {
        $key = $class . '::' . $method;
        if (isset($this->methodSources[$key])) {
            return $this->methodSources[$key];
        }

        $reflection = new \ReflectionMethod($class, $method);
        $file = $reflection->getFileName();
        if ($file === false) {
            self::fail(sprintf('Cannot locate the source file of %s.', $key));
        }

        $lines = file($file);
        if ($lines === false) {
            self::fail(sprintf('Cannot read the source file of %s.', $key));
        }

        $start = $reflection->getStartLine();
        $end = $reflection->getEndLine();
        if ($start === false || $end === false) {
            self::fail(sprintf('Cannot locate the line range of %s.', $key));
        }

        $doc = $reflection->getDocComment();
        $returnType = $reflection->getReturnType();

        $source = [
            'doc' => $doc === false ? '' : $doc,
            'body' => implode('', array_slice($lines, $start - 1, $end - $start + 1)),
            'returnType' => $returnType instanceof \ReflectionNamedType ? $returnType->getName() : null,
        ];

        $this->methodSources[$key] = $source;

        return $source;
    }

    /**
     * @param class-string $class
     * @return array<string, string> property name => declared class name
     */
    private function propertyTypes(string $class): array
    {
        $types = [];
        $reflection = new \ReflectionClass($class);

        foreach ($reflection->getProperties() as $property) {
            if ($property->getDeclaringClass()->getName() !== $class) {
                continue;
            }

            $type = $property->getType();
            if ($type instanceof \ReflectionNamedType && !$type->isBuiltin()) {
                $types[$property->getName()] = $type->getName();
            }
        }

        return $types;
    }

    /**
     * @return list<string>
     */
    private function thisMethodCalls(string $body): array
    {
        $matches = [];
        $found = preg_match_all('/\$this->([A-Za-z_][A-Za-z0-9_]*)\s*\(/', $body, $matches);
        if ($found === false) {
            self::fail('Invalid regular expression for $this->method() calls.');
        }
        if ($found === 0) {
            return [];
        }

        return array_values(array_unique($matches[1]));
    }

    /**
     * @return list<array{string, string}>
     */
    private function propertyMethodCalls(string $body): array
    {
        $matches = [];
        $found = preg_match_all(
            '/\$this->([A-Za-z_][A-Za-z0-9_]*)->([A-Za-z_][A-Za-z0-9_]*)\s*\(/',
            $body,
            $matches,
        );
        if ($found === false) {
            self::fail('Invalid regular expression for $this->property->method() calls.');
        }
        if ($found === 0) {
            return [];
        }

        $calls = [];
        foreach ($matches[1] as $index => $property) {
            $calls[] = [$property, $matches[2][$index]];
        }

        return $calls;
    }

    /**
     * @return list<string>
     */
    private function staticMethodCalls(string $body): array
    {
        $matches = [];
        $found = preg_match_all('/\b(?:self|static)::([A-Za-z_][A-Za-z0-9_]*)\s*\(/', $body, $matches);
        if ($found === false) {
            self::fail('Invalid regular expression for self::method() calls.');
        }
        if ($found === 0) {
            return [];
        }

        return array_values(array_unique($matches[1]));
    }

    /**
     * @param class-string $class
     */
    private function sourceContains(string $class, string $needle): bool
    {
        $reflection = new \ReflectionClass($class);
        $file = $reflection->getFileName();
        if ($file === false) {
            self::fail(sprintf('Cannot locate the source file of %s.', $class));
        }

        $source = file_get_contents($file);
        if ($source === false) {
            self::fail(sprintf('Cannot read the source file of %s.', $class));
        }

        return str_contains($source, $needle);
    }

    private function assertDocblockDocuments(string $method, string $doc, string $exception): void
    {
        $found = preg_match(
            '/@throws\s+\S*' . preg_quote($exception, '/') . '\b/',
            $doc,
        );

        self::assertSame(
            1,
            $found,
            sprintf(
                '%s must document @throws %s; its body can raise it.',
                $method,
                $exception,
            ),
        );
    }
}
