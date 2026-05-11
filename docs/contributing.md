# Contributing Guide

Guide for developers who want to contribute to the TiKV PHP Client project.

## Table of Contents

1. [Getting Started as a Developer](#getting-started-as-a-developer)
2. [Before Your First Commit](#before-your-first-commit)
3. [Development Workflow](#development-workflow)
4. [Project Structure](#project-structure)
5. [Testing](#testing)
6. [Code Standards](#code-standards)
7. [Submitting Changes](#submitting-changes)

## Getting Started as a Developer

### Prerequisites

Before you start developing, ensure you have:

- **PHP >= 8.2** with extensions:
  - `grpc` - gRPC extension
  - `protobuf` - Protocol Buffers extension
- **Composer** - PHP dependency manager
- **Docker & Docker Compose** - For running TiKV cluster locally
- **Git** - Version control
- **Make** - Build automation (usually pre-installed on Linux/Mac)

### Initial Setup

1. **Fork the repository** on GitHub

2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/tikv-php.git
   cd tikv-php
   ```

3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/crazy-goat/tikv-php.git
   ```

4. **Install dependencies**:
   ```bash
   make install
   # Or manually:
   composer install
   ```

5. **Start TiKV cluster** (for E2E tests):
   ```bash
   make up
   ```

6. **Verify everything works**:
   ```bash
   make test
   ```

## Before Your First Commit

### 1. Understand the Project Structure

Read these files to understand the codebase:

- `README.md` - Project overview and usage
- `docs/architecture.md` - System architecture (if exists)
- `docs/superpowers/plans/README.md` - Implementation roadmap
- `composer.json` - Dependencies and scripts

### 2. Set Up Your Development Environment

#### PHP Configuration

Ensure your `php.ini` has:

```ini
memory_limit = 256M
max_execution_time = 300
```

#### IDE Setup

Recommended IDE configuration:

**PHPStorm/IntelliJ:**
- Enable PHP PSR-12 code style
- Install PHPStan plugin
- Configure PHPUnit test runner

**VS Code:**
- Install extensions:
  - PHP Intelephense
  - PHP CS Fixer
  - PHPStan
  - PHPUnit Test Explorer

### 3. Run the Test Suite

Before making any changes, ensure all tests pass:

```bash
# Run all tests
make test

# Or separately:
make test-unit      # Unit tests only (fast)
make test-e2e       # E2E tests (requires TiKV)
```

Expected output:
```
OK (21 tests, 45 assertions)  # Unit tests
OK (141 tests, 312 assertions) # E2E tests
```

### 4. Explore the Codebase

Key files to understand:

```
src/Client/RawKv/RawKvClient.php      # Main client - start here
src/Client/Connection/PdClient.php      # PD discovery
src/Client/Grpc/GrpcClient.php          # gRPC wrapper
src/Client/Cache/RegionCache.php        # Region caching
src/Client/Retry/BackoffType.php       # Retry logic
```

Run the examples to see how it works:

```bash
php examples/basic.php
php examples/batch.php
php examples/scan.php
```

### 5. Check Code Standards

Before committing, ensure your code follows standards:

```bash
# Check code style
composer run cs

# Fix code style automatically
composer run cs-fix

# Run static analysis
composer run phpstan

# Run all linting
composer run lint
```

### 6. Create a Branch

Never commit directly to `main`:

```bash
# Update your local main
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/my-feature-name
# or
git checkout -b fix/issue-description
```

Branch naming conventions:
- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation
- `refactor/description` - Code refactoring

## Development Workflow

### Making Changes

1. **Write tests first** (TDD approach):
   ```bash
   # Add test to tests/Unit/ or tests/E2E/
   # Run just your new test
   vendor/bin/phpunit --filter YourTestName
   ```

2. **Implement the feature**

3. **Run tests continuously**:
   ```bash
   # Watch mode (if available)
   vendor/bin/phpunit --testdox --colors=always
   ```

4. **Check code quality**:
   ```bash
   composer run lint
   ```

### Commit Messages

Follow conventional commits format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation
- `test:` - Tests
- `refactor:` - Code refactoring
- `perf:` - Performance improvement
- `chore:` - Maintenance

Examples:
```
feat(rawkv): add support for reverse scan

Implement reverse scanning with native reverse=true flag.
Results are returned in descending order.

Closes #45
```

```
fix(retry): handle ServerIsBusy with exponential backoff

ServerIsBusy errors now use progressive backoff instead of
immediate retry, reducing server load.
```

### Before Committing Checklist

- [ ] Tests pass (`make test`)
- [ ] Code style passes (`composer run cs`)
- [ ] Static analysis passes (`composer run phpstan`)
- [ ] Documentation updated (if needed)
- [ ] Examples work (if applicable)
- [ ] Commit message follows convention

## Project Structure

### Source Code (`src/`)

```
src/
├── Client/
│   ├── Batch/           # Parallel batch execution
│   ├── Cache/           # Region and store caching
│   ├── Connection/      # PD client
│   ├── Exception/       # Custom exceptions
│   ├── Grpc/            # gRPC wrapper
│   ├── RawKv/           # RawKV client and DTOs
│   ├── Retry/           # Retry logic
│   └── Tls/             # TLS configuration
└── Proto/               # Generated protobuf classes
    ├── Coprocessor/
    ├── Deadlock/
    ├── Errorpb/
    ├── Kvrpcpb/         # RawKV protocol
    ├── Metapb/          # Metadata
    ├── Pdpb/            # PD protocol
    └── Tikvpb/          # TiKV services
```

### Tests (`tests/`)

```
tests/
├── Unit/                # Unit tests (no TiKV needed)
│   ├── RawKv/
│   ├── Connection/
│   └── ...
└── E2E/                 # End-to-end tests (needs TiKV)
    └── RawKvE2ETest.php
```

### Documentation (`docs/`)

```
docs/
├── README.md            # Documentation index
├── getting-started.md   # User guide
├── configuration.md     # Configuration guide
├── operations.md        # Operations reference
├── advanced.md          # Advanced patterns
├── architecture.md      # System architecture
├── troubleshooting.md   # Common issues
├── contributing.md      # This file
└── superpowers/         # Implementation plans
    ├── plans/
    └── specs/
```

## Testing

### Unit Tests

Unit tests don't require TiKV:

```bash
# Run all unit tests
make test-unit

# Run specific test
vendor/bin/phpunit --filter RawKvClientTest

# Run with coverage
vendor/bin/phpunit --coverage-html coverage/
```

Writing unit tests:

```php
<?php
namespace CrazyGoat\TiKV\Tests\Unit\RawKv;

use PHPUnit\Framework\TestCase;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

class MyFeatureTest extends TestCase
{
    public function testMyFeature(): void
    {
        // Arrange
        $mockPdClient = $this->createMock(PdClientInterface::class);
        // ... setup mocks
        
        // Act
        $client = new RawKvClient($mockPdClient, $mockGrpc);
        $result = $client->myMethod();
        
        // Assert
        $this->assertEquals('expected', $result);
    }
}
```

### E2E Tests

E2E tests require a running TiKV cluster:

```bash
# Start TiKV
make up

# Run E2E tests
make test-e2e

# Or manually
vendor/bin/phpunit --testsuite E2E
```

Writing E2E tests:

```php
<?php
namespace CrazyGoat\TiKV\Tests\E2E;

use PHPUnit\Framework\TestCase;
use CrazyGoat\TiKV\Client\RawKv\RawKvClient;

class MyFeatureE2ETest extends TestCase
{
    private static ?RawKvClient $client = null;
    private array $keysToCleanup = [];
    
    public static function setUpBeforeClass(): void
    {
        $pdEndpoints = ['127.0.0.1:2379'];
        self::$client = RawKvClient::create($pdEndpoints);
    }
    
    protected function setUp(): void
    {
        $this->keysToCleanup = [];
    }
    
    protected function tearDown(): void
    {
        // Cleanup after each test
        foreach ($this->keysToCleanup as $key) {
            try {
                self::$client->delete($key);
            } catch (\Exception) {
                // Ignore cleanup errors
            }
        }
    }
    
    public function testMyFeature(): void
    {
        $key = 'test:my-feature:' . uniqid();
        $this->keysToCleanup[] = $key;
        
        // Test your feature
        self::$client->put($key, 'value');
        $result = self::$client->get($key);
        
        $this->assertEquals('value', $result);
    }
}
```

### Test Data Management

Always clean up test data:

```php
private function cleanup(string $pattern): void
{
    $results = self::$client->scanPrefix($pattern, keyOnly: true);
    $keys = array_column($results, 'key');
    if (!empty($keys)) {
        self::$client->batchDelete($keys);
    }
}
```

## Code Standards

### PHP Standards

We follow PSR-12 coding standard:

- 4 spaces for indentation (no tabs)
- Opening braces on same line for classes/methods
- One blank line after `use` statements
- Strict types declaration: `declare(strict_types=1);`

Example:

```php
<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\RawKv;

use CrazyGoat\TiKV\Client\Exception\TiKvException;

final class RawKvClient
{
    public function get(string $key): ?string
    {
        // Implementation
    }
}
```

### Type Declarations

Always use strict types:

```php
// Good
public function get(string $key): ?string;
public function batchPut(array $pairs, int|array $ttl = 0): void;

// Bad
public function get($key);
public function batchPut($pairs, $ttl = 0);
```

### Documentation

Document all public methods:

```php
/**
 * Get the remaining TTL for a key.
 *
 * @param string $key The key to check
 * @return int|null Remaining TTL in seconds, or null if not found/no TTL
 */
public function getKeyTTL(string $key): ?int;
```

### Naming Conventions

- Classes: `PascalCase` (e.g., `RawKvClient`)
- Methods: `camelCase` (e.g., `batchGet`)
- Constants: `UPPER_SNAKE_CASE`
- Private properties: `camelCase`
- Variables: `camelCase`

## Submitting Changes

### Pull Request Process

1. **Push your branch**:
   ```bash
   git push origin feature/my-feature
   ```

2. **Create Pull Request** on GitHub:
   - Use clear title following commit convention
   - Fill in the PR template
   - Link related issues

3. **PR Checklist**:
   - [ ] Tests pass
   - [ ] Code style passes
   - [ ] Documentation updated
   - [ ] CHANGELOG.md updated (if applicable)
   - [ ] Examples updated (if applicable)

4. **Code Review**:
   - Address reviewer comments
   - Keep commits clean (squash if needed)
   - Rebase on main if there are conflicts

5. **Merge**:
   - Maintainers will merge when approved
   - Use "Squash and merge" for clean history

### What to Include

**For Features:**
- Implementation
- Unit tests
- E2E tests (if applicable)
- Documentation update
- Example (if applicable)

**For Bug Fixes:**
- Fix
- Regression test
- Brief explanation in PR description

**For Documentation:**
- Updated/added docs
- Spell check
- Link verification

## Getting Help

### Resources

- **Documentation**: Browse `docs/` directory
- **Examples**: Check `examples/` directory
- **Tests**: See `tests/` for usage patterns
- **Plans**: See `docs/superpowers/plans/` for roadmap

### Communication

- **Issues**: GitHub Issues for bugs and features
- **Discussions**: GitHub Discussions for questions
- **PRs**: For code review and contributions

### Common Issues for New Contributors

**Issue: Tests fail with "Connection refused"**
```bash
# Solution: Start TiKV cluster
make up
```

**Issue: Code style check fails**
```bash
# Solution: Auto-fix code style
composer run cs-fix
```

**Issue: PHPStan errors**
```bash
# Solution: Check specific errors
composer run phpstan
# Fix type issues or add annotations
```

**Issue: gRPC extension not found**
```bash
# Solution: Install gRPC
pecl install grpc
# Add to php.ini: extension=grpc.so
```

## Development Tips

### Debugging

Enable verbose logging:

```php
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$logger = new Logger('debug');
$logger->pushHandler(new StreamHandler('php://stderr', Logger::DEBUG));

$client = RawKvClient::create(['127.0.0.1:2379'], logger: $logger);
```

### Profiling

Use Blackfire or XHProf for performance analysis:

```bash
# Install Blackfire
composer require --dev blackfire/php-sdk

# Profile a script
blackfire run php examples/batch.php
```

### Proto Generation

If you modify protobuf definitions:

```bash
# Regenerate PHP classes from proto files
make proto-generate

# Clean and regenerate
make proto-clean && make proto-generate
```

### Docker Tips

```bash
# Rebuild containers after dependency changes
make build

# View logs
make logs

# Reset everything
make clean && make up

# Development shell
make shell
```

## Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Mentioned in release notes
- Credited in relevant documentation

Thank you for contributing to TiKV PHP Client!
