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
  - `grpc` - gRPC extension (1.57 or newer)
  - `protobuf` - Protocol Buffers extension
  - `pcov` (optional) - coverage driver for `composer test:coverage`
    (see [Coverage Lane](development.md#coverage-lane))
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

5. **Optional: start TiKV for a manual E2E run**:
   ```bash
   make up
   ```
   The `make test-e2e` target manages its own temporary cluster; use this
   command only when running one suite manually.

6. **Verify everything works**:
   ```bash
   make test
   ```

## Before Your First Commit

### 1. Understand the Project Structure

Read these files to understand the codebase:

- `README.md` - Project overview and usage
- `docs/architecture.md` - System architecture
- `docs/workflow.md` - Issue-to-merge development process
- [GitHub Issues](https://github.com/crazy-goat/tikv-php/issues) - Roadmap and implementation plans
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
composer test:e2e   # the same end-to-end run, through composer
```

Both runs should end without failures or errors. The exact test count changes
with every commit, so do not read it as a health signal — check that the
summary reports no failures and no errors:

```
OK (... tests, ... assertions)  # Unit tests
OK (... tests, ... assertions)  # E2E tests
```

PHPUnit may also end with `OK, but some tests were skipped!` or
`OK, but there were issues!` when it skipped tests or raised warnings and
deprecations. That is not a failure. The `Unit` suite excludes the
gRPC-dependent files and runs clean without `ext-grpc`; the `Integration` suite
skips its gRPC tests when the extension is missing, and the `Grpc` suite
(`composer test:grpc`) is the strict one: it runs those tests and treats a skip
as a failure.

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

# Run every check: PHPCS, Rector (dry run), PHPStan, shellcheck and hadolint
composer run lint

# Apply the automatic fixes (Rector and phpcbf) first, then run every check
composer run lint:fix

# Rector only: check, or rewrite the files in place
composer run rector
composer run rector:fix
```

### 6. Create a Branch

Never commit directly to `master` (see [`workflow.md`](workflow.md) for the
canonical branching and PR process):

```bash
# Update your local master
git checkout master
git pull upstream master

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
   # Add test to tests/Unit/, tests/Integration/ or tests/E2E/
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
- [ ] All checks pass (`composer run lint`; `composer run lint:fix` fixes what it can)
- [ ] Documentation updated (if needed)
- [ ] Examples work (if applicable)
- [ ] Commit message follows convention

## Project Structure

### Source Code (`src/`)

```
src/
├── Client/
│   ├── Batch/           # Parallel batch execution
│   │   └── BatchCommands/   # BatchCommands transport
│   ├── Cache/           # Region and store caching
│   ├── Codec/           # MVCC key/value codecs for API V1 and V2
│   ├── Connection/      # PD client, connection factory, keyspace resolution
│   ├── Exception/       # Custom exceptions
│   ├── Grpc/            # gRPC wrappers for RawKV and API V2
│   ├── Observability/   # Metrics and debug logging
│   ├── RawKv/           # RawKV client, scanner and SST ingest
│   │   └── Dto/         # RawKV result and option DTOs
│   ├── Region/          # Region routing, grouping and cache warming
│   │   └── Dto/         # Region and store DTOs
│   ├── Retry/           # Retry budgets, classification and backoff
│   ├── Tls/             # TLS configuration
│   ├── TxnKv/           # TxnKV client, transactions and lock resolver
│   │   └── Exception/   # Transactional exceptions
│   └── Util/            # Key ordering and redaction helpers
└── Proto/               # Generated protobuf and gRPC classes
    ├── Backup/
    ├── Coprocessor/
    ├── Deadlock/
    ├── Disaggregated/
    ├── DiskUsage/
    ├── Encryptionpb/
    ├── Eraftpb/
    ├── Errorpb/
    ├── GPBMetadata/
    ├── ImportSstpb/     # SST ingest protocol
    ├── Keyspacepb/
    ├── Kvrpcpb/         # RawKV and TxnKV protocol
    ├── Metapb/          # Metadata
    ├── Mpp/
    ├── Pdpb/            # PD protocol
    ├── RaftServerpb/
    ├── ReplicationModepb/
    ├── ResourceManager/
    ├── Tikvpb/          # TiKV services
    └── Tracepb/
```

`src/Proto/` is generated by `make proto-generate`; do not edit it by hand.

### Tests (`tests/`)

```
tests/
├── Unit/                # Unit tests (no TiKV needed)
│   ├── Batch/           # Batch execution and BatchCommands
│   ├── Bin/             # Guards for the scripts in bin/
│   ├── Cache/           # Region and store caches
│   ├── Ci/              # CI runners and PHPUnit configuration guards
│   ├── Codec/           # MVCC codecs
│   ├── Connection/      # PD client, connection factory, keyspaces
│   ├── Docs/            # Documentation guards
│   ├── Exception/       # Exception behaviour
│   ├── Grpc/            # gRPC-dependent tests (the Grpc suite)
│   ├── Observability/   # Metrics and debug logging
│   ├── Phpstan/         # Fixtures for the custom PHPStan rules
│   ├── RawKv/           # RawKV client and operations
│   │   └── Dto/         # RawKV DTO tests
│   ├── Region/          # Region routing and cache warming
│   ├── Retry/           # Retry and backoff
│   ├── Security/        # Redaction guards
│   ├── Support/         # Shared test doubles and helpers
│   ├── Tls/             # TLS configuration
│   ├── TxnKv/           # Transactions and lock resolution
│   └── Util/            # Key ordering and redaction
├── Integration/         # Integration tests (no TiKV needed)
│   └── LoggingIntegrationTest.php
├── E2E/                 # End-to-end tests (need a TiKV cluster)
│   ├── ApiV2E2ETest.php
│   ├── BatchCommandsConnectionE2ETest.php
│   ├── ClusterProbe.php
│   ├── FastCommitModesE2ETest.php
│   ├── GcSafePointE2ETest.php
│   ├── PdFailoverE2ETest.php
│   ├── RawKvE2ETest.php
│   ├── ReplicaReadE2ETest.php
│   ├── ResourceControlRawKvE2ETest.php
│   ├── ResourceControlTxnKvE2ETest.php
│   ├── ResourceGroupAdmin.php
│   └── TxnKvE2ETest.php
└── phpstan/
    └── bootstrap.php    # Bootstrap for the custom PHPStan rules
```

The suites are selected in `phpunit.xml`: `Unit` (no TiKV, no `ext-grpc`),
`Grpc` (needs `ext-grpc`), `Integration` (no cluster; its gRPC tests skip
without `ext-grpc`), and `E2E-RawKV`, `E2E-TxnKV` and `E2E-ApiV2` (each needs
its own cluster mode).

### Documentation (`docs/`)

```
docs/
├── README.md            # Documentation index
├── getting-started.md   # User guide
├── configuration.md     # Configuration guide
├── operations.md        # Operations reference
├── advanced.md          # Advanced patterns
├── architecture.md      # System architecture
├── error-handling.md    # Exception hierarchy and retry safety
├── troubleshooting.md   # Common issues
├── development.md       # Implementation guide
├── workflow.md          # Issue-to-merge process
├── release-workflow.md  # Release process
├── contributing.md      # This file
└── helpers/             # FAQ and recorded decisions
```

## Testing

### Unit Tests

Unit tests don't require TiKV:

```bash
# Run all unit tests
make test-unit

# Run specific test
vendor/bin/phpunit --filter RawKvClientTest

# Run the suites CI measures under a coverage driver, then check the floor
composer test:coverage
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

E2E tests use Docker and switch between two cluster modes. The
`make test-e2e` runner manages that lifecycle (and removes its temporary
volumes on exit), so a separate `make up` is not required for the full run.
It waits until all four service containers are running and PD reports all
three TiKV stores `Up` — a healthy PD alone is not enough, because the test
container reaches the stores by their Compose service names. On a failed run
it prints `compose ps` and the container logs before tearing the cluster
down, so the `down -v` does not erase the evidence:

```bash
# Run RawKV E2E, then switch to V1 and run TxnKV E2E
make test-e2e

# The same run through composer
composer test:e2e

# Or manually run the RawKV suite inside the Compose network
# (start the default V1TTL cluster first)
make up
docker compose run --rm --no-deps php-client \
  vendor/bin/phpunit --testsuite E2E-RawKV --no-coverage

# TxnKV uses the V1 cluster override and is run by make test-e2e
# (do not point the TxnKV container at a V1TTL cluster)
```

**The suites need different cluster modes, so a single-cluster run of both
ends red.** `E2E-RawKV` needs the V1TTL cluster (`tikv.toml`,
`enable-ttl = true`); `E2E-TxnKV` needs the V1 cluster
(`docker-compose.txnkv.yml`, no TTL); `E2E-ApiV2` needs the API V2 cluster
(`docker-compose.apiv2.yml`). A TTL-enabled cluster rejects transactional
requests and a cluster without TTL rejects TTL requests, so one cluster
cannot serve two of these suites — see
[TxnKV Fails on a TTL-Enabled Cluster](troubleshooting.md#txnkv-fails-on-a-ttl-enabled-cluster).

That is why the entry points are split:

| Command | Cluster it needs | What it runs |
|---|---|---|
| `composer test:e2e` (or `make test-e2e`) | Docker; it starts and switches both clusters itself | the complete run: `E2E-RawKV` against the V1TTL cluster, then `E2E-TxnKV` against the V1 cluster |
| `composer test:e2e:raw` | a running **V1TTL** cluster at `$PD_ENDPOINTS` | the `E2E-RawKV` suite only |
| `composer test:e2e:txn` | a running **V1** cluster (no TTL) at `$PD_ENDPOINTS` | the `E2E-TxnKV` suite only |
| `composer test:e2e:apiv2` | a running **API V2** cluster at `$PD_ENDPOINTS`, plus `$TIKV_ALT_KEYSPACE` | the `E2E-ApiV2` suite only |

The per-mode scripts run PHPUnit on the **host**, so `$PD_ENDPOINTS` has to name an
endpoint the host can reach. The default, `pd:2379`, is a Docker Compose service name
and resolves only inside the compose network; on the host use the published PD port:

```bash
PD_ENDPOINTS=localhost:${PD_PORT:-2379} composer test:e2e:raw
```

The API V2 lane needs a second keyspace that already exists in the cluster (the
suite proves that the same user key stays isolated per keyspace). Create it once
and name it in `TIKV_ALT_KEYSPACE`:

```bash
docker compose exec -T pd wget -qO- \
  --header='Content-Type: application/json' \
  --post-data='{"name":"php-e2e-alt"}' \
  http://127.0.0.1:2379/pd/api/v2/keyspaces

TIKV_ALT_KEYSPACE=php-e2e-alt PD_ENDPOINTS=localhost:${PD_PORT:-2379} \
  composer test:e2e:apiv2
```

Every one of these commands fails when it runs nothing: a selection that
matches no test is a non-zero exit code, not a green run that tested
nothing. The API V2 lane cannot report a green run that skipped its whole
suite either — the script sets `TIKV_API_VERSION=2` itself and treats a
skipped test as a failure. Without a cluster the end-to-end suites fail fast
(the cluster probe in `tests/E2E/ClusterProbe.php` asks PD whether it is
there), so a missing cluster is reported in seconds instead of after an hour
of retries.

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
 * @return int|null Remaining TTL in seconds, or null both when the key does
 *         not exist and when it exists without an expiry; use
 *         getKeyTTLResult() to tell the two apart
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

The full issue-to-merge process lives in [`workflow.md`](workflow.md); this
section is the short version for contributors.

1. **Push your branch**:
   ```bash
   git push origin feature/my-feature
   ```

2. **Create Pull Request** on GitHub:
   - Use clear title following commit convention
   - Fill in the PR template
   - Link related issues (`Closes #<N>`)

3. **PR Checklist**:
   - [ ] Tests pass
   - [ ] Code style passes
   - [ ] Documentation updated
   - [ ] `CHANGELOG.md` updated under `## [Unreleased]` (never under a released
         version heading) when the change is user-visible
   - [ ] Examples updated (if applicable)

4. **Code Review**:
   - Address reviewer comments
   - Keep commits clean (squash if needed)
   - Merge or rebase `master` into your branch if there are conflicts

5. **Merge**:
   - Maintainers merge once `ci-ok` is green and the branch is up to date with
     `master`
   - Squash merge only: `gh pr merge --squash --delete-branch`

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
- **Roadmap**: See [GitHub Issues](https://github.com/crazy-goat/tikv-php/issues) for open work and implementation plans

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

**Issue: Rector reports changes during `composer run lint`**
```bash
# Solution: Let Rector rewrite the files, then re-run the checks
composer run rector:fix
composer run lint
```

**Issue: PHPStan errors**
```bash
# Solution: Check specific errors
composer run phpstan
# Fix type issues or add annotations
```

**Issue: gRPC extension not found**

The client requires the `ext-grpc` PHP extension, 1.57 or newer, and
`composer.json` enforces it. See
[Installing the gRPC Extension](getting-started.md#installing-the-grpc-extension)
for the per-platform instructions (Debian/Ubuntu, Alpine, macOS) and the
version check.

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
- Mentioned in release notes
- Credited in relevant documentation

Thank you for contributing to TiKV PHP Client!
