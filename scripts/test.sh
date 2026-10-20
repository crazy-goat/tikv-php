#!/bin/bash
set -e

echo "=========================================="
echo "TiKV PHP Client - Test Suite"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print section headers
print_section() {
    echo ""
    echo -e "${YELLOW}$1${NC}"
    echo "------------------------------------------"
}

# Check if we're in the right directory
if [ ! -f "composer.json" ]; then
    echo -e "${RED}Error: composer.json not found${NC}"
    echo "Please run this script from the project root"
    exit 1
fi

# Install dependencies if needed
if [ ! -d "vendor" ]; then
    print_section "Installing dependencies..."
    # composer.json requires the ext-grpc extension (">=1.57"), but neither
    # the Unit nor the Integration suite needs it. The platform requirement
    # is ignored only when the extension is really missing; otherwise
    # Composer still enforces the version. See docs/helpers/faq.md,
    # "composer.json requires ext-grpc, but the unit job installs without it".
    install_args=(--no-interaction)
    if ! php -r 'exit(extension_loaded("grpc") ? 0 : 1);'; then
        echo "ext-grpc is not installed - installing without the platform check."
        install_args+=(--ignore-platform-req=ext-grpc)
    fi
    composer install "${install_args[@]}"
fi

# Run Unit tests
print_section "Running Unit Tests..."
vendor/bin/phpunit --testsuite Unit --testdox
UNIT_EXIT=$?

# Run Integration tests (if any)
print_section "Running Integration Tests..."
vendor/bin/phpunit --testsuite Integration --testdox || true

# Summary
echo ""
echo "=========================================="
if [ $UNIT_EXIT -eq 0 ]; then
    echo -e "${GREEN}Unit tests passed!${NC}"
else
    echo -e "${RED}Unit tests failed!${NC}"
fi
echo "=========================================="

exit $UNIT_EXIT
