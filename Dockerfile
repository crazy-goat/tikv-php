FROM php:8.4-cli-alpine

# Install grpc extension via pecl
RUN apk add --no-cache linux-headers && \
    pecl install grpc && \
    docker-php-ext-enable grpc

# Install protobuf tools and composer
RUN apk add --no-cache protobuf protobuf-dev grpc-plugins composer

# Install zip extension (needed by composer)
RUN apk add --no-cache zip unzip libzip-dev && \
    docker-php-ext-install zip

# Set working directory
WORKDIR /app

# Copy composer files first for better caching
COPY composer.json ./

# Install dependencies
RUN composer install --no-interaction --no-scripts --no-autoloader

# Copy source code
COPY . .

# Generate autoloader
RUN composer dump-autoload

# Run tests by default
CMD ["vendor/bin/phpunit", "--testsuite", "E2E", "--testdox"]
