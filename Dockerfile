FROM alpine:3.21

# Install PHP 8.3 + pre-built grpc extension + protoc + grpc_php_plugin
RUN apk add --no-cache \
    php83 \
    php83-pecl-grpc \
    php83-phar \
    php83-mbstring \
    php83-openssl \
    php83-curl \
    php83-ctype \
    php83-dom \
    php83-xml \
    php83-simplexml \
    php83-xmlwriter \
    php83-tokenizer \
    php83-iconv \
    php83-zip \
    composer \
    protobuf \
    protobuf-dev \
    grpc-plugins

# Create symlink for php command
RUN ln -sf /usr/bin/php83 /usr/bin/php

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
