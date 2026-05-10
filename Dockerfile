FROM alpine:edge

# Add Alpine Edge repositories
RUN echo "https://dl-cdn.alpinelinux.org/alpine/edge/main" > /etc/apk/repositories && \
    echo "https://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories

# Install PHP 8.4 + pre-built grpc extension + protoc + grpc_php_plugin
RUN apk add --no-cache \
    php84 \
    php84-pecl-grpc \
    php84-phar \
    php84-mbstring \
    php84-openssl \
    php84-curl \
    php84-ctype \
    php84-dom \
    php84-xml \
    php84-simplexml \
    php84-xmlwriter \
    php84-tokenizer \
    php84-iconv \
    php84-zip \
    composer \
    protobuf \
    protobuf-dev \
    grpc-plugins

# Create symlink for php command
RUN ln -sf /usr/bin/php84 /usr/bin/php

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
