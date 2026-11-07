# Pinned to the multi-arch index digest of alpine:3.21 (3.21.10) so a rebuild
# cannot silently pick up different base-layer content.
FROM alpine:3.21@sha256:87a4361f6c819618a6bafa2ed5228714cb0bb22195d2b49c95eb26799990128a

# The image runs as an unprivileged user. Compose passes the host owner's
# UID/GID so the bind-mounted checkout stays writable; the defaults keep a
# standalone `docker build .` usable as a non-root image.
ARG APP_UID=1000
ARG APP_GID=1000

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

# Create the unprivileged user before anything writes to /app, so Composer and
# PHPUnit own the working tree instead of running as root. The home directory
# stays outside /app: at runtime /app is a bind mount of the host checkout, and
# Composer's cache must not leak into it.
RUN addgroup -g "${APP_GID}" app \
    && adduser -D -u "${APP_UID}" -G app -s /sbin/nologin app \
    && mkdir -p /app \
    && chown app:app /app

# Set working directory
WORKDIR /app

# Copy composer files first for better caching
COPY --chown=app:app composer.json composer.lock ./

# Install dependencies as the unprivileged user, so vendor/ is owned by it and
# the autoloader can be regenerated below without root.
USER app
RUN composer install --no-interaction --no-scripts --no-autoloader

# Copy source code
COPY --chown=app:app . .

# Generate autoloader
RUN composer dump-autoload

# Run tests by default, as the unprivileged user
USER app
CMD ["vendor/bin/phpunit", "--testsuite", "E2E-RawKV", "--testdox", "--no-coverage"]
