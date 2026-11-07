# Pinned to the multi-arch index digest of alpine:3.21 (3.21.10) so a rebuild
# cannot silently pick up different base-layer content.
FROM alpine:3.21@sha256:87a4361f6c819618a6bafa2ed5228714cb0bb22195d2b49c95eb26799990128a

# The image runs as an unprivileged user. Compose passes the host owner's
# UID/GID so the bind-mounted checkout stays writable; the defaults keep a
# standalone `docker build .` usable as a non-root image.
ARG APP_UID=1000
ARG APP_GID=1000

# Install PHP 8.3 + pre-built grpc extension + protoc + grpc_php_plugin.
# `bash` is not needed by the client, but the image is also the documented
# place to run the Unit suite, and tests/Unit/Ci/E2eRunnerReadinessTest drives
# its harness through `bash -c`.
RUN apk add --no-cache \
    bash \
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

# Refuse a root-running image even when someone invokes `sudo make build`.
RUN if [ "${APP_UID}" = "0" ]; then \
        echo "ERROR: APP_UID=0 would run the container as root; run make build as your normal user or set HOST_UID to a non-zero value." >&2; \
        exit 1; \
    fi

# Create a named account when the host IDs are free. Host GIDs often collide
# with an Alpine system group (macOS `staff` is GID 20, and Alpine owns several
# low GIDs), so a collision falls back to a free GID for `app`: the bind mount
# is governed by the owner UID, not the group. A host UID that already exists
# (root, ntp, …) is reused numerically; USER below is numeric, so no passwd
# entry is required. Only APP_UID=0 is refused, by the guard above.
RUN if ! grep -q "^[^:]*:[^:]*:${APP_GID}:" /etc/group; then \
        addgroup -g "${APP_GID}" app; \
    else \
        echo "WARNING: APP_GID=${APP_GID} already exists; using a free GID for the app group. The bind mount is governed by APP_UID." >&2; \
        addgroup app; \
    fi \
    && if ! grep -q "^[^:]*:[^:]*:${APP_UID}:" /etc/passwd; then \
        adduser -D -u "${APP_UID}" -G app -s /sbin/nologin app; \
    else \
        echo "WARNING: APP_UID=${APP_UID} already exists in the base image; reusing that account numerically." >&2; \
    fi \
    && mkdir -p /app /home/app \
    && chown -R "${APP_UID}:app" /app /home/app

# Keep Composer's cache out of the bind-mounted checkout.
ENV HOME=/home/app

# Set working directory
WORKDIR /app

# Copy composer files first for better caching
COPY --chown=${APP_UID}:app composer.json composer.lock ./

# Install dependencies as the unprivileged user, so vendor/ is owned by it and
# the autoloader can be regenerated below without root.
USER ${APP_UID}:app
RUN composer install --no-interaction --no-scripts --no-autoloader

# Copy source code
COPY --chown=${APP_UID}:app . .

# Generate autoloader
RUN composer dump-autoload

# Run tests by default, as the unprivileged user
USER ${APP_UID}:app
CMD ["vendor/bin/phpunit", "--testsuite", "E2E-RawKV", "--testdox", "--no-coverage"]
