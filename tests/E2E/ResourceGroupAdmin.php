<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Tests\E2E;

use RuntimeException;

use function is_array;
use function is_float;
use function is_int;
use function is_numeric;
use function is_string;
use function sprintf;

/**
 * Minimal PD resource-manager HTTP client for the E2E tests (issue #427).
 *
 * PD 8.5 serves the resource-manager REST API on its client port:
 *
 *   POST   /resource-manager/api/v1/config/group         create
 *   GET    /resource-manager/api/v1/config/group/{name}  read (with_stats=true adds ru_consumption)
 *   DELETE /resource-manager/api/v1/config/group/{name}  delete
 *
 * The API is reachable from the compose network (`pd:2379`) and from the
 * host through the published PD port. The helper deliberately uses plain
 * cURL instead of a client dependency.
 *
 * RU accounting note: `ru_consumption` is filled from the ResourceManager
 * service's token-bucket stream. TiKV consumes the *request priority*
 * carried in `Context.resource_control_context`, and its RU consumption is
 * reported to PD by a resource-manager client (TiDB); a native TiKV client
 * alone does not populate the PD statistics. See `docs/helpers/decisions.md`
 * ("Resource-control E2E observation limit"). The E2E tests therefore use
 * this helper to prove that the group exists and that grouped traffic
 * succeeds, which is the fallback the review accepted.
 */
final class ResourceGroupAdmin
{
    /** The resource-manager REST prefix on PD's client port. */
    private const API_PREFIX = '/resource-manager/api/v1/config';

    /**
     * @param list<string> $pdEndpoints
     */
    public static function apiBase(array $pdEndpoints): string
    {
        $endpoint = $pdEndpoints[0] ?? 'pd:2379';
        if (!str_contains($endpoint, '://')) {
            $endpoint = 'http://' . $endpoint;
        }

        $parts = parse_url($endpoint);
        if (!is_array($parts) || !isset($parts['host'])) {
            throw new RuntimeException(sprintf('Cannot parse PD endpoint "%s"', $endpoint));
        }

        return sprintf(
            '%s://%s:%d',
            is_string($parts['scheme'] ?? null) ? $parts['scheme'] : 'http',
            $parts['host'],
            (int) ($parts['port'] ?? 2379),
        );
    }

    /**
     * Whether the PD instance exposes the resource-manager API at all.
     *
     * @param list<string> $pdEndpoints
     */
    public static function isAvailable(array $pdEndpoints): bool
    {
        try {
            self::request($pdEndpoints, 'GET', '/groups');

            return true;
        } catch (RuntimeException) {
            return false;
        }
    }

    /**
     * Create an RU-mode group with a generous token bucket, so the traffic
     * of one E2E test is never throttled.
     *
     * @param list<string> $pdEndpoints
     */
    public static function create(array $pdEndpoints, string $name): void
    {
        self::request($pdEndpoints, 'POST', '/group', [
            'name' => $name,
            'mode' => 1, // GroupMode::RUMode
            'r_u_settings' => [
                'r_u' => [
                    'settings' => [
                        'fill_rate' => 100000,
                        'burst_limit' => 1000000,
                    ],
                ],
            ],
            'priority' => 1,
        ]);
    }

    /**
     * Read a group back, with its consumption statistics when PD has any.
     *
     * @param list<string> $pdEndpoints
     *
     * @return array<string, mixed>
     */
    public static function get(array $pdEndpoints, string $name): array
    {
        return self::request(
            $pdEndpoints,
            'GET',
            '/group/' . rawurlencode($name) . '?with_stats=true',
        );
    }

    /**
     * @param list<string> $pdEndpoints
     */
    public static function delete(array $pdEndpoints, string $name): void
    {
        try {
            self::request($pdEndpoints, 'DELETE', '/group/' . rawurlencode($name));
        } catch (RuntimeException) {
            // Cleanup must not mask the test result; a leftover group is
            // harmless and a later run uses a unique name anyway.
        }
    }

    /**
     * Total RU recorded for a group, or 0.0 when PD reports no stats.
     *
     * @param array<string, mixed> $group
     */
    public static function ruTotal(array $group): float
    {
        $stats = $group['ru_consumption'] ?? null;
        if (!is_array($stats)) {
            return 0.0;
        }

        return self::number($stats['r_r_u'] ?? null) + self::number($stats['w_r_u'] ?? null);
    }

    private static function number(mixed $value): float
    {
        if (is_int($value) || is_float($value)) {
            return (float) $value;
        }
        if (is_string($value) && is_numeric($value)) {
            return (float) $value;
        }

        return 0.0;
    }

    /**
     * @param list<string>                        $pdEndpoints
     * @param array<string, mixed>|null            $body
     *
     * @return array<string, mixed>
     */
    private static function request(
        array $pdEndpoints,
        string $method,
        string $path,
        ?array $body = null,
    ): array {
        if (!function_exists('curl_init')) {
            throw new RuntimeException('The E2E resource-group helper needs ext-curl');
        }

        $handle = curl_init(self::apiBase($pdEndpoints) . self::API_PREFIX . $path);
        if ($handle === false) {
            throw new RuntimeException('Cannot create a cURL handle');
        }

        $headers = ['Accept: application/json'];

        if ($body !== null) {
            $headers[] = 'Content-Type: application/json';
            $encoded = json_encode($body, JSON_THROW_ON_ERROR);
            curl_setopt($handle, CURLOPT_POSTFIELDS, $encoded);
        }

        if ($method === '') {
            throw new RuntimeException('HTTP method must not be empty');
        }
        curl_setopt($handle, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($handle, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($handle, CURLOPT_CONNECTTIMEOUT, 5);
        curl_setopt($handle, CURLOPT_TIMEOUT, 10);
        curl_setopt($handle, CURLOPT_HTTPHEADER, $headers);

        $raw = curl_exec($handle);
        $status = curl_getinfo($handle, CURLINFO_RESPONSE_CODE);
        $error = curl_error($handle);
        curl_close($handle);

        if (!is_string($raw)) {
            throw new RuntimeException(sprintf('PD resource-manager request failed: %s', $error));
        }
        if ($status >= 300) {
            throw new RuntimeException(sprintf(
                'PD resource-manager %s %s returned HTTP %d: %s',
                $method,
                $path,
                $status,
                $raw,
            ));
        }

        if (trim($raw) === '') {
            return [];
        }

        $decoded = json_decode($raw, true);
        if (!is_array($decoded)) {
            if ($method !== 'GET') {
                // POST/PUT/DELETE answer with a plain-text "Success!", not JSON.
                return [];
            }

            throw new RuntimeException(sprintf(
                'PD resource-manager %s %s returned non-object JSON: %s',
                $method,
                $path,
                $raw,
            ));
        }

        /** @var array<string, mixed> $decoded */
        return $decoded;
    }
}
