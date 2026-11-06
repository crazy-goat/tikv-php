<?php

declare(strict_types=1);

namespace CrazyGoat\TiKV\Client\Region;

use CrazyGoat\Proto\Kvrpcpb\Context;
use CrazyGoat\Proto\Kvrpcpb\ResourceControlContext;
use CrazyGoat\TiKV\Client\Exception\InvalidArgumentException;

use function array_key_exists;
use function get_debug_type;
use function is_string;
use function preg_match;
use function sprintf;
use function var_export;

/**
 * Resource-control metadata carried on the `Context` of every TiKV request
 * (issue #427, GAP-13).
 *
 * TiKV's resource control is configured per cluster through resource groups
 * (PD's resource manager). A client participates by naming the group its
 * requests belong to and by attaching an opaque tag used for query
 * attribution. Two wire fields exist in the pinned kvproto for that:
 *
 * - `Context.resource_control_context.resource_group_name` (field 28), which
 *   ties the request to a resource group and therefore to its quota;
 * - `Context.resource_group_tag` (field 18, bytes), which carries any
 *   additional serialized information about the request (for example an
 *   application or query identifier).
 *
 * Both are optional. When no value is configured, neither field is touched,
 * so the serialized request is byte-identical to a request built before this
 * feature existed. This class deliberately does not implement client-side
 * Request Unit accounting: reading `ExecDetailsV2`/`RUV2` from responses and
 * feeding a local token bucket is a separate, larger feature.
 *
 * Group names follow PD's resource-group naming rule: 1 to 64 characters,
 * letters, digits, underscores and hyphens only. The tag is opaque bytes and
 * only has to be non-empty when set.
 */
final readonly class ResourceControl
{
    /** Client/begin() option key for the resource group name. */
    public const OPTION_RESOURCE_GROUP = 'resourceGroup';

    /** Client/begin() option key for the opaque resource-group tag. */
    public const OPTION_RESOURCE_GROUP_TAG = 'resourceGroupTag';

    /** Maximum resource-group-name length accepted by PD. */
    public const MAX_RESOURCE_GROUP_NAME_LENGTH = 64;

    private const RESOURCE_GROUP_NAME_PATTERN = '/\A[A-Za-z0-9_-]{1,64}\z/';

    public function __construct(
        private ?string $groupName = null,
        private ?string $resourceGroupTag = null,
    ) {
        if ($this->groupName !== null) {
            $this->assertGroupName($this->groupName);
        }
        if ($this->resourceGroupTag !== null) {
            $this->assertTag($this->resourceGroupTag);
        }
    }

    /**
     * Build the resource-control values from an options array.
     *
     * Absent keys mean "not configured" and stay null; a present value must
     * be a non-empty string. `$label` names the options array in the error
     * message (`options` for client options, `begin() options` for a
     * transaction).
     *
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException When a configured value is invalid
     */
    public static function fromOptions(array $options, string $label = 'options'): self
    {
        return new self(
            self::readOption($options, self::OPTION_RESOURCE_GROUP, $label),
            self::readOption($options, self::OPTION_RESOURCE_GROUP_TAG, $label),
        );
    }

    public function getGroupName(): ?string
    {
        return $this->groupName;
    }

    public function getResourceGroupTag(): ?string
    {
        return $this->resourceGroupTag;
    }

    public function isEmpty(): bool
    {
        return $this->groupName === null && $this->resourceGroupTag === null;
    }

    /**
     * Layer per-transaction overrides on top of a client-wide default.
     * A null value in $overrides keeps the corresponding default.
     */
    public function withOverrides(self $overrides): self
    {
        return new self(
            $overrides->groupName ?? $this->groupName,
            $overrides->resourceGroupTag ?? $this->resourceGroupTag,
        );
    }

    /**
     * Apply the configured values to a request context.
     *
     * Nothing is written when the corresponding value is null. In particular
     * no empty message is set, because proto3 does not serialize a default
     * message/scalar and an untouched context keeps its previous wire bytes.
     */
    public function apply(Context $context): void
    {
        if ($this->groupName !== null) {
            $resourceControlContext = new ResourceControlContext();
            $resourceControlContext->setResourceGroupName($this->groupName);
            $context->setResourceControlContext($resourceControlContext);
        }

        if ($this->resourceGroupTag !== null) {
            $context->setResourceGroupTag($this->resourceGroupTag);
        }
    }

    /**
     * @param array<string, mixed> $options
     *
     * @throws InvalidArgumentException
     */
    private static function readOption(array $options, string $key, string $label): ?string
    {
        if (!array_key_exists($key, $options)) {
            return null;
        }

        $value = $options[$key];
        if (!is_string($value)) {
            throw new InvalidArgumentException(sprintf(
                "%s['%s'] must be a string, %s given",
                $label,
                $key,
                get_debug_type($value),
            ));
        }
        if ($value === '') {
            throw new InvalidArgumentException(sprintf(
                "%s['%s'] must not be empty",
                $label,
                $key,
            ));
        }

        return $value;
    }

    private function assertGroupName(string $groupName): void
    {
        if (preg_match(self::RESOURCE_GROUP_NAME_PATTERN, $groupName) !== 1) {
            throw new InvalidArgumentException(sprintf(
                'Resource group name must be 1-%d characters of letters, digits, underscores or hyphens, got %s',
                self::MAX_RESOURCE_GROUP_NAME_LENGTH,
                var_export($groupName, true),
            ));
        }
    }

    private function assertTag(string $tag): void
    {
        if ($tag === '') {
            throw new InvalidArgumentException('Resource group tag must not be empty');
        }
    }
}
