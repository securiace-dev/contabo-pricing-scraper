<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Phase A.6.1 — amendment 8 (binding default exposure).
 *
 * The default seed exposure preset for a NEW `customer_configurable_product`.
 * Production exposure is admin-curated and preview-first: nothing is exposed to
 * the customer until an admin ticks it. This preset is the safe starting point.
 *
 * "Retail VPS Minimal" intent:
 *   - Image     → expose OS-category values ONLY. Panels/Apps/Blockchain hidden.
 *                 (Image is ONE WHMCS dropdown — exposure is per-CATEGORY via the
 *                 image_category_visibility map, not per-dimension.)
 *   - Region    → exposed, limited curated set (admin narrows the list).
 *   - Storage   → HIDDEN (use plan default; not meaningful to a retail buyer).
 *   - Backup    → optional, EXPOSED (Contabo Auto Backup, customer opt-in).
 *   - IPv4      → quantity exposed IF the unit price is known.
 *   - Bandwidth → HIDDEN.
 *   - Private Networking → HIDDEN.
 *
 * Dimension keys mirror the A.5.2 model: Image is ONE dimension; Networking is
 * split into three independent concerns (Bandwidth / IPv4 / Private Networking);
 * Region / Storage Type / Data Protection are one each.
 *
 * The return shape per dimension_key is:
 *   {expose_to_customer: bool, hidden: bool, default_value: ?string, note: string}
 * The Image entry additionally carries `image_category_visibility` mapping each
 * Contabo image category → visible/hidden.
 *
 * PHP 7.4 polyglot; all-static, no DB, no I/O.
 */
final class RetailVpsMinimalPreset
{
    public const NAME = 'Retail VPS Minimal';

    // Canonical dimension keys (must match DimensionParser / the link tables).
    public const DIM_IMAGE              = 'image';
    public const DIM_REGION             = 'region';
    public const DIM_STORAGE            = 'storage_type';
    public const DIM_BACKUP             = 'data_protection';
    public const DIM_IPV4               = 'ipv4';
    public const DIM_BANDWIDTH          = 'bandwidth';
    public const DIM_PRIVATE_NETWORKING = 'private_networking';

    // Contabo image categories (Image dimension is split visually by these).
    public const IMG_CAT_OS         = 'OS';
    public const IMG_CAT_PANELS     = 'Panels';
    public const IMG_CAT_APPS       = 'Apps';
    public const IMG_CAT_BLOCKCHAIN = 'Blockchain';

    public static function name(): string
    {
        return self::NAME;
    }

    /**
     * Default exposure flags per dimension_key for a new
     * customer_configurable_product under this preset.
     *
     * @param string $planSlug optional plan context. The preset is
     *        plan-agnostic in v1 (same flags for every plan); the param exists
     *        so advanced presets / per-plan tweaks can hook in later without an
     *        API change. ipv4UnitPriceKnown() may consult it in future.
     * @return array<string, array{expose_to_customer:bool, hidden:bool, default_value:?string, note:string}>
     */
    public static function exposureFor(string $planSlug = ''): array
    {
        $ipv4Exposed = self::ipv4UnitPriceKnown($planSlug);

        $map = [
            self::DIM_IMAGE => [
                'expose_to_customer' => true,
                'hidden'             => false,
                'default_value'      => null,
                'note'               => 'Expose OS-category images only; Panels/Apps/Blockchain hidden (see image_category_visibility).',
                // Per-category visibility WITHIN the single Image dropdown.
                'image_category_visibility' => [
                    self::IMG_CAT_OS         => ['expose_to_customer' => true,  'hidden' => false],
                    self::IMG_CAT_PANELS     => ['expose_to_customer' => false, 'hidden' => true],
                    self::IMG_CAT_APPS       => ['expose_to_customer' => false, 'hidden' => true],
                    self::IMG_CAT_BLOCKCHAIN => ['expose_to_customer' => false, 'hidden' => true],
                ],
            ],
            self::DIM_REGION => [
                'expose_to_customer' => true,
                'hidden'             => false,
                'default_value'      => null,
                'note'               => 'Region exposed (limited curated set); admin narrows the available list.',
            ],
            self::DIM_STORAGE => [
                'expose_to_customer' => false,
                'hidden'             => true,
                'default_value'      => null,
                'note'               => 'Storage hidden in Retail Minimal; plan default storage used.',
            ],
            self::DIM_BACKUP => [
                'expose_to_customer' => true,
                'hidden'             => false,
                'default_value'      => null,
                'note'               => 'Contabo Auto Backup optional (customer opt-in); exposed but not forced.',
            ],
            self::DIM_IPV4 => [
                'expose_to_customer' => $ipv4Exposed,
                'hidden'             => !$ipv4Exposed,
                'default_value'      => null,
                'note'               => $ipv4Exposed
                    ? 'IPv4 quantity exposed (unit price known).'
                    : 'IPv4 hidden until a unit price is known (amendment 8).',
            ],
            self::DIM_BANDWIDTH => [
                'expose_to_customer' => false,
                'hidden'             => true,
                'default_value'      => null,
                'note'               => 'Bandwidth hidden in Retail Minimal.',
            ],
            self::DIM_PRIVATE_NETWORKING => [
                'expose_to_customer' => false,
                'hidden'             => true,
                'default_value'      => null,
                'note'               => 'Private Networking hidden in Retail Minimal.',
            ],
        ];

        return $map;
    }

    /**
     * Convenience: the per-category visibility map for the Image dimension under
     * this preset.
     *
     * @return array<string, array{expose_to_customer:bool, hidden:bool}>
     */
    public static function imageCategoryVisibility(string $planSlug = ''): array
    {
        $image = self::exposureFor($planSlug)[self::DIM_IMAGE];
        /** @var array<string, array{expose_to_customer:bool, hidden:bool}> $vis */
        $vis = $image['image_category_visibility'] ?? [];
        return $vis;
    }

    /**
     * Whether IPv4 quantity should be exposed. v1: only when the unit price is
     * known. The preset has no live pricing context, so the conservative default
     * is TRUE (the syncer hides it again if it later finds no unit price). This
     * keeps the preset declarative; the hook point exists for per-plan logic.
     */
    private static function ipv4UnitPriceKnown(string $planSlug): bool
    {
        // Plan-agnostic in v1. Returning true expresses "expose IPv4 qty when a
        // unit price is known"; the syncer is responsible for confirming the
        // price exists before it actually exposes the option to customers.
        unset($planSlug);
        return true;
    }
}
