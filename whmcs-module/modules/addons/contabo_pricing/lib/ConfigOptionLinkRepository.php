<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * A.6.3 — sole read/write chokepoint for the addon-owned configurable-option
 * link tables (design §8):
 *
 *   - mod_contabo_config_group_link        (profile, product, group_key) → whmcs_group_id
 *   - mod_contabo_config_option_link       (profile, dimension_key)      → whmcs_option_id + exposure flags
 *   - mod_contabo_config_option_value_link (option_link_id, value_key)   → whmcs_sub_id + EUR delta
 *
 * This is what makes the apply path IDEMPOTENT and OWNERSHIP-SCOPED: every WHMCS
 * config object the addon creates is recorded here against its Contabo key, so a
 * re-apply reuses the same WHMCS id (no duplicates) and the addon only ever
 * touches objects it created. Mirrors {@see MappingRepository} — plain Capsule,
 * upsert-by-unique-key + re-read for the id.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ConfigOptionLinkRepository
{
    private const T_GROUP  = 'mod_contabo_config_group_link';
    private const T_OPTION = 'mod_contabo_config_option_link';
    private const T_VALUE  = 'mod_contabo_config_option_value_link';

    /**
     * Admin-curated exposure flags on an option link (design §3). Only these
     * keys are writable through {@see upsertOptionLink}; anything else is
     * dropped, so a caller can never set an unknown column.
     *
     * @var list<string>
     */
    public const EXPOSURE_FLAGS = [
        'expose_to_customer', 'hidden', 'deprecated', 'allowed_for_new_orders',
        'allowed_on_create', 'allowed_post_provision', 'allowed_on_reinstall',
        'allowed_on_upgrade', 'allowed_on_downgrade', 'pass_to_provisioning',
        'destructive_if_changed', 'requires_confirmation', 'requires_admin_approval',
    ];

    // ── group link ──────────────────────────────────────────────────────────

    /**
     * Upsert a group link by (profile_id, whmcs_product_id, group_key). When
     * $whmcsGroupId is given it is recorded (the id of the WHMCS group the
     * adapter just created/found). Returns the full row.
     *
     * @return array<string,mixed>
     */
    public function upsertGroupLink(int $profileId, int $productId, string $groupKey, ?int $whmcsGroupId = null): array
    {
        $now = date('Y-m-d H:i:s');
        $key = ['profile_id' => $profileId, 'whmcs_product_id' => $productId, 'group_key' => $groupKey];

        $values = ['enabled' => 1, 'updated_at' => $now];
        if ($whmcsGroupId !== null) {
            $values['whmcs_group_id'] = $whmcsGroupId;
        }
        if ($this->findGroupLink($profileId, $productId, $groupKey) === null) {
            $values['created_at'] = $now;
        }

        Capsule::table(self::T_GROUP)->updateOrInsert($key, $values);
        return $this->findGroupLink($profileId, $productId, $groupKey) ?? [];
    }

    /** @return array<string,mixed>|null */
    public function findGroupLink(int $profileId, int $productId, string $groupKey): ?array
    {
        $r = Capsule::table(self::T_GROUP)
            ->where('profile_id', $profileId)
            ->where('whmcs_product_id', $productId)
            ->where('group_key', $groupKey)
            ->first();
        return $r !== null ? (array) $r : null;
    }

    // ── option link ─────────────────────────────────────────────────────────

    /**
     * Upsert an option link by (profile_id, dimension_key). Exposure flags are
     * whitelisted to {@see EXPOSURE_FLAGS}; omit them to keep the migration
     * defaults (preview-first: nothing exposed until ticked).
     *
     * @param array<string,bool|int> $exposure
     * @return array<string,mixed>
     */
    public function upsertOptionLink(int $profileId, string $dimensionKey, int $optiontype, ?int $whmcsOptionId = null, array $exposure = []): array
    {
        $now = date('Y-m-d H:i:s');
        $key = ['profile_id' => $profileId, 'dimension_key' => $dimensionKey];

        $values = ['optiontype' => $optiontype, 'enabled' => 1, 'updated_at' => $now];
        if ($whmcsOptionId !== null) {
            $values['whmcs_option_id'] = $whmcsOptionId;
        }
        foreach (self::EXPOSURE_FLAGS as $flag) {
            if (array_key_exists($flag, $exposure)) {
                $values[$flag] = $exposure[$flag] ? 1 : 0;
            }
        }
        if ($this->findOptionLink($profileId, $dimensionKey) === null) {
            $values['created_at'] = $now;
        }

        Capsule::table(self::T_OPTION)->updateOrInsert($key, $values);
        return $this->findOptionLink($profileId, $dimensionKey) ?? [];
    }

    /** @return array<string,mixed>|null */
    public function findOptionLink(int $profileId, string $dimensionKey): ?array
    {
        $r = Capsule::table(self::T_OPTION)
            ->where('profile_id', $profileId)
            ->where('dimension_key', $dimensionKey)
            ->first();
        return $r !== null ? (array) $r : null;
    }

    // ── value link ──────────────────────────────────────────────────────────

    /**
     * Upsert a value link by (option_link_id, contabo_value_key). The
     * contabo_label is the round-trip key provisioning uses to map a WHMCS
     * sub-option back to a Contabo value (§17).
     *
     * @return array<string,mixed>
     */
    public function upsertValueLink(
        int $optionLinkId,
        string $valueKey,
        string $label,
        ?int $whmcsSubId = null,
        bool $isDefault = false,
        float $monthlyEurDelta = 0.0
    ): array {
        $now = date('Y-m-d H:i:s');
        $key = ['option_link_id' => $optionLinkId, 'contabo_value_key' => $valueKey];

        $values = [
            'contabo_label'     => mb_substr($label, 0, 190),
            'is_default'        => $isDefault ? 1 : 0,
            'monthly_eur_delta' => $monthlyEurDelta,
            'updated_at'        => $now,
        ];
        if ($whmcsSubId !== null) {
            $values['whmcs_sub_id'] = $whmcsSubId;
        }
        if ($this->findValueLink($optionLinkId, $valueKey) === null) {
            $values['created_at'] = $now;
        }

        Capsule::table(self::T_VALUE)->updateOrInsert($key, $values);
        return $this->findValueLink($optionLinkId, $valueKey) ?? [];
    }

    /** @return array<string,mixed>|null */
    public function findValueLink(int $optionLinkId, string $valueKey): ?array
    {
        $r = Capsule::table(self::T_VALUE)
            ->where('option_link_id', $optionLinkId)
            ->where('contabo_value_key', $valueKey)
            ->first();
        return $r !== null ? (array) $r : null;
    }

    /**
     * Round-trip lookup used by provisioning (§17): given a WHMCS sub-option id,
     * return its Contabo value link (label + key).
     *
     * @return array<string,mixed>|null
     */
    public function findValueLinkByWhmcsSubId(int $whmcsSubId): ?array
    {
        $r = Capsule::table(self::T_VALUE)->where('whmcs_sub_id', $whmcsSubId)->first();
        return $r !== null ? (array) $r : null;
    }
}
