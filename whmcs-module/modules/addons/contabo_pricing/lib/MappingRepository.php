<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * The single guarded write path into `mod_contabo_mapping`.
 *
 * Phase A.5 (schema v3) dropped the three legacy per-cycle boolean columns in
 * favour of two integer bitmasks (`catalog_cycles_mask` + `renewal_cycles_mask`).
 * Any controller / AJAX path that still tried to write the dropped columns
 * raised SQLSTATE[42S22] (Unknown column) on the migrated schema. This
 * repository is the only place mapping rows are written, and it enforces a
 * strict column whitelist so a stray legacy key can never reach SQL again.
 *
 * PHP 7.4 polyglot: no readonly, no constructor promotion, no match, no
 * str_starts_with, no named args, no non-capturing catch.
 */
final class MappingRepository
{
    /** The ONLY columns that may be written to mod_contabo_mapping. */
    private const WRITABLE = [
        'profile_id', 'product_id', 'product_group_id',
        'catalog_cycles_mask', 'renewal_cycles_mask',
        'markup_overrides_json', 'setup_fee_overrides_json',
        'respect_disabled_cycles', 'overwrite_free_cycles', 'sync_setup_fees',
        'rounding_mode', 'active',
    ];

    /** Columns coerced to an integer bitmask in [0, CycleSet::MASK_MAX]. */
    private const MASK_COLUMNS = [
        'catalog_cycles_mask', 'renewal_cycles_mask',
    ];

    /** Columns coerced to a 0/1 integer. */
    private const BOOL_COLUMNS = [
        'respect_disabled_cycles', 'overwrite_free_cycles', 'sync_setup_fees', 'active',
    ];

    /** Columns coerced to a valid JSON string. */
    private const JSON_COLUMNS = [
        'markup_overrides_json', 'setup_fee_overrides_json',
    ];

    private const TABLE = 'mod_contabo_mapping';

    /**
     * Whitelist-filter $data, attach timestamps, then updateOrInsert keyed on
     * (profile_id, product_id). Any key outside WRITABLE is dropped; a
     * forbidden legacy `apply_to_*` key triggers a logActivity dev warning so
     * regressions surface, then it is dropped.
     *
     * @param array<string,mixed> $data
     * @return int  the mapping row id
     */
    public function createOrUpdate(array $data): int
    {
        $data = $this->stripForbiddenLegacyKeys($data);

        // Strict whitelist: nothing outside WRITABLE survives.
        $row = array_intersect_key($data, array_flip(self::WRITABLE));
        $row = $this->coerceTypes($row);

        $profileId = (int) ($row['profile_id'] ?? 0);
        $productId = (int) ($row['product_id'] ?? 0);
        $row['profile_id'] = $profileId;
        $row['product_id'] = $productId;

        $now = date('Y-m-d H:i:s');

        // Match the (profile_id, product_id) row. updateOrInsert will UPDATE if
        // present (created_at preserved) or INSERT a fresh row (created_at set).
        $existing = $this->findByProfileAndProduct($profileId, $productId);

        $values = $row;
        $values['updated_at'] = $now;
        if ($existing === null) {
            $values['created_at'] = $now;
        }

        Capsule::table(self::TABLE)->updateOrInsert(
            ['profile_id' => $profileId, 'product_id' => $productId],
            $values
        );

        $id = Capsule::table(self::TABLE)
            ->where('profile_id', $profileId)
            ->where('product_id', $productId)
            ->value('id');

        return (int) $id;
    }

    /** @return array<string,mixed>|null */
    public function findByProfileAndProduct(int $profileId, int $productId): ?array
    {
        $found = Capsule::table(self::TABLE)
            ->where('profile_id', $profileId)
            ->where('product_id', $productId)
            ->first();

        if ($found === null) {
            return null;
        }

        return (array) $found;
    }

    /**
     * Detect any forbidden legacy `apply_to_*` key, log a dev warning so we
     * catch regressions, then drop it before it can reach SQL.
     *
     * @param array<string,mixed> $data
     * @return array<string,mixed>
     */
    private function stripForbiddenLegacyKeys(array $data): array
    {
        foreach (array_keys($data) as $key) {
            $name = (string) $key;
            if (strpos($name, 'apply_to_') === 0) {
                if (function_exists('logActivity')) {
                    logActivity(
                        'Contabo Pricing: MappingRepository dropped forbidden legacy key "'
                        . $name . '" from a runtime mapping payload (schema v3 uses cycle masks).'
                    );
                }
                unset($data[$key]);
            }
        }
        return $data;
    }

    /**
     * Defensive type coercion so a sloppy caller can never poison the row:
     *   - masks  → int clamped to [0, CycleSet::MASK_MAX]
     *   - bools  → 0/1 int
     *   - json   → valid JSON string (arrays are json_encode'd; invalid blobs
     *              collapse to '{}')
     *   - product_group_id → nullable int
     *   - rounding_mode    → string
     *
     * @param array<string,mixed> $row
     * @return array<string,mixed>
     */
    private function coerceTypes(array $row): array
    {
        foreach (self::MASK_COLUMNS as $col) {
            if (array_key_exists($col, $row)) {
                $row[$col] = $this->clampMask($row[$col]);
            }
        }

        foreach (self::BOOL_COLUMNS as $col) {
            if (array_key_exists($col, $row)) {
                $row[$col] = !empty($row[$col]) ? 1 : 0;
            }
        }

        foreach (self::JSON_COLUMNS as $col) {
            if (array_key_exists($col, $row)) {
                $row[$col] = $this->coerceJsonString($row[$col]);
            }
        }

        if (array_key_exists('product_group_id', $row)) {
            $row['product_group_id'] = ($row['product_group_id'] === null || $row['product_group_id'] === '')
                ? null
                : (int) $row['product_group_id'];
        }

        if (array_key_exists('rounding_mode', $row)) {
            $row['rounding_mode'] = (string) $row['rounding_mode'];
        }

        return $row;
    }

    /**
     * Coerce a mask value to an int in [0, CycleSet::MASK_MAX].
     *
     * @param mixed $raw
     */
    private function clampMask($raw): int
    {
        $val = is_numeric($raw) ? (int) $raw : 0;
        if ($val < 0) {
            $val = 0;
        }
        if ($val > CycleSet::MASK_MAX) {
            $val = CycleSet::MASK_MAX;
        }
        return $val;
    }

    /**
     * Coerce an arbitrary JSON field to a valid JSON string. Arrays / objects
     * are json_encode'd; a string is validated (and re-encoded as '{}' if it
     * isn't decodable); anything else collapses to '{}'.
     *
     * @param mixed $raw
     */
    private function coerceJsonString($raw): string
    {
        if (is_array($raw)) {
            $encoded = json_encode($raw, JSON_UNESCAPED_SLASHES);
            return $encoded === false ? '{}' : $encoded;
        }

        if (is_string($raw)) {
            if ($raw === '') {
                return '{}';
            }
            $decoded = json_decode($raw, true);
            if ($decoded === null && strtolower(trim($raw)) !== 'null') {
                // Not valid JSON — refuse to store a malformed blob.
                return '{}';
            }
            return $raw;
        }

        return '{}';
    }
}
