<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Sole read/write chokepoint for the addon-owned compatibility matrix table
 * (design §5):
 *
 *   mod_contabo_option_compatibility (plan_slug, dimension_key, value_key)
 *     → compatible_with_json / incompatible_with_json / required_values_json
 *     + min_value / max_value + source_snapshot_id + last_verified_at
 *
 * It encodes which configurable-option values may be combined for a given plan
 * (e.g. an image that is unavailable in a region, or an IPv4 count above the
 * provider limit) and is the gate {@see validateCombination()} consults before
 * a selection is allowed through to ordering / provisioning.
 *
 * Mirrors {@see ConfigOptionLinkRepository}: plain Capsule, upsert-by-unique-key
 * then re-read, every ->first() result cast to (array) (real WHMCS returns
 * stdClass, FakeCapsule returns arrays — cast religiously), a whitelist of
 * writable columns, and the *_json columns are LONGTEXT holding json_encode'd
 * payloads (never a native JSON type).
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion,
 * no named args, no str_starts_with.
 */
final class ConfigOptionCompatibilityRepository
{
    private const T_COMPAT = 'mod_contabo_option_compatibility';

    /**
     * Upsert a compatibility rule keyed by (plan_slug, dimension_key, value_key).
     *
     * $rule may carry any of:
     *   - 'compatible_with'   list<string> → compatible_with_json
     *   - 'incompatible_with' list<string> → incompatible_with_json
     *   - 'required_values'   list<string> → required_values_json
     *   - 'min_value'         int|null
     *   - 'max_value'         int|null
     *   - 'source_snapshot_id' int|null
     * Any other key is ignored (chokepoint whitelist). last_verified_at is
     * always stamped. Returns the full re-read row.
     *
     * @param array<string,mixed> $rule
     * @return array<string,mixed>
     */
    public function upsertRule(string $planSlug, string $dimensionKey, string $valueKey, array $rule): array
    {
        $now = date('Y-m-d H:i:s');
        $key = [
            'plan_slug'     => $planSlug,
            'dimension_key' => $dimensionKey,
            'value_key'     => $valueKey,
        ];

        $values = ['last_verified_at' => $now];

        // JSON list columns: only written when explicitly supplied so an
        // omitted key keeps whatever the row already had.
        $jsonMap = [
            'compatible_with'   => 'compatible_with_json',
            'incompatible_with' => 'incompatible_with_json',
            'required_values'   => 'required_values_json',
        ];
        foreach ($jsonMap as $ruleKey => $column) {
            if (array_key_exists($ruleKey, $rule)) {
                $values[$column] = $this->encodeList($rule[$ruleKey]);
            }
        }

        // Nullable integer bounds + provenance.
        foreach (['min_value', 'max_value', 'source_snapshot_id'] as $intKey) {
            if (array_key_exists($intKey, $rule)) {
                $values[$intKey] = $rule[$intKey] === null ? null : (int) $rule[$intKey];
            }
        }

        Capsule::table(self::T_COMPAT)->updateOrInsert($key, $values);

        return $this->find($planSlug, $dimensionKey, $valueKey) ?? [];
    }

    /**
     * Read the rule for one (plan, dimension, value), or null when none exists.
     *
     * @return array<string,mixed>|null
     */
    public function find(string $planSlug, string $dimensionKey, string $valueKey): ?array
    {
        $r = Capsule::table(self::T_COMPAT)
            ->where('plan_slug', $planSlug)
            ->where('dimension_key', $dimensionKey)
            ->where('value_key', $valueKey)
            ->first();

        return $r !== null ? (array) $r : null;
    }

    /**
     * All compatibility rules for one plan, in stable id order. Mirrors
     * {@see ConfigOptionCapabilityRepository::listForPlan}. Used by the
     * compatibility editor to overlay saved rules onto the plan's dimensions.
     *
     * @return list<array<string,mixed>>
     */
    public function listForPlan(string $planSlug): array
    {
        $rows = Capsule::table(self::T_COMPAT)
            ->where('plan_slug', $planSlug)
            ->orderBy('id')
            ->get();

        $out = [];
        foreach ($rows as $row) {
            $out[] = (array) $row;
        }
        return $out;
    }

    /**
     * Validate a set of configurable-option selections against the plan's
     * compatibility matrix. Deterministic and side-effect-free (read-only).
     *
     * $selections is a list of:
     *   ['dimension_key' => string, 'value_key' => string, 'qty' => int (optional)]
     *
     * For each selected value that has a rule, three checks run:
     *   (a) incompatible_with — none of the OTHER selected value_keys may appear
     *       in this value's incompatible set;
     *   (b) required_values   — every value_key it requires must be present in
     *       the selection;
     *   (c) min_value/max_value — when set, the selected qty (default 1) must
     *       fall within [min_value, max_value].
     *
     * A value with no rule imposes no constraint (treated as valid).
     *
     * Returns:
     *   [
     *     'valid'      => bool,
     *     'violations' => list<[
     *         'dimension_key' => string,
     *         'value_key'     => string,
     *         'reason'        => string,   // 'incompatible' | 'missing_required' | 'qty_out_of_range'
     *         'detail'        => string,
     *     ]>,
     *   ]
     *
     * @param list<array<string,mixed>> $selections
     * @return array{valid:bool,violations:list<array<string,string>>}
     */
    public function validateCombination(string $planSlug, array $selections): array
    {
        // Normalise the selection up front so the checks are pure list ops.
        $selected = [];        // list of value_keys present
        $normalized = [];      // list of ['dimension_key','value_key','qty']
        foreach ($selections as $sel) {
            $dim = isset($sel['dimension_key']) ? (string) $sel['dimension_key'] : '';
            $val = isset($sel['value_key']) ? (string) $sel['value_key'] : '';
            $qty = isset($sel['qty']) ? (int) $sel['qty'] : 1;
            $normalized[] = ['dimension_key' => $dim, 'value_key' => $val, 'qty' => $qty];
            $selected[] = $val;
        }

        $violations = [];

        foreach ($normalized as $sel) {
            $rule = $this->find($planSlug, $sel['dimension_key'], $sel['value_key']);
            if ($rule === null) {
                continue; // no rule → no constraint
            }

            // (a) incompatible_with: any OTHER selected value listed here is a clash.
            $incompatible = $this->decodeList($rule['incompatible_with_json'] ?? null);
            foreach ($incompatible as $bad) {
                if ($bad === $sel['value_key']) {
                    continue; // ignore self-references defensively
                }
                if (in_array($bad, $selected, true)) {
                    $violations[] = [
                        'dimension_key' => $sel['dimension_key'],
                        'value_key'     => $sel['value_key'],
                        'reason'        => 'incompatible',
                        'detail'        => $sel['value_key'] . ' is incompatible with ' . $bad,
                    ];
                }
            }

            // (b) required_values: each must be present in the selection.
            $required = $this->decodeList($rule['required_values_json'] ?? null);
            foreach ($required as $req) {
                if (!in_array($req, $selected, true)) {
                    $violations[] = [
                        'dimension_key' => $sel['dimension_key'],
                        'value_key'     => $sel['value_key'],
                        'reason'        => 'missing_required',
                        'detail'        => $sel['value_key'] . ' requires ' . $req,
                    ];
                }
            }

            // (c) qty bounds, only when the column is set.
            $min = $this->intOrNull($rule['min_value'] ?? null);
            $max = $this->intOrNull($rule['max_value'] ?? null);
            if ($min !== null && $sel['qty'] < $min) {
                $violations[] = [
                    'dimension_key' => $sel['dimension_key'],
                    'value_key'     => $sel['value_key'],
                    'reason'        => 'qty_out_of_range',
                    'detail'        => 'qty ' . $sel['qty'] . ' is below minimum ' . $min,
                ];
            }
            if ($max !== null && $sel['qty'] > $max) {
                $violations[] = [
                    'dimension_key' => $sel['dimension_key'],
                    'value_key'     => $sel['value_key'],
                    'reason'        => 'qty_out_of_range',
                    'detail'        => 'qty ' . $sel['qty'] . ' is above maximum ' . $max,
                ];
            }
        }

        return [
            'valid'      => $violations === [],
            'violations' => $violations,
        ];
    }

    /**
     * Encode a list payload for a *_json column. A non-array is coerced to an
     * empty list so the column always holds a JSON array string.
     *
     * @param mixed $value
     */
    private function encodeList($value): string
    {
        $list = is_array($value) ? array_values($value) : [];
        $encoded = json_encode($list, JSON_UNESCAPED_SLASHES);
        return $encoded === false ? '[]' : $encoded;
    }

    /**
     * Decode a *_json column back to a list of strings. Null / empty / invalid
     * blobs decode to an empty list (no constraint).
     *
     * @param mixed $raw
     * @return list<string>
     */
    private function decodeList($raw): array
    {
        if ($raw === null || $raw === '') {
            return [];
        }
        $decoded = json_decode((string) $raw, true);
        if (!is_array($decoded)) {
            return [];
        }
        $out = [];
        foreach ($decoded as $item) {
            $out[] = (string) $item;
        }
        return $out;
    }

    /**
     * Treat NULL / empty string as "unset"; everything else as an int bound.
     *
     * @param mixed $value
     */
    private function intOrNull($value): ?int
    {
        if ($value === null || $value === '') {
            return null;
        }
        return (int) $value;
    }
}
