<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Mode-aware profile identity: slug + canonical fingerprint.
 *
 * A profile's *identity* is the set of fields that make two profiles "the
 * same thing". The slug is the human/URL key; the fingerprint is a stable
 * sha1 over a canonical, ksort-ed projection of the identity fields so that
 * two profiles with the same slug can be compared structurally (same config
 * → load existing; different config → conflict).
 *
 * CRITICAL identity rule — Image is ONE value, not four:
 *   The Contabo configurator exposes OS / App / Control Panel / Blockchain as
 *   four *categories* of a single mutually-exclusive **Image** choice. A
 *   profile can pick exactly one image (e.g. "Ubuntu 24.04", or a composite
 *   "cPanel on AlmaLinux"). The identity projection therefore collapses any
 *   Image:* selection keys into a single canonical `image` value — it must
 *   NEVER emit separate os/app/controlpanel/blockchain keys, or two profiles
 *   that picked different *categories* of the same logical image would look
 *   structurally different when they are not.
 *
 * Modes:
 *   - fixed_admin_profile         — fully implemented here. The admin pins
 *                                   every dimension; identity = the concrete
 *                                   chosen values.
 *   - customer_configurable_product — RESERVED for A.6. Identity is *shape*-
 *                                   based (which options are exposed, schema
 *                                   hash, default-value hash) NOT the concrete
 *                                   selectable values, because the customer
 *                                   chooses those at order time. Returns a
 *                                   documented placeholder projection only.
 */
final class ProfileIdentityResolver
{
    public const MODE_FIXED        = 'fixed_admin_profile';
    public const MODE_CONFIGURABLE = 'customer_configurable_product';

    /**
     * Build the kebab-cased slug from identity input.
     *
     * Accepts the same associative input the profile create path uses:
     *   plan_slug (required), period_months, region, os.
     * (Moved verbatim from ProfileManager::buildSlug — the plan_slug case is
     * preserved; only region/os are lowercased.)
     *
     * @param array<string,mixed> $input
     */
    public static function buildSlug(array $input): string
    {
        $planSlug = (string) ($input['plan_slug'] ?? '');
        $period   = (int) ($input['period_months'] ?? 0);
        $region   = (string) ($input['region'] ?? '');
        $os        = (string) ($input['os'] ?? '');

        $bits = [$planSlug, $period . 'mo'];
        if ($region !== '') { $bits[] = strtolower($region); }
        if ($os !== '')     { $bits[] = strtolower($os); }
        $slug = implode('-', $bits);
        $slug = preg_replace('~[^a-z0-9-]+~i', '-', $slug) ?? $slug;
        return trim((string) $slug, '-');
    }

    /**
     * sha1 over the canonical (ksort-ed) identity projection.
     *
     * sha1() is 40 hex chars; the column is CHAR(64) so it fits with room to
     * spare. We store the bare 40-char digest (no padding) — comparisons are
     * exact-string so padding would only hurt.
     *
     * @param array<string,mixed> $input must include (or default) 'profile_mode'
     */
    public static function buildFingerprint(array $input): string
    {
        $projection = self::identityProjection($input);
        return sha1((string) json_encode($projection));
    }

    /**
     * The canonical identity projection: a ksort-ed associative array that is
     * both hashed (buildFingerprint) AND stored verbatim in
     * profile_identity_json so future re-fingerprinting is auditable.
     *
     * Dispatches on profile_mode. Unknown modes fall back to the fixed
     * projection (safest: it captures concrete values).
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    public static function identityProjection(array $input): array
    {
        $mode = (string) ($input['profile_mode'] ?? self::MODE_FIXED);

        if ($mode === self::MODE_CONFIGURABLE) {
            $projection = self::configurableProjection($input);
        } else {
            $projection = self::fixedProjection($input);
        }

        ksort($projection);
        return $projection;
    }

    /**
     * fixed_admin_profile identity projection.
     *
     * Captures the concrete pinned values:
     *   profile_mode, plan_slug, period_months, image (collapsed single value),
     *   region, storage, data_protection (backup), networking,
     *   pricing_strategy, tax_strategy.
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function fixedProjection(array $input): array
    {
        $options = self::optionsOf($input);

        return [
            'profile_mode'    => self::MODE_FIXED,
            'plan_slug'       => (string) ($input['plan_slug'] ?? ''),
            'period_months'   => (int) ($input['period_months'] ?? 0),
            // Image is ONE mutually-exclusive choice (OS|App|ControlPanel|
            // Blockchain are categories of it). Collapsed to a single value.
            'image'           => self::collapseImage($input, $options),
            'region'          => self::canonValue($input['region'] ?? self::selectionLabel($options, 'Region')),
            'storage'         => self::dimensionValue($options, ['Storage', 'Disk', 'SSD', 'NVMe']),
            'data_protection' => self::dimensionValue($options, ['Backup', 'Data Protection', 'DataProtection', 'Snapshot']),
            'networking'      => self::dimensionValue($options, ['Networking', 'Network', 'Bandwidth', 'IP', 'IPv4', 'Private Networking']),
            'pricing_strategy'=> self::strategyValue($input, 'pricing_strategy', ['sync_strategy']),
            'tax_strategy'    => self::strategyValue($input, 'tax_strategy', ['gst', 'gst18', 'apply_gst18']),
        ];
    }

    /**
     * customer_configurable_product identity projection — RESERVED (A.6).
     *
     * Deliberately SHAPE-shaped, not value-shaped: a configurable product's
     * identity is "which options does it expose and what are the defaults",
     * NOT the concrete OS/region the customer eventually picks. Two configurable
     * products with the same exposed-option schema + defaults + strategies are
     * the same product even if customers later pick different images/regions.
     *
     * This is a documented placeholder: it emits schema/hash-shaped keys and
     * intentionally does NOT include concrete selectable values (no 'image',
     * no 'region' literal). Full implementation lands in A.6.
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function configurableProjection(array $input): array
    {
        $exposed  = self::exposedOptionSchema($input);
        $defaults = self::defaultValues($input);

        return [
            'profile_mode'              => self::MODE_CONFIGURABLE,
            'plan_slug'                 => (string) ($input['plan_slug'] ?? ''),
            // WHMCS product scope this configurable profile is bound to.
            'product_scope'             => self::canonValue($input['product_id'] ?? $input['product_scope'] ?? ''),
            // Hash of the *shape* of exposed options (the option keys + their
            // allowed-value sets), NOT the chosen value.
            'exposed_option_schema_hash'=> self::hashShape($exposed),
            'default_values_hash'       => self::hashShape($defaults),
            'pricing_strategy_hash'     => self::hashShape(self::strategyValue($input, 'pricing_strategy', ['sync_strategy'])),
            'tax_strategy_hash'         => self::hashShape(self::strategyValue($input, 'tax_strategy', ['gst', 'gst18', 'apply_gst18'])),
        ];
    }

    // ───────────────────────── helpers ─────────────────────────

    /**
     * Collapse the Image selection into one canonical value.
     *
     * Looks for the single chosen image across the mutually-exclusive
     * categories. Precedence is deterministic (sorted by category key) so the
     * same logical image always hashes the same regardless of which category
     * the configurator reported it under. If an explicit top-level `os` was
     * supplied (legacy fixed-image path), it wins as the image label.
     *
     * @param array<string,mixed> $input
     * @param array<string,mixed> $options
     */
    private static function collapseImage(array $input, array $options): string
    {
        // Gather every Image:* selection from the options payload.
        $imageCats = [];
        foreach ($options as $key => $val) {
            if (is_string($key) && stripos($key, 'Image') === 0) {
                $label = self::valueLabel($val);
                if ($label !== '') {
                    $imageCats[$key] = $label;
                }
            }
        }
        if ($imageCats !== []) {
            // Deterministic single value: category:label pairs, ksort-ed and
            // joined. Because exactly one image is expected this is normally a
            // single pair, but joining is robust to composite images
            // (e.g. control-panel-on-OS) without splitting into 4 keys.
            ksort($imageCats);
            $parts = [];
            foreach ($imageCats as $cat => $label) {
                $parts[] = self::canonValue($cat) . ':' . self::canonValue($label);
            }
            return implode('|', $parts);
        }
        // Fall back to explicit os/image input keys.
        $os = self::canonValue($input['image'] ?? $input['os'] ?? '');
        return $os;
    }

    /**
     * Resolve a dimension by trying several candidate selection keys.
     * Returns the canonicalised label of the first match, or '' if none.
     *
     * @param array<string,mixed> $options
     * @param list<string>        $candidates
     */
    private static function dimensionValue(array $options, array $candidates): string
    {
        foreach ($candidates as $cand) {
            if (array_key_exists($cand, $options)) {
                $label = self::valueLabel($options[$cand]);
                if ($label !== '') {
                    return self::canonValue($label);
                }
            }
        }
        // Case-insensitive contains match as a fallback.
        foreach ($options as $key => $val) {
            if (!is_string($key)) { continue; }
            foreach ($candidates as $cand) {
                if (stripos($key, $cand) !== false) {
                    $label = self::valueLabel($val);
                    if ($label !== '') {
                        return self::canonValue($label);
                    }
                }
            }
        }
        return '';
    }

    /**
     * Resolve a strategy value from an explicit input key, falling back to
     * named aliases (e.g. tax_strategy ← apply_gst18). Returns canonical string.
     *
     * @param array<string,mixed> $input
     * @param list<string>        $aliases
     */
    private static function strategyValue(array $input, string $primary, array $aliases): string
    {
        if (array_key_exists($primary, $input) && $input[$primary] !== '' && $input[$primary] !== null) {
            return self::canonValue($input[$primary]);
        }
        foreach ($aliases as $alias) {
            if (array_key_exists($alias, $input) && $input[$alias] !== '' && $input[$alias] !== null) {
                return self::canonValue($input[$alias]);
            }
        }
        return '';
    }

    /** @param array<string,mixed> $options */
    private static function selectionLabel(array $options, string $key): string
    {
        return array_key_exists($key, $options) ? self::valueLabel($options[$key]) : '';
    }

    /**
     * Extract the configurator options payload, regardless of whether it
     * arrives as an already-decoded array or a JSON string.
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function optionsOf(array $input): array
    {
        $raw = $input['options'] ?? null;
        if (is_array($raw)) {
            return $raw;
        }
        if (is_string($raw) && $raw !== '') {
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                return $decoded;
            }
        }
        return [];
    }

    /**
     * A selection value is either a scalar label or a {label,...} array.
     *
     * @param mixed $val
     */
    private static function valueLabel($val): string
    {
        if (is_array($val)) {
            if (isset($val['label'])) { return (string) $val['label']; }
            if (isset($val['value'])) { return (string) $val['value']; }
            return '';
        }
        if (is_scalar($val)) { return (string) $val; }
        return '';
    }

    /**
     * Canonicalise a scalar identity token: trim + lowercase so cosmetic
     * differences don't change the fingerprint.
     *
     * @param mixed $val
     */
    private static function canonValue($val): string
    {
        if (is_array($val)) {
            $val = self::valueLabel($val);
        }
        if (!is_scalar($val)) {
            return '';
        }
        return strtolower(trim((string) $val));
    }

    /**
     * Reserved-mode helper: project the *shape* of exposed options. In A.6
     * this reads the configurable-option schema; here it best-effort reads an
     * `exposed_options` input key (option-name => allowed values) so the
     * placeholder is structurally honest without depending on A.6 tables.
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function exposedOptionSchema(array $input): array
    {
        $exposed = $input['exposed_options'] ?? [];
        if (!is_array($exposed)) { return []; }
        $shape = [];
        foreach ($exposed as $optName => $allowed) {
            // Keep only the *keys* / allowed-value set — never a chosen value.
            $shape[(string) $optName] = is_array($allowed)
                ? array_values(array_map([self::class, 'canonValue'], $allowed))
                : [];
        }
        ksort($shape);
        return $shape;
    }

    /**
     * Reserved-mode helper: project the default-values shape (option => default).
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function defaultValues(array $input): array
    {
        $defaults = $input['default_values'] ?? [];
        if (!is_array($defaults)) { return []; }
        $out = [];
        foreach ($defaults as $k => $v) {
            $out[(string) $k] = self::canonValue($v);
        }
        ksort($out);
        return $out;
    }

    /**
     * Stable sha1 over a shape (array or scalar). Used for the reserved-mode
     * hash-of-shape sub-keys.
     *
     * @param mixed $shape
     */
    private static function hashShape($shape): string
    {
        return sha1((string) json_encode($shape));
    }
}
