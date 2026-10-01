<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use ContaboPricing\CatalogImportService;

/**
 * PHP port of build() in src/api/catalog.rs: wraps scraped plans (and optional
 * configuration/compatibility data) into the versioned, hashed catalog
 * envelope that CatalogImportService::import() consumes.
 *
 * Hashing uses CatalogImportService::canonicalJson(), the same canonical form
 * the importer re-verifies, so a built envelope always passes import checks.
 * One known representational difference from the Rust producer: PHP decodes an
 * empty JSON object as an empty array, so an empty map re-encodes as `[]`
 * rather than `{}`. Both sides of the importer's hash check are PHP, so this
 * is self-consistent; it only matters when comparing against Rust-produced
 * hashes of payloads that contain empty objects.
 */
final class CatalogEnvelopeBuilder
{
    public const SCHEMA_VERSION = '1.0';

    /**
     * @param list<array<string,mixed>> $plans
     * @param array<string,mixed> $configurations the Rust `configs` document ({plans:{url:{slug,options:{dim:[choice]}}}})
     * @param array<string,mixed> $compatibility the Rust `option_catalog` document
     * @return array<string,mixed>
     */
    public function build(
        array $plans,
        array $configurations,
        array $compatibility,
        string $observedAt,
        string $sourceVersion,
        ?string $catalogVersionOverride = null
    ): array {
        $items = [];
        $profiles = [];

        foreach ($plans as $plan) {
            $slug = self::pickString($plan, ['product_slug', 'slug']) ?? 'unknown-plan';
            $label = self::pickString($plan, ['product_name', 'title']) ?? $slug;
            $providerId = self::observedProviderId($plan);

            $items[] = self::catalogItem(
                'plan:' . $slug,
                $providerId,
                'plan',
                $label,
                $observedAt,
                $plan,
                ['family' => $plan['family'] ?? null]
            );

            $profiles[] = [
                'machine_id' => 'profile:' . $slug . ':default',
                'plan_machine_id' => 'plan:' . $slug,
                'label' => $label,
                'source_observed_at' => $observedAt,
                'availability_state' => 'observed',
                'deprecated' => false,
            ];
        }

        $this->appendConfigurationItems($configurations, $observedAt, $items);

        $payload = [
            'schema_version' => self::SCHEMA_VERSION,
            'source_version' => $sourceVersion,
            'source_observed_at' => $observedAt,
            'effective_at' => $observedAt,
            'plans' => $plans,
            'profiles' => $profiles,
            'items' => $items,
            'compatibility' => $compatibility,
            'configurations' => $configurations,
        ];

        $versionSeedHash = hash('sha256', CatalogImportService::canonicalJson($payload));
        $timestampKey = substr((string) preg_replace('/[^0-9]/', '', $observedAt), 0, 14);
        $catalogVersion = $catalogVersionOverride !== null && $catalogVersionOverride !== ''
            ? $catalogVersionOverride
            : 'catalog-' . ($timestampKey === '' ? 'unknown' : $timestampKey) . '-' . substr($versionSeedHash, 0, 16);

        $envelope = $payload;
        foreach ($envelope['items'] as $i => $item) {
            $envelope['items'][$i]['catalog_version'] = $catalogVersion;
        }
        $envelope['catalog_version'] = $catalogVersion;
        $envelope['payload_hash'] = hash('sha256', CatalogImportService::canonicalJson($envelope));

        return $envelope;
    }

    /**
     * Per-plan options (PlanNormalizer's `options`) in the `configurations`
     * document shape: {plans:{slug:{slug,family,options:{dimension:[choice]}}}}.
     * A choice carries option_label, category (= its dimension), monthly_price,
     * setup_price and, for object storage, size_gb.
     *
     * @param list<array<string,mixed>> $plans
     * @return array{plans:array<string,array<string,mixed>>}
     */
    public static function configurationsFromPlans(array $plans): array
    {
        $out = [];
        foreach ($plans as $plan) {
            if (!is_array($plan) || !isset($plan['options']) || !is_array($plan['options']) || !isset($plan['product_slug'])) {
                continue;
            }
            $dims = [];
            foreach ($plan['options'] as $dimension => $choices) {
                if ($choices === null) {
                    continue;
                }
                $list = is_array($choices) && self::isList($choices) ? $choices : [$choices];
                foreach ($list as $c) {
                    if (!is_array($c) || !isset($c['name'])) {
                        continue;
                    }
                    $choice = $c;
                    unset($choice['name']);
                    $dims[(string) $dimension][] = ['option_label' => (string) $c['name'], 'category' => (string) $dimension] + $choice;
                }
            }
            if ($dims !== []) {
                $out[(string) $plan['product_slug']] = [
                    'slug' => (string) $plan['product_slug'],
                    'family' => (string) ($plan['family'] ?? ''),
                    'options' => $dims,
                ];
            }
        }
        return ['plans' => $out];
    }

    /**
     * @param array<string,mixed> $configurations
     * @param list<array<string,mixed>> $items
     */
    private function appendConfigurationItems(array $configurations, string $observedAt, array &$items): void
    {
        $plans = $configurations['plans'] ?? null;
        if (!self::isObject($plans)) {
            return;
        }
        $seenIds = [];
        foreach ($items as $it) {
            $seenIds[(string) $it['machine_id']] = true;
        }
        foreach ($plans as $plan) {
            if (!is_array($plan)) {
                continue;
            }
            $planSlug = self::pickString($plan, ['slug']) ?? 'unknown-plan';
            $dimensions = $plan['options'] ?? null;
            if (!self::isObject($dimensions)) {
                continue;
            }
            foreach ($dimensions as $dimension => $choices) {
                if (!is_array($choices) || !self::isList($choices)) {
                    continue;
                }
                foreach ($choices as $choice) {
                    $choice = is_array($choice) ? $choice : [];
                    $label = self::pickString($choice, ['option_label']) ?? 'Unknown option';
                    $category = self::pickString($choice, ['category']) ?? 'default';
                    $machineId = 'option:' . self::machinePart($planSlug)
                        . ':' . self::machinePart((string) $dimension)
                        . ':' . self::machinePart($category)
                        . ':' . self::machinePart($label);
                    if (strlen($machineId) > 191) {
                        // the importer bounds machine ids at 191 chars: keep them unique, never truncate blindly
                        $machineId = substr($machineId, 0, 170) . '-' . substr(hash('sha256', $machineId), 0, 20);
                    }
                    if (isset($seenIds[$machineId])) {
                        continue; // two labels that differ only by punctuation collapse to one id
                    }
                    $seenIds[$machineId] = true;
                    $items[] = self::catalogItem(
                        $machineId,
                        self::observedProviderId($choice),
                        'configuration_option',
                        $label,
                        $observedAt,
                        $choice,
                        [
                            'plan_machine_id' => 'plan:' . $planSlug,
                            'dimension' => (string) $dimension,
                            'category' => $category,
                        ]
                    );
                }
            }
        }
    }

    /**
     * @param array<string,mixed> $payload
     * @param array<string,mixed> $compatibility
     * @return array<string,mixed>
     */
    private static function catalogItem(
        string $machineId,
        ?string $providerId,
        string $itemType,
        string $label,
        string $observedAt,
        array $payload,
        array $compatibility
    ): array {
        return [
            'machine_id' => $machineId,
            'provider_id' => $providerId,
            'item_type' => $itemType,
            'label' => $label,
            'catalog_version' => null,
            'effective_at' => $observedAt,
            'availability_state' => 'observed',
            'deprecated' => false,
            'compatibility' => $compatibility,
            'source_observed_at' => $observedAt,
            'payload_hash' => hash('sha256', CatalogImportService::canonicalJson($payload)),
            'payload' => $payload,
        ];
    }

    /** @param array<string,mixed> $value */
    private static function observedProviderId(array $value): ?string
    {
        foreach (['provider_id', 'provider_sku_id', 'customer_api_id'] as $key) {
            if (isset($value[$key]) && is_string($value[$key])) {
                return $value[$key];
            }
        }
        return null;
    }

    /**
     * Mirrors `.get(a).or_else(|| .get(b)).and_then(as_str)`: the first key that
     * EXISTS wins (even when null), and a non-string value yields null.
     *
     * @param array<string,mixed> $data
     * @param list<string> $keys
     */
    private static function pickString(array $data, array $keys): ?string
    {
        foreach ($keys as $key) {
            if (array_key_exists($key, $data)) {
                return is_string($data[$key]) ? $data[$key] : null;
            }
        }
        return null;
    }

    /** Rust machine_part(): lower-case ASCII alphanumerics joined by single dashes. */
    public static function machinePart(string $value): string
    {
        $lower = function_exists('mb_strtolower') ? mb_strtolower($value, 'UTF-8') : strtolower($value);
        return trim((string) preg_replace('/[^a-z0-9]+/', '-', $lower), '-');
    }

    /** @param mixed $v JSON "object" per serde as_object(): an associative array, or the (ambiguous) empty array */
    private static function isObject($v): bool
    {
        return is_array($v) && ($v === [] || !self::isList($v));
    }

    /** @param array<mixed> $v */
    private static function isList(array $v): bool
    {
        $i = 0;
        foreach ($v as $k => $_) {
            if ($k !== $i++) {
                return false;
            }
        }
        return true;
    }
}
