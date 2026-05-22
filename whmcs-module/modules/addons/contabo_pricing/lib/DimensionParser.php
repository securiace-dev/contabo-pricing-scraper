<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Normalises Contabo's raw configurator `options` object into a list of
 * "WHMCS option specs" ready for the syncer to materialise as configurable
 * options.
 *
 * ## Input shape (verified, data/output/contabo_configs.json)
 *
 * `plans[<url>].options` is an object keyed by up to five Contabo dimensions —
 * `Image`, `Networking`, `Region`, `Storage Type`, `Data Protection` — each
 * value being a flat array of rows:
 *
 *   {plan_sku, currency, dimension, category, option_label,
 *    monthly_price_delta, setup_fee_delta[, is_default, region_group, country,
 *    country_code, subregion]}
 *
 * ## Split rules (PHASE_A52_DESIGN_IMPACT.md §2)
 *
 *   - **Image** → exactly ONE spec. Its four categories (OS/Apps/Panels/
 *     Blockchain) are visual groupings of a single mutually-exclusive choice;
 *     {@see ImageOptionNormalizer} collapses the rows and prefixes labels.
 *   - **Networking** → THREE specs, because Bandwidth, IPv4 and Private
 *     Networking are independent concerns, not alternatives:
 *       `Networking:Bandwidth`, `Networking:IPv4`,
 *       `Networking:Private Networking`. Each spec carries only its category's
 *       rows.
 *   - **Region / Storage Type / Data Protection** → ONE spec each (one
 *     mutually-exclusive choice).
 *
 * ## Single-value dimensions
 *
 * A dimension (or, for Networking, a split concern) that exposes ≤1 selectable
 * value carries no real choice, so it is OMITTED from `specs` and recorded in
 * `omitted[]` with a reason. Image is never omitted this way — it is always
 * emitted as one spec even if collapsed to a single value, because the syncer
 * needs the Image option to exist for provisioning round-trips.
 *
 * ## Currency guard (amendment 10)
 *
 * This parser is currency-agnostic — it passes `monthly_price_delta` /
 * `setup_fee_delta` through verbatim as EUR marginal costs. **v1 consumers
 * (the syncer) sync currency 1 (INR) ONLY**; USD/EUR/GBP are explicitly
 * not-synced. This class does not enforce that, but downstream code must.
 *
 * ## Negative-delta clamp (amendment 1)
 *
 * Pricing lives in the syncer, but the clamp policy is exposed here as a
 * static helper {@see clampDelta} so every consumer uses the same rule: v1
 * clamps option deltas to ≥ 0 (WHMCS negative configurable-option pricing is
 * untested). The parser itself does NOT clamp — it preserves the raw EUR
 * delta so the syncer can decide (clamp / rebase default / mark admin-only).
 *
 * Pure class: no DB, no WHMCS calls, fully deterministic.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class DimensionParser
{
    public const DIM_IMAGE           = 'Image';
    public const DIM_NETWORKING      = 'Networking';
    public const DIM_REGION          = 'Region';
    public const DIM_STORAGE_TYPE    = 'Storage Type';
    public const DIM_DATA_PROTECTION = 'Data Protection';

    /**
     * Networking sub-concern categories, in the order they should appear, each
     * mapped to its normalised dimension key.
     *
     * @var array<string,string>
     */
    private const NETWORKING_SPLIT = [
        'Bandwidth'          => 'Networking:Bandwidth',
        'IPv4'               => 'Networking:IPv4',
        'Private Networking' => 'Networking:Private Networking',
    ];

    /** Mutually-exclusive single-choice dimensions handled generically. */
    private const SINGLE_CHOICE_DIMENSIONS = [
        self::DIM_REGION,
        self::DIM_STORAGE_TYPE,
        self::DIM_DATA_PROTECTION,
    ];

    /**
     * Parse a Contabo `options` object into normalised WHMCS option specs.
     *
     * @param array<string,array<int,array<string,mixed>>> $optionsByDimension the Contabo `options` object
     * @return array{
     *     specs: list<array{
     *         dimension_key: string,
     *         optiontype: int,
     *         category_groups?: list<string>,
     *         values: list<array{
     *             value_key: string,
     *             label: string,
     *             category: string,
     *             monthly_eur_delta: float,
     *             setup_eur_delta: float,
     *             is_default: bool,
     *             sortorder?: int
     *         }>,
     *         default_value_key?: ?string
     *     }>,
     *     omitted: list<array{dimension_key: string, reason: string, value_count: int}>
     * }
     */
    public static function parse(array $optionsByDimension): array
    {
        $specs   = [];
        $omitted = [];

        foreach ($optionsByDimension as $dimension => $rows) {
            if (!is_array($rows)) {
                continue;
            }

            if ($dimension === self::DIM_IMAGE) {
                // Image always becomes exactly ONE spec via the normalizer.
                $specs[] = ImageOptionNormalizer::normalize($rows);
                continue;
            }

            if ($dimension === self::DIM_NETWORKING) {
                self::splitNetworking($rows, $specs, $omitted);
                continue;
            }

            if (in_array($dimension, self::SINGLE_CHOICE_DIMENSIONS, true)) {
                self::emitSingleChoice($dimension, $rows, $specs, $omitted);
                continue;
            }

            // Unknown dimension: treat as a generic single-choice control so
            // a future Contabo dimension still surfaces rather than silently
            // vanishing.
            self::emitSingleChoice($dimension, $rows, $specs, $omitted);
        }

        return [
            'specs'   => $specs,
            'omitted' => $omitted,
        ];
    }

    /**
     * v1 negative-delta clamp helper (amendment 1). The syncer applies this to
     * every option delta before writing tblpricing: WHMCS negative
     * configurable-option pricing is untested, so v1 floors deltas at zero.
     */
    public static function clampDelta(float $delta): float
    {
        return max(0.0, $delta);
    }

    /**
     * Emit the three independent Networking specs (Bandwidth / IPv4 / Private
     * Networking), each from only its category's rows. A concern with ≤1 value
     * is omitted.
     *
     * @param array<int,array<string,mixed>> $rows
     * @param list<array<string,mixed>>      $specs   (by-ref accumulator)
     * @param list<array<string,mixed>>      $omitted (by-ref accumulator)
     */
    private static function splitNetworking(array $rows, array &$specs, array &$omitted): void
    {
        // Bucket rows by category once.
        $byCategory = [];
        foreach (self::NETWORKING_SPLIT as $category => $_key) {
            $byCategory[$category] = [];
        }
        foreach ($rows as $row) {
            $category = (string) ($row['category'] ?? '');
            if (isset($byCategory[$category])) {
                $byCategory[$category][] = $row;
            }
            // Rows in an unexpected Networking category are dropped: the three
            // known concerns are exhaustive per verified data.
        }

        foreach (self::NETWORKING_SPLIT as $category => $dimensionKey) {
            self::emitSpec($dimensionKey, $byCategory[$category], $specs, $omitted);
        }
    }

    /**
     * Emit one spec for a mutually-exclusive single-choice dimension. The
     * dimension key is the dimension name verbatim (Region / Storage Type /
     * Data Protection).
     *
     * @param array<int,array<string,mixed>> $rows
     * @param list<array<string,mixed>>      $specs   (by-ref accumulator)
     * @param list<array<string,mixed>>      $omitted (by-ref accumulator)
     */
    private static function emitSingleChoice(string $dimensionKey, array $rows, array &$specs, array &$omitted): void
    {
        self::emitSpec($dimensionKey, $rows, $specs, $omitted);
    }

    /**
     * Build and append a generic option spec, or omit it when there is ≤1
     * selectable value. Shared by single-choice dimensions and Networking
     * concerns.
     *
     * @param array<int,array<string,mixed>> $rows
     * @param list<array<string,mixed>>      $specs   (by-ref accumulator)
     * @param list<array<string,mixed>>      $omitted (by-ref accumulator)
     */
    private static function emitSpec(string $dimensionKey, array $rows, array &$specs, array &$omitted): void
    {
        $values = self::buildValues($rows);

        if (count($values) <= 1) {
            $omitted[] = [
                'dimension_key' => $dimensionKey,
                'reason'        => 'single_value',
                'value_count'   => count($values),
            ];
            return;
        }

        $default = null;
        foreach ($values as $value) {
            if ($value['is_default']) {
                $default = $value['value_key'];
                break;
            }
        }

        $specs[] = [
            'dimension_key'     => $dimensionKey,
            'optiontype'        => OptionTypeMapper::mapForWithValueCount($dimensionKey, count($values)),
            'values'            => $values,
            'default_value_key' => $default,
        ];
    }

    /**
     * Normalise a flat array of Contabo rows into option-value structs. Rows
     * with an empty label are dropped (they cannot be a selectable value).
     *
     * @param array<int,array<string,mixed>> $rows
     * @return list<array{
     *     value_key: string,
     *     label: string,
     *     category: string,
     *     monthly_eur_delta: float,
     *     setup_eur_delta: float,
     *     is_default: bool
     * }>
     */
    private static function buildValues(array $rows): array
    {
        $values = [];
        foreach ($rows as $row) {
            $label = (string) ($row['option_label'] ?? '');
            if ($label === '') {
                continue;
            }
            $category = (string) ($row['category'] ?? '');
            $values[] = [
                'value_key'         => $category . ':' . $label,
                'label'             => $label,
                'category'          => $category,
                'monthly_eur_delta' => (float) ($row['monthly_price_delta'] ?? 0),
                'setup_eur_delta'   => (float) ($row['setup_fee_delta'] ?? 0),
                'is_default'        => !empty($row['is_default']),
            ];
        }
        return $values;
    }
}
