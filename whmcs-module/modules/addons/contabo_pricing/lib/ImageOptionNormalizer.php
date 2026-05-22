<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Collapses Contabo's flat "Image" dimension rows into ONE WHMCS configurable
 * option.
 *
 * ## Why one option, never four (verified design rule)
 *
 * Contabo's configurator exposes the Image dimension as ~34 rows spread across
 * four categories — OS (17), Panels (6), Apps (8), Blockchain (3) on
 * cloud-vps-10 — but they are **mutually exclusive**: a server boots from
 * exactly ONE image. cPanel/Plesk are Panels-category *images* (a panel image
 * IS the image), not add-ons layered on top of an OS. Therefore the category
 * is a *visual grouping only*, and the correct WHMCS shape is a single
 * dropdown option whose sub-values are all 34 images — never four separate
 * options (which would let a customer pick "Ubuntu" AND "cPanel" AND "Docker"
 * simultaneously, an impossible build). See PHASE_A52_DESIGN_IMPACT.md §2.
 *
 * ## Label grouping without optgroups (amendment 2)
 *
 * The WHMCS order-form template is not assumed to render HTML <optgroup>s, so
 * grouping is encoded two ways that survive a flat <select>:
 *   - a label PREFIX: `[OS] Ubuntu 24.04`, `[Panel] cPanel`, `[App] Docker`,
 *     `[Blockchain] Geth`;
 *   - an explicit integer `sortorder` that clusters values by category in a
 *     sensible retail order (OS first, then Panels, Apps, Blockchain) and then
 *     alphabetically by label within each category.
 *
 * ## Provisioning round-trip
 *
 * {@see provisioningValue} returns the single `category:label` identifier
 * (the `contabo_label`) that the provisioning module passes to Contabo. It
 * NEVER emits four separate values — there is exactly one image per server.
 *
 * Pure class: no DB, no WHMCS calls, fully deterministic.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ImageOptionNormalizer
{
    /** The normalised dimension key for the single Image option. */
    public const DIMENSION_KEY = 'Image';

    /**
     * Retail display order for Image categories. Lower index sorts first.
     * OS is the everyday default, Panels are the next most common managed
     * choice, then Apps, then niche Blockchain images.
     *
     * @var array<string,int>
     */
    private const CATEGORY_ORDER = [
        'OS'         => 0,
        'Panels'     => 1,
        'Apps'       => 2,
        'Blockchain' => 3,
    ];

    /**
     * Category → human label prefix. Singular and retail-friendly, e.g.
     * `[Panel] cPanel` rather than `[Panels] cPanel`.
     *
     * @var array<string,string>
     */
    private const CATEGORY_PREFIX = [
        'OS'         => 'OS',
        'Panels'     => 'Panel',
        'Apps'       => 'App',
        'Blockchain' => 'Blockchain',
    ];

    /** Sort buckets are spaced this far apart so labels never collide. */
    private const CATEGORY_BUCKET = 1000;

    /**
     * Collapse the flat Image rows into ONE option spec.
     *
     * @param array<int,array<string,mixed>> $imageRows the Contabo `options['Image']` array
     * @return array{
     *     dimension_key: string,
     *     optiontype: int,
     *     category_groups: list<string>,
     *     values: list<array{
     *         value_key: string,
     *         label: string,
     *         category: string,
     *         sortorder: int,
     *         monthly_eur_delta: float,
     *         setup_eur_delta: float,
     *         is_default: bool
     *     }>,
     *     default_value_key: ?string
     * }
     */
    public static function normalize(array $imageRows): array
    {
        $values = [];
        foreach ($imageRows as $row) {
            $category = (string) ($row['category'] ?? '');
            $label    = (string) ($row['option_label'] ?? '');
            if ($label === '') {
                // A row with no label cannot be a selectable image; skip it
                // rather than emit a blank, unselectable sub-option.
                continue;
            }

            $values[] = [
                'value_key'         => self::valueKey($category, $label),
                'label'             => self::prefixedLabel($category, $label),
                'category'          => $category,
                // sortorder is filled after the full set is sorted, so it is a
                // dense, stable, category-clustered sequence.
                'sortorder'         => 0,
                'monthly_eur_delta' => self::toFloat($row['monthly_price_delta'] ?? 0),
                'setup_eur_delta'   => self::toFloat($row['setup_fee_delta'] ?? 0),
                'is_default'        => !empty($row['is_default']),
            ];
        }

        // Order by category bucket, then by (unprefixed) label within the
        // bucket. usort is not stable pre-8.0 in theory but the comparator is
        // total (category index then label then value_key), so the result is
        // deterministic on every PHP version.
        usort($values, static function (array $a, array $b): int {
            $ca = self::categoryRank($a['category']);
            $cb = self::categoryRank($b['category']);
            if ($ca !== $cb) {
                return $ca <=> $cb;
            }
            $cmp = strcasecmp($a['label'], $b['label']);
            if ($cmp !== 0) {
                return $cmp;
            }
            return strcmp($a['value_key'], $b['value_key']);
        });

        // Assign a dense sortorder that preserves category clustering: each
        // category occupies its own thousand-band so later insertions inside a
        // band don't reshuffle other categories.
        $perCategoryIndex = [];
        $default = null;
        foreach ($values as $i => &$value) {
            $cat = $value['category'];
            $idx = $perCategoryIndex[$cat] ?? 0;
            $perCategoryIndex[$cat] = $idx + 1;
            $value['sortorder'] = self::categoryRank($cat) * self::CATEGORY_BUCKET + $idx;

            if ($default === null && $value['is_default']) {
                $default = $value['value_key'];
            }
        }
        unset($value);

        return [
            'dimension_key'     => self::DIMENSION_KEY,
            // 34 mutually-exclusive values → always a dropdown (optiontype 1).
            'optiontype'        => OptionTypeMapper::mapFor(self::DIMENSION_KEY),
            'category_groups'   => self::orderedCategoryGroups($values),
            'values'            => $values,
            'default_value_key' => $default,
        ];
    }

    /**
     * The single `category:label` image identifier to hand to provisioning.
     *
     * This is the inverse of {@see valueKey}: callers pass the `value_key`
     * chosen in WHMCS and get back the `contabo_label` round-trip string. The
     * value_key already encodes `category:label`, so this returns it verbatim
     * (after trimming) — proving there is exactly ONE image per selection.
     *
     * @param string $selectedValueKey a `value_key` produced by normalize()
     * @return string the `category:label` identifier (one image, never four)
     */
    public static function provisioningValue(string $selectedValueKey): string
    {
        return trim($selectedValueKey);
    }

    /**
     * Build the stable round-trip key for an image row: `category:label`.
     * Used both as the WHMCS sub-option key and the provisioning identifier.
     */
    public static function valueKey(string $category, string $label): string
    {
        return $category . ':' . $label;
    }

    /**
     * Apply the amendment-2 label prefix, e.g. `[Panel] cPanel/WHM`.
     * Unknown categories pass through with no prefix rather than a misleading
     * one.
     */
    public static function prefixedLabel(string $category, string $label): string
    {
        $prefix = self::CATEGORY_PREFIX[$category] ?? null;
        if ($prefix === null) {
            return $label;
        }
        return '[' . $prefix . '] ' . $label;
    }

    /** Retail sort rank for a category; unknown categories sort last. */
    private static function categoryRank(string $category): int
    {
        return self::CATEGORY_ORDER[$category] ?? count(self::CATEGORY_ORDER);
    }

    /**
     * The distinct categories actually present, in retail order.
     *
     * @param list<array{category:string}> $values
     * @return list<string>
     */
    private static function orderedCategoryGroups(array $values): array
    {
        $seen = [];
        foreach ($values as $value) {
            $seen[$value['category']] = true;
        }
        $groups = array_keys($seen);
        usort($groups, static function (string $a, string $b): int {
            return self::categoryRank($a) <=> self::categoryRank($b);
        });
        return $groups;
    }

    /** Coerce a JSON int|float|string price delta to a float. */
    private static function toFloat($value): float
    {
        return (float) $value;
    }
}
