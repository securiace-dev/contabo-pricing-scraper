<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * Phase A.6.1 — ExposureResolver.
 *
 * Bridges the DimensionParser dimension keys to RetailVpsMinimalPreset's
 * exposure decisions, so the apply path AND the exposure editor get one clean
 * API for "should this option/value be exposed to customers / hidden in WHMCS?".
 *
 * The two key vocabularies differ:
 *   - DimensionParser keys (used everywhere else in the addon):
 *       Image, Region, Storage Type, Data Protection,
 *       Networking:Bandwidth, Networking:IPv4, Networking:Private Networking
 *   - RetailVpsMinimalPreset keys (its internal data shape, snake_case):
 *       image, region, storage_type, data_protection,
 *       bandwidth, ipv4, private_networking
 *
 * This class owns the one true map between them and answers exposure questions.
 * It is conservative by design: anything we do not recognise is treated as
 * hidden / not-exposed, so a typo or a new dimension can never accidentally leak
 * an option to customers.
 *
 * PHP 7.4 polyglot; all-static (RetailVpsMinimalPreset is static); no DB, no I/O.
 */
final class ExposureResolver
{
    /**
     * Conservative default for anything we cannot map: never expose, always hide.
     */
    private const CONSERVATIVE = [
        'expose_to_customer' => false,
        'hidden'             => true,
    ];

    /**
     * DimensionParser dimension key → RetailVpsMinimalPreset dimension key.
     *
     * @return array<string, string>
     */
    private static function dimensionKeyMap(): array
    {
        // The Networking sub-concern keys are spelled verbatim: DimensionParser
        // keeps its NETWORKING_SPLIT map private, so we mirror its exact key
        // strings here ('Networking:Bandwidth' etc.). The four single-dimension
        // keys come from DimensionParser's public constants.
        return [
            DimensionParser::DIM_IMAGE             => RetailVpsMinimalPreset::DIM_IMAGE,              // Image → image
            DimensionParser::DIM_REGION            => RetailVpsMinimalPreset::DIM_REGION,             // Region → region
            DimensionParser::DIM_STORAGE_TYPE      => RetailVpsMinimalPreset::DIM_STORAGE,            // Storage Type → storage_type
            DimensionParser::DIM_DATA_PROTECTION   => RetailVpsMinimalPreset::DIM_BACKUP,             // Data Protection → data_protection
            'Networking:IPv4'                      => RetailVpsMinimalPreset::DIM_IPV4,               // Networking:IPv4 → ipv4
            'Networking:Bandwidth'                 => RetailVpsMinimalPreset::DIM_BANDWIDTH,          // Networking:Bandwidth → bandwidth
            'Networking:Private Networking'        => RetailVpsMinimalPreset::DIM_PRIVATE_NETWORKING, // Networking:Private Networking → private_networking
        ];
    }

    /**
     * Decide exposure for a DimensionParser dimension key.
     *
     * Maps the DimensionParser key to its RetailVpsMinimalPreset key, then reads
     * the preset's exposure flags. An unknown dimension key — or one the preset
     * unexpectedly does not carry — yields the conservative default
     * (not exposed, hidden), so we never expose something we do not recognise.
     *
     * @param string $dimensionKey a DimensionParser dimension key, e.g. 'Image',
     *        'Region', 'Networking:IPv4'.
     * @param string $planSlug optional plan context, forwarded to the preset.
     * @return array{expose_to_customer: bool, hidden: bool}
     */
    public static function decideForDimension(string $dimensionKey, string $planSlug = ''): array
    {
        $map = self::dimensionKeyMap();
        if (!isset($map[$dimensionKey])) {
            return self::CONSERVATIVE;
        }

        $presetKey = $map[$dimensionKey];
        $exposure  = RetailVpsMinimalPreset::exposureFor($planSlug);
        if (!isset($exposure[$presetKey])) {
            return self::CONSERVATIVE;
        }

        $entry = $exposure[$presetKey];

        return [
            'expose_to_customer' => (bool) $entry['expose_to_customer'],
            'hidden'             => (bool) $entry['hidden'],
        ];
    }

    /**
     * Decide exposure for a single Contabo image category (OS / Panels / Apps /
     * Blockchain) within the one Image dropdown.
     *
     * Reads RetailVpsMinimalPreset::imageCategoryVisibility(). An unknown
     * category yields the conservative default (not exposed, hidden).
     *
     * @param string $category one of OS / Panels / Apps / Blockchain.
     * @param string $planSlug optional plan context, forwarded to the preset.
     * @return array{expose_to_customer: bool, hidden: bool}
     */
    public static function decideForImageCategory(string $category, string $planSlug = ''): array
    {
        $visibility = RetailVpsMinimalPreset::imageCategoryVisibility($planSlug);
        if (!isset($visibility[$category])) {
            return self::CONSERVATIVE;
        }

        $entry = $visibility[$category];

        return [
            'expose_to_customer' => (bool) $entry['expose_to_customer'],
            'hidden'             => (bool) $entry['hidden'],
        ];
    }

    /**
     * The human-readable name of the backing preset.
     */
    public static function presetName(): string
    {
        return RetailVpsMinimalPreset::name();
    }
}
