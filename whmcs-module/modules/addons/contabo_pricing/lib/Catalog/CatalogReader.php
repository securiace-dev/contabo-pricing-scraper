<?php
declare(strict_types=1);

namespace ContaboPricing\Catalog;

use ContaboPricing\AdminController;
use WHMCS\Database\Capsule;

/**
 * Read side of the addon-native catalog: serves the shapes the Rust API used
 * to serve (meta / plans / plan / configurator) from the imported, versioned
 * catalog tables. Pure reads; never writes.
 *
 * PHP 7.4 polyglot: no promotion, no match, no readonly.
 */
final class CatalogReader
{
    private const VERSION_TABLE = 'mod_contabo_catalog_versions';
    private const ITEM_TABLE = 'mod_contabo_catalog_items';
    public const API_SCHEMA_VERSION = '1.1';

    /** @var array<string,mixed>|null|false */
    private $latestCache = false;

    /** @return array<string,mixed>|null newest observed|published version row */
    public function latestVersion(): ?array
    {
        if ($this->latestCache !== false) {
            return $this->latestCache;
        }
        $rows = Capsule::table(self::VERSION_TABLE)
            ->whereIn('state', ['observed', 'published'])
            ->orderByDesc('id')
            ->limit(1)
            ->get();
        $row = null;
        foreach ($rows as $r) {
            $row = (array) $r;
            break;
        }
        $this->latestCache = $row;
        return $row;
    }

    /**
     * @return list<array<string,mixed>> plan rows (shape of API v1.1 /plans)
     */
    public function plans(?string $family = null): array
    {
        $version = $this->latestVersion();
        if ($version === null) {
            return [];
        }
        $rows = Capsule::table(self::ITEM_TABLE)
            ->where('catalog_version_id', (int) $version['id'])
            ->where('item_type', 'plan')
            ->get();
        $out = [];
        foreach ($rows as $r) {
            $row = (array) $r;
            $payload = json_decode((string) ($row['payload_json'] ?? ''), true);
            if (!is_array($payload)) {
                continue;
            }
            if ($family !== null && $family !== ''
                && strcasecmp((string) ($payload['family'] ?? ''), $family) !== 0) {
                continue;
            }
            $out[] = $payload;
        }
        return $out;
    }

    /** @return array<string,mixed>|null */
    public function plan(string $slug): ?array
    {
        foreach ($this->plans() as $plan) {
            $s = (string) ($plan['product_slug'] ?? ($plan['slug'] ?? ''));
            if ($s === $slug) {
                return $plan;
            }
        }
        return null;
    }

    /**
     * @return array<string,mixed> configurator doc ({slug, options:{dim:[choice]}}); empty options when absent
     */
    public function configurator(string $slug): array
    {
        $empty = ['slug' => $slug, 'options' => []];
        $version = $this->latestVersion();
        if ($version === null) {
            return $empty;
        }
        $envelope = json_decode((string) ($version['envelope_json'] ?? ''), true);
        if (!is_array($envelope)) {
            return $empty;
        }
        $plans = $envelope['configurations']['plans'] ?? null;
        if (!is_array($plans)) {
            return $empty;
        }
        foreach ($plans as $entry) {
            if (is_array($entry) && (string) ($entry['slug'] ?? '') === $slug) {
                if (!isset($entry['options']) || !is_array($entry['options'])) {
                    $entry['options'] = [];
                }
                return $entry;
            }
        }
        return $empty;
    }

    /** @return array<string,mixed> */
    public function meta(): array
    {
        $version = $this->latestVersion();
        $scraper = 'addon-' . AdminController::VERSION;
        return [
            'scraper_version' => $scraper,
            'schema_version' => self::API_SCHEMA_VERSION,
            'snapshot_meta' => [
                'generated_at' => $version === null ? '' : self::isoUtc((string) ($version['source_observed_at'] ?? '')),
                'plan_count' => $version === null ? 0 : count($this->plans()),
                'scraper_version' => $scraper,
            ],
        ];
    }

    /** Stored MySQL timestamps are the UTC wall clock of the observed ISO string. */
    private static function isoUtc(string $mysql): string
    {
        $ts = $mysql === '' ? false : strtotime($mysql . ' UTC');
        return $ts === false ? $mysql : gmdate('Y-m-d\TH:i:s\Z', $ts);
    }
}
