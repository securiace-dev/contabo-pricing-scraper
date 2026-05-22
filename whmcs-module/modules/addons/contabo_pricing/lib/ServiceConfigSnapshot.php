<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * A.6.4 — selected-service config snapshot (design §12).
 *
 * Captures a point-in-time record of a service's configuration + the prices it
 * was sold at, into mod_contabo_service_config_snapshot. This is the STABLE
 * basis for renewal margin (amendment 5 / §13): renewal must price against what
 * the customer actually agreed to, not the live, mutable tblhostingconfigoptions
 * × tblpricing rows (which drift when the catalog is repriced).
 *
 * The figures come from {@see ServiceRevenueResolver} (base = product catalog
 * price, config_options = Σ selected option recurrings — never the stale
 * tblhosting.recurringamount). selected_image / selected_region are recovered by
 * round-tripping each selected WHMCS sub-option id back to its Contabo value via
 * {@see ConfigOptionLinkRepository::findValueLinkByWhmcsSubId} (§17).
 *
 * Read-only against WHMCS billing tables; the only write is the snapshot row
 * (overridable {@see storeRow} for DB-free tests). PHP 7.4 polyglot.
 */
class ServiceConfigSnapshot
{
    private const TABLE = 'mod_contabo_service_config_snapshot';
    private const PROVISIONING_METADATA_VERSION = 1;

    /** @var ServiceRevenueResolver */
    private $resolver;

    /** @var ConfigOptionLinkRepository */
    private $links;

    /** @var array<string,mixed> */
    private $settings;

    /**
     * @param array<string,mixed> $settings
     */
    public function __construct(array $settings = [], ?ServiceRevenueResolver $resolver = null, ?ConfigOptionLinkRepository $links = null)
    {
        $this->settings = $settings;
        $this->resolver = $resolver !== null ? $resolver : new ServiceRevenueResolver();
        $this->links    = $links !== null ? $links : new ConfigOptionLinkRepository();
    }

    /**
     * Capture a snapshot for a live service. Resolves the service → product →
     * mapping → profile → latest version, sums the agreed-upon prices, recovers
     * the selected image/region, and persists one row. Returns the snapshot id
     * (0 when the service can't be resolved — never throws into a hook).
     */
    public function capture(int $serviceId): int
    {
        $service = $this->fetchService($serviceId);
        if ($service === []) {
            return 0;
        }
        $productId = (int) ($service['packageid'] ?? 0);

        $mapping = $this->fetchMapping($productId);
        $profileId = $mapping !== null ? (int) ($mapping['profile_id'] ?? 0) : 0;
        $profile = $profileId > 0 ? $this->fetchProfile($profileId) : null;
        $version = $profileId > 0 ? $this->fetchLatestVersion($profileId) : null;

        $revenue = $this->resolver->resolveForService($serviceId);
        $selected = $this->recoverSelections($revenue);
        // Enriched selections carry each option's EUR delta (the cost basis) so
        // the snapshot is self-contained for Phase B's landedCostWithSelections.

        $row = [
            'service_id'                    => $serviceId,
            'profile_id'                    => $profileId > 0 ? $profileId : null,
            'profile_mode'                  => $profile !== null ? (string) ($profile['profile_mode'] ?? 'unknown') : 'unmapped',
            'plan_slug'                     => $profile !== null ? (string) ($profile['plan_slug'] ?? '') : '',
            'whmcs_product_id'              => $productId,
            'selected_image'                => $selected['image'],
            'selected_region'               => $selected['region'],
            'selected_options_json'         => $this->encode($selected['selections']),
            'contabo_payload_json'          => null, // built by the provisioning module (Phase C)
            'base_price_snapshot'           => (float) ($revenue['base'] ?? 0.0),
            'config_option_price_snapshot'  => (float) ($revenue['config_options'] ?? 0.0),
            'landed_cost_snapshot'          => null, // landedCostWithSelections is Phase B (§13)
            'tax_mode_snapshot'             => (string) ($this->settings['tax_registration_mode'] ?? ''),
            'pricing_version_snapshot'      => $version !== null ? (string) ($version['version'] ?? '') : null,
            'provisioning_metadata_version' => self::PROVISIONING_METADATA_VERSION,
            'created_at'                    => date('Y-m-d H:i:s'),
            'updated_at'                    => date('Y-m-d H:i:s'),
        ];

        return $this->storeRow($row);
    }

    /**
     * The most recent snapshot for a service (renewal reads this).
     *
     * @return array<string,mixed>|null
     */
    public function latestForService(int $serviceId): ?array
    {
        $r = Capsule::table(self::TABLE)
            ->where('service_id', $serviceId)
            ->orderByDesc('id')
            ->first();
        return $r !== null ? (array) $r : null;
    }

    /**
     * Recover the selected image + region labels AND build the enriched
     * selections list: for each selected sub-option, round-trip to its Contabo
     * value link to pick up the dimension, label and — critically — the
     * `monthly_eur_delta` (the cost basis Phase B needs). Lines whose value link
     * is missing (e.g. selections made outside the addon) keep their sell-side
     * fields with eur_monthly = 0, so the list is always complete.
     *
     * @param array{breakdown:array<string,mixed>} $revenue
     * @return array{image:?string, region:?string, selections:list<array<string,mixed>>}
     */
    private function recoverSelections(array $revenue): array
    {
        $image = null;
        $region = null;
        $selections = [];
        $cfg = $revenue['breakdown']['config_options'] ?? [];
        if (!is_array($cfg)) {
            return ['image' => null, 'region' => null, 'selections' => []];
        }
        foreach ($cfg as $line) {
            $subId = (int) ($line['sub_id'] ?? 0);
            $qty   = (int) ($line['qty'] ?? 1);
            $valueLink = $subId > 0 ? $this->links->findValueLinkByWhmcsSubId($subId) : null;

            $dim = '';
            $label = '';
            $eurMonthly = 0.0;
            if ($valueLink !== null) {
                $eurMonthly = (float) ($valueLink['monthly_eur_delta'] ?? 0.0);
                $label = (string) ($valueLink['contabo_label'] ?? '');
                $optionLink = $this->fetchOptionLinkById((int) ($valueLink['option_link_id'] ?? 0));
                $dim = $optionLink !== null ? (string) ($optionLink['dimension_key'] ?? '') : '';
                if ($dim === 'Image' && $image === null) {
                    $image = $label;
                } elseif ($dim === 'Region' && $region === null) {
                    $region = $label;
                }
            }

            $selections[] = [
                'sub_id'      => $subId,
                'qty'         => $qty,
                'dimension'   => $dim,
                'label'       => $label,
                'unit_sell'   => (float) ($line['unit'] ?? 0.0),
                'line_sell'   => (float) ($line['line'] ?? 0.0),
                'eur_monthly' => $eurMonthly, // cost basis for landedCostWithSelections
            ];
        }
        return ['image' => $image, 'region' => $region, 'selections' => $selections];
    }

    // ── data access (overridable for tests) ───────────────────────────────────

    /** @return array<string,mixed> */
    protected function fetchService(int $serviceId): array
    {
        $r = Capsule::table('tblhosting')->where('id', $serviceId)->first();
        return $r !== null ? (array) $r : [];
    }

    /** @return array<string,mixed>|null */
    protected function fetchMapping(int $productId): ?array
    {
        $r = Capsule::table('mod_contabo_mapping')
            ->where('product_id', $productId)
            ->where('active', 1)
            ->first();
        return $r !== null ? (array) $r : null;
    }

    /** @return array<string,mixed>|null */
    protected function fetchProfile(int $profileId): ?array
    {
        $r = Capsule::table('mod_contabo_profile')->where('id', $profileId)->first();
        return $r !== null ? (array) $r : null;
    }

    /** @return array<string,mixed>|null */
    protected function fetchLatestVersion(int $profileId): ?array
    {
        $r = Capsule::table('mod_contabo_profile_version')
            ->where('profile_id', $profileId)
            ->orderByDesc('version')
            ->first();
        return $r !== null ? (array) $r : null;
    }

    /** @return array<string,mixed>|null */
    protected function fetchOptionLinkById(int $optionLinkId): ?array
    {
        if ($optionLinkId <= 0) {
            return null;
        }
        $r = Capsule::table('mod_contabo_config_option_link')->where('id', $optionLinkId)->first();
        return $r !== null ? (array) $r : null;
    }

    /** @param array<string,mixed> $row */
    protected function storeRow(array $row): int
    {
        return (int) Capsule::table(self::TABLE)->insertGetId($row);
    }

    /** @param mixed $value */
    private function encode($value): string
    {
        $json = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        return $json === false ? '[]' : $json;
    }
}
