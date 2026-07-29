<?php
declare(strict_types=1);

namespace SecuriAceVps;

use WHMCS\Database\Capsule;

/**
 * Recovers what the CUSTOMER actually selected on a configurable product and
 * round-trips each selection back to its Contabo meaning via the addon's link
 * tables (PHASE_A6 §17):
 *
 *   tblhostingconfigoptions.optionid (WHMCS sub-option id)
 *     → mod_contabo_config_option_value_link.whmcs_sub_id
 *       → {contabo_value_key, contabo_label} + its option link's
 *         {dimension_key, pass_to_provisioning}
 *
 * Every read is hasTable-guarded: when the addon (or its link tables) is not
 * installed this resolver returns [] and provisioning falls back to the
 * product-level config options — the module never hard-depends on the addon.
 *
 * Join-free per-row queries (mirrors ServiceRevenueResolver::fetchConfigOptions)
 * so the logic is exercisable against FakeCapsule.
 */
final class ConfigOptionResolver
{
    private const T_VALUE  = 'mod_contabo_config_option_value_link';
    private const T_OPTION = 'mod_contabo_config_option_link';

    /**
     * @return list<array{dimension_key:string, value_key:string, label:string, qty:int}>
     *         Only selections whose option link has pass_to_provisioning=1.
     */
    public function selectionsForService(int $serviceId): array
    {
        if ($serviceId <= 0 || !$this->linkTablesPresent()) {
            return [];
        }

        $selected = Capsule::table('tblhostingconfigoptions')
            ->where('relid', $serviceId)
            ->get();

        $out = [];
        foreach ($selected as $sel) {
            $sel = (array) $sel;
            $subId = (int) ($sel['optionid'] ?? 0);
            if ($subId <= 0) {
                continue;
            }

            $valueLink = Capsule::table(self::T_VALUE)->where('whmcs_sub_id', $subId)->first();
            $valueLink = $valueLink !== null ? (array) $valueLink : null;
            if ($valueLink === null) {
                // Selection made outside the addon's curation — nothing to map.
                continue;
            }

            $optionLink = Capsule::table(self::T_OPTION)
                ->where('id', (int) ($valueLink['option_link_id'] ?? 0))
                ->first();
            $optionLink = $optionLink !== null ? (array) $optionLink : null;
            if ($optionLink === null) {
                continue;
            }
            if ((int) ($optionLink['pass_to_provisioning'] ?? 0) !== 1) {
                // Admin explicitly flagged this dimension as not provisioned.
                continue;
            }

            $out[] = [
                'dimension_key' => (string) ($optionLink['dimension_key'] ?? ''),
                'value_key'     => (string) ($valueLink['contabo_value_key'] ?? ''),
                'label'         => (string) ($valueLink['contabo_label'] ?? ''),
                'qty'           => max(1, (int) ($sel['qty'] ?? 1)),
            ];
        }
        return $out;
    }

    private function linkTablesPresent(): bool
    {
        try {
            $schema = Capsule::schema();
            return $schema->hasTable(self::T_VALUE) && $schema->hasTable(self::T_OPTION);
        } catch (\Throwable $e) {
            return false;
        }
    }
}
