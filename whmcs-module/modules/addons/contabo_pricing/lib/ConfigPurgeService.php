<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * A.6.5 — config-object-aware purge (design §19).
 *
 * The plain mod_contabo_* purge truncates the addon's own tables but LEAVES the
 * WHMCS configurable options the addon created (groups / options / sub-options /
 * pricing / product links) orphaned on the products. This service removes ONLY
 * those addon-created WHMCS objects — identified strictly by the ids recorded in
 * the link tables (mod_contabo_config_*_link). It NEVER touches a WHMCS object
 * the addon didn't create, and NEVER touches clients, invoices, services or any
 * non-config table.
 *
 * Read the link tables → delete the exact recorded WHMCS ids. Idempotent (a 2nd
 * run after the links are gone is a no-op). Returns per-table delete counts.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ConfigPurgeService
{
    /**
     * Delete the WHMCS config objects the addon created, scoped strictly to the
     * ids in the link tables. Order: pricing + sub-options (by value-link
     * whmcs_sub_id), then options (by option-link whmcs_option_id), then
     * product-links + groups (by group-link whmcs_group_id).
     *
     * @return array{subs:int, sub_pricing:int, options:int, product_links:int, groups:int}
     */
    public function removeAddonCreatedWhmcsObjects(): array
    {
        $counts = ['subs' => 0, 'sub_pricing' => 0, 'options' => 0, 'product_links' => 0, 'groups' => 0];

        // 1. Sub-options + their configoptions pricing (relid = sub id).
        foreach ($this->ids('mod_contabo_config_option_value_link', 'whmcs_sub_id') as $subId) {
            $counts['sub_pricing'] += Capsule::table('tblpricing')
                ->where('type', 'configoptions')->where('relid', $subId)->delete();
            $counts['subs'] += Capsule::table('tblproductconfigoptionssub')->where('id', $subId)->delete();
        }

        // 2. Options.
        foreach ($this->ids('mod_contabo_config_option_link', 'whmcs_option_id') as $optionId) {
            $counts['options'] += Capsule::table('tblproductconfigoptions')->where('id', $optionId)->delete();
        }

        // 3. Group → product link + the group itself.
        foreach ($this->ids('mod_contabo_config_group_link', 'whmcs_group_id') as $groupId) {
            $counts['product_links'] += Capsule::table('tblproductconfiglinks')->where('gid', $groupId)->delete();
            $counts['groups'] += Capsule::table('tblproductconfiggroups')->where('id', $groupId)->delete();
        }

        return $counts;
    }

    /**
     * Distinct, positive WHMCS ids recorded in a link table column. Each row is
     * normalised (real WHMCS returns stdClass; FakeCapsule returns arrays).
     *
     * @return list<int>
     */
    private function ids(string $table, string $column): array
    {
        $out = [];
        // No whereNotNull(): a NULL id casts to 0 below and is filtered out, so
        // the positive-id guard already covers it (and keeps FakeCapsule happy).
        $rows = Capsule::table($table)->get([$column]);
        foreach ($rows as $r) {
            $r = (array) $r;
            $id = (int) ($r[$column] ?? 0);
            if ($id > 0) {
                $out[$id] = $id; // dedupe
            }
        }
        return array_values($out);
    }
}
