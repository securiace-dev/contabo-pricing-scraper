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
 * Destructive write path → has dry-run coverage: {@see previewRemoval} counts
 * exactly what {@see removeAddonCreatedWhmcsObjects} would delete, writing nothing.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ConfigPurgeService
{
    /**
     * Delete the WHMCS config objects the addon created, scoped strictly to the
     * ids in the link tables.
     *
     * @return array{subs:int, sub_pricing:int, options:int, product_links:int, groups:int}
     */
    public function removeAddonCreatedWhmcsObjects(): array
    {
        return $this->run(true);
    }

    /**
     * DRY-RUN: count exactly what {@see removeAddonCreatedWhmcsObjects} WOULD
     * delete, WITHOUT deleting anything. Same scope, same order, same shape — so
     * the numbers a preview shows are precisely what the real purge will remove.
     *
     * @return array{subs:int, sub_pricing:int, options:int, product_links:int, groups:int}
     */
    public function previewRemoval(): array
    {
        return $this->run(false);
    }

    /**
     * Shared engine. Order: pricing + sub-options (by value-link whmcs_sub_id),
     * then options (by option-link whmcs_option_id), then product-links + groups
     * (by group-link whmcs_group_id). $delete=true deletes; false counts.
     *
     * @return array{subs:int, sub_pricing:int, options:int, product_links:int, groups:int}
     */
    private function run(bool $delete): array
    {
        $counts = ['subs' => 0, 'sub_pricing' => 0, 'options' => 0, 'product_links' => 0, 'groups' => 0];

        if (!$delete) {
            // Dry-run: no transaction needed — just count.
            // 1. Sub-options + their configoptions pricing (relid = sub id).
            foreach ($this->ids('mod_contabo_config_option_value_link', 'whmcs_sub_id') as $subId) {
                $counts['sub_pricing'] += $this->affect(
                    Capsule::table('tblpricing')->where('type', 'configoptions')->where('relid', $subId), false
                );
                $counts['subs'] += $this->affect(
                    Capsule::table('tblproductconfigoptionssub')->where('id', $subId), false
                );
            }

            // 2. Options.
            foreach ($this->ids('mod_contabo_config_option_link', 'whmcs_option_id') as $optionId) {
                $counts['options'] += $this->affect(
                    Capsule::table('tblproductconfigoptions')->where('id', $optionId), false
                );
            }

            // 3. Group → product link + the group itself.
            foreach ($this->ids('mod_contabo_config_group_link', 'whmcs_group_id') as $groupId) {
                $counts['product_links'] += $this->affect(
                    Capsule::table('tblproductconfiglinks')->where('gid', $groupId), false
                );
                $counts['groups'] += $this->affect(
                    Capsule::table('tblproductconfiggroups')->where('id', $groupId), false
                );
            }

            return $counts;
        }

        Capsule::connection()->transaction(function () use (&$counts) {
            // 1. Sub-options + their configoptions pricing (relid = sub id).
            foreach ($this->ids('mod_contabo_config_option_value_link', 'whmcs_sub_id') as $subId) {
                $counts['sub_pricing'] += $this->affect(
                    Capsule::table('tblpricing')->where('type', 'configoptions')->where('relid', $subId), true
                );
                $counts['subs'] += $this->affect(
                    Capsule::table('tblproductconfigoptionssub')->where('id', $subId), true
                );
            }

            // 2. Options.
            foreach ($this->ids('mod_contabo_config_option_link', 'whmcs_option_id') as $optionId) {
                $counts['options'] += $this->affect(
                    Capsule::table('tblproductconfigoptions')->where('id', $optionId), true
                );
            }

            // 3. Group → product link + the group itself.
            foreach ($this->ids('mod_contabo_config_group_link', 'whmcs_group_id') as $groupId) {
                $counts['product_links'] += $this->affect(
                    Capsule::table('tblproductconfiglinks')->where('gid', $groupId), true
                );
                $counts['groups'] += $this->affect(
                    Capsule::table('tblproductconfiggroups')->where('id', $groupId), true
                );
            }
        });

        return $counts;
    }

    /**
     * Delete the matched rows (returning the affected count) when $delete is true,
     * or just count them (dry-run) when false.
     *
     * @param mixed $query a Capsule query builder with its where-clauses applied
     */
    private function affect($query, bool $delete): int
    {
        return $delete ? (int) $query->delete() : (int) $query->count();
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
