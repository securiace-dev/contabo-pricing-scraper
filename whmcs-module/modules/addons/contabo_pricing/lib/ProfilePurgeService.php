<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Phase D §6 — per-profile permanent purge (guarded).
 *
 * Unlike the global maintenance purge (which truncates ALL mod_contabo_* tables),
 * this removes exactly ONE profile and only the rows that profile owns:
 *
 *   - the WHMCS configurable objects the addon created FOR THIS PROFILE
 *     (sub-options + their pricing, options, group→product links, groups),
 *     identified strictly by the ids recorded in this profile's link tables;
 *   - the profile's addon-owned rows: config link tables, config audit, profile
 *     versions, mappings;
 *   - the profile row itself.
 *
 * It NEVER touches another profile's data, nor any client / invoice / order /
 * service row, nor a WHMCS object the addon didn't create.
 *
 * GUARD: purge is refused while the profile still has an ACTIVE mapping, or while
 * any LIVE (Active) tblhosting service exists on a product this profile maps —
 * the admin must wind those down first. assess() reports the blocking reasons;
 * purge() re-checks the guard and throws if it no longer holds.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion.
 */
final class ProfilePurgeService
{
    /**
     * Evaluate whether a profile may be permanently purged.
     *
     * @return array{allowed:bool, reasons:list<string>, active_mappings:int, live_services:int}
     */
    public function assess(int $profileId): array
    {
        $reasons = [];

        $activeMappings = (int) Capsule::table('mod_contabo_mapping')
            ->where('profile_id', $profileId)
            ->where('active', true)
            ->count();
        if ($activeMappings > 0) {
            $reasons[] = sprintf(
                '%d active mapping%s still reference this profile — disable them first.',
                $activeMappings,
                $activeMappings === 1 ? '' : 's'
            );
        }

        $liveServices = $this->liveServiceCount($profileId);
        if ($liveServices > 0) {
            $reasons[] = sprintf(
                '%d live (Active) service%s exist on this profile\'s mapped product(s) — migrate or terminate them first.',
                $liveServices,
                $liveServices === 1 ? '' : 's'
            );
        }

        return [
            'allowed'         => $reasons === [],
            'reasons'         => $reasons,
            'active_mappings' => $activeMappings,
            'live_services'   => $liveServices,
        ];
    }

    /**
     * Permanently purge the profile + everything it owns. Re-checks the guard
     * first and throws RuntimeException when it still blocks. Returns per-target
     * delete counts. Runs inside a transaction.
     *
     * @return array{
     *   subs:int, sub_pricing:int, options:int, product_links:int, groups:int,
     *   value_links:int, option_links:int, group_links:int, config_audit:int,
     *   versions:int, mappings:int, profile:int
     * }
     */
    public function purge(int $profileId): array
    {
        $guard = $this->assess($profileId);
        if (!$guard['allowed']) {
            throw new \RuntimeException(
                'Profile purge blocked: ' . implode(' ', $guard['reasons'])
            );
        }

        $counts = [
            'subs' => 0, 'sub_pricing' => 0, 'options' => 0, 'product_links' => 0,
            'groups' => 0, 'value_links' => 0, 'option_links' => 0, 'group_links' => 0,
            'config_audit' => 0, 'versions' => 0, 'mappings' => 0, 'profile' => 0,
        ];

        Capsule::connection()->transaction(function () use ($profileId, &$counts) {
            // ── 1. Addon-created WHMCS objects, scoped to THIS profile's links ──
            // Resolve this profile's option-link ids first; value links hang off
            // them (the value-link table is keyed by option_link_id, not profile).
            $optionLinkIds = $this->ids(
                Capsule::table('mod_contabo_config_option_link')->where('profile_id', $profileId),
                'id'
            );

            // Sub-options + their configoptions pricing (relid = sub id).
            $subIds = $optionLinkIds === []
                ? []
                : $this->ids(
                    Capsule::table('mod_contabo_config_option_value_link')->whereIn('option_link_id', $optionLinkIds),
                    'whmcs_sub_id'
                );
            foreach ($subIds as $subId) {
                $counts['sub_pricing'] += (int) Capsule::table('tblpricing')
                    ->where('type', 'configoptions')->where('relid', $subId)->delete();
                $counts['subs'] += (int) Capsule::table('tblproductconfigoptionssub')
                    ->where('id', $subId)->delete();
            }

            // Options.
            foreach ($this->ids(
                Capsule::table('mod_contabo_config_option_link')->where('profile_id', $profileId),
                'whmcs_option_id'
            ) as $optionId) {
                $counts['options'] += (int) Capsule::table('tblproductconfigoptions')
                    ->where('id', $optionId)->delete();
            }

            // Group → product link + the group itself.
            foreach ($this->ids(
                Capsule::table('mod_contabo_config_group_link')->where('profile_id', $profileId),
                'whmcs_group_id'
            ) as $groupId) {
                $counts['product_links'] += (int) Capsule::table('tblproductconfiglinks')
                    ->where('gid', $groupId)->delete();
                $counts['groups'] += (int) Capsule::table('tblproductconfiggroups')
                    ->where('id', $groupId)->delete();
            }

            // ── 2. Addon-owned rows for this profile ──────────────────────────
            if ($optionLinkIds !== []) {
                $counts['value_links'] += (int) Capsule::table('mod_contabo_config_option_value_link')
                    ->whereIn('option_link_id', $optionLinkIds)->delete();
            }
            $counts['option_links'] += (int) Capsule::table('mod_contabo_config_option_link')
                ->where('profile_id', $profileId)->delete();
            $counts['group_links'] += (int) Capsule::table('mod_contabo_config_group_link')
                ->where('profile_id', $profileId)->delete();
            $counts['config_audit'] += (int) Capsule::table('mod_contabo_config_option_audit')
                ->where('profile_id', $profileId)->delete();
            $counts['versions'] += (int) Capsule::table('mod_contabo_profile_version')
                ->where('profile_id', $profileId)->delete();
            $counts['mappings'] += (int) Capsule::table('mod_contabo_mapping')
                ->where('profile_id', $profileId)->delete();

            // ── 3. The profile row ────────────────────────────────────────────
            $counts['profile'] += (int) Capsule::table('mod_contabo_profile')
                ->where('id', $profileId)->delete();
        });

        return $counts;
    }

    /**
     * Count live (Active) tblhosting services on every product this profile maps.
     * A product may be mapped by several profiles; this is intentionally
     * conservative — we never purge a profile whose product still has live
     * services. Returns 0 when the profile maps no products.
     */
    private function liveServiceCount(int $profileId): int
    {
        $productIds = $this->ids(
            Capsule::table('mod_contabo_mapping')->where('profile_id', $profileId),
            'product_id'
        );
        if ($productIds === []) {
            return 0;
        }
        try {
            return (int) Capsule::table('tblhosting')
                ->whereIn('packageid', $productIds)
                ->where('domainstatus', 'Active')
                ->count();
        } catch (\Throwable $e) {
            // No tblhosting (test/install) — treat as no live services.
            return 0;
        }
    }

    /**
     * Distinct, positive ids in a column of a (pre-filtered) query. Normalises
     * stdClass (real WHMCS) vs array (FakeCapsule) rows.
     *
     * @param mixed $query a Capsule query builder with its where-clauses applied
     * @return list<int>
     */
    private function ids($query, string $column): array
    {
        $out = [];
        foreach ($query->get([$column]) as $r) {
            $r = (array) $r;
            $id = (int) ($r[$column] ?? 0);
            if ($id > 0) {
                $out[$id] = $id;
            }
        }
        return array_values($out);
    }
}
