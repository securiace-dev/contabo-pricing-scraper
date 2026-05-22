<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * A.5.2 amendment #6 — supplies the §4 DEFAULT capability classification per
 * normalised dimension and seeds the (initially empty)
 * mod_contabo_option_capability table through
 * {@see ConfigOptionCapabilityRepository}.
 *
 * The defaults encode the post-provision change classification table in
 * docs/PHASE_A52_DESIGN_IMPACT.md §4 ("Capability matrix"): which dimension
 * changes are destructive / lose data / need a reinstall-or-recreate, which are
 * safe in-place edits, and what provisioning verb each implies. Until a Phase C
 * deploy-API verification round upgrades a row to api_verified, EVERY default
 * carries {@see ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION} —
 * these are conservative assumptions, not verified guarantees, so by the
 * amendment-6 gate ({@see ConfigOptionCapabilityRepository::canAutoApply}) none
 * of the destructive ones may auto-apply yet.
 *
 * Dimension keys are exactly the normalised keys emitted by
 * {@see DimensionParser}: Image, Region, Storage Type, Data Protection,
 * Networking:Bandwidth, Networking:IPv4, Networking:Private Networking.
 *
 * Pure provider: defaultsFor() is deterministic and DB-free; only seedForPlan()
 * touches the DB, and only through the repository chokepoint.
 *
 * PHP 7.4 polyglot: no enums, no match, no readonly, no constructor promotion,
 * no named args.
 */
final class CapabilityDefaultsProvider
{
    public const DIM_IMAGE                       = 'Image';
    public const DIM_REGION                      = 'Region';
    public const DIM_STORAGE_TYPE                = 'Storage Type';
    public const DIM_DATA_PROTECTION             = 'Data Protection';
    public const DIM_NETWORKING_BANDWIDTH        = 'Networking:Bandwidth';
    public const DIM_NETWORKING_IPV4             = 'Networking:IPv4';
    public const DIM_NETWORKING_PRIVATE          = 'Networking:Private Networking';

    /**
     * Default capability flags for one normalised dimension key (§4). The
     * returned array is shaped for {@see ConfigOptionCapabilityRepository::upsertCapability}:
     * only whitelisted BOOLEAN_FLAGS / WRITABLE_SCALARS keys, booleans as 0/1,
     * plus a provisioning_action verb and a capability_source of
     * manual_assumption.
     *
     * An unknown dimension key gets the conservative default: treat it as
     * destructive and require admin approval, but allow it at create time so a
     * new Contabo dimension still surfaces rather than being silently blocked.
     *
     * @return array<string,mixed>
     */
    public function defaultsFor(string $dimensionKey): array
    {
        if ($dimensionKey === self::DIM_IMAGE) {
            // Image/OS change → reinstall, data loss expected (§4 row 1).
            $flags = [
                'requires_reinstall'       => 1,
                'destructive_change'       => 1,
                'data_loss_expected'       => 1,
                'requires_backup_warning'  => 1,
                'requires_admin_approval'  => 1,
                'allowed_on_create'        => 1,
                'allowed_on_reinstall'     => 1,
                'allowed_on_post_provision' => 1,
                'provisioning_action'      => 'reinstall',
            ];
        } elseif ($dimensionKey === self::DIM_REGION) {
            // Region change → assume recreate, data loss expected (§4 row 2).
            $flags = [
                'requires_recreate'        => 1,
                'destructive_change'       => 1,
                'data_loss_expected'       => 1,
                'requires_backup_warning'  => 1,
                'requires_admin_approval'  => 1,
                'allowed_on_create'        => 1,
                'provisioning_action'      => 'recreate',
            ];
        } elseif ($dimensionKey === self::DIM_STORAGE_TYPE) {
            // Storage change → plan-dependent; assume destructive (§4 row 3).
            $flags = [
                'requires_reinstall'       => 1,
                'destructive_change'       => 1,
                'data_loss_expected'       => 1,
                'requires_admin_approval'  => 1,
                'allowed_on_create'        => 1,
                'provisioning_action'      => 'reinstall',
            ];
        } elseif ($dimensionKey === self::DIM_DATA_PROTECTION) {
            // Backup toggle → in-place, non-destructive, billing change (§4 row 5).
            $flags = [
                'destructive_change'        => 0,
                'allowed_on_create'         => 1,
                'allowed_on_post_provision' => 1,
                'allowed_on_upgrade'        => 1,
                'allowed_on_downgrade'      => 1,
                'billing_change_possible'   => 1,
                'provisioning_action'       => 'toggle_backup',
            ];
        } elseif ($dimensionKey === self::DIM_NETWORKING_IPV4) {
            // IPv4 qty → in-place, non-destructive, billing change (§4 row 4).
            $flags = [
                'destructive_change'        => 0,
                'allowed_on_create'         => 1,
                'allowed_on_post_provision' => 1,
                'allowed_on_upgrade'        => 1,
                'allowed_on_downgrade'      => 1,
                'billing_change_possible'   => 1,
                'provisioning_action'       => 'adjust_ipv4',
            ];
        } elseif ($dimensionKey === self::DIM_NETWORKING_BANDWIDTH) {
            // Bandwidth tier → in-place, non-destructive, billing change (§4 row 6).
            $flags = [
                'destructive_change'        => 0,
                'allowed_on_create'         => 1,
                'allowed_on_post_provision' => 1,
                'billing_change_possible'   => 1,
                'provisioning_action'       => 'adjust_bandwidth',
            ];
        } elseif ($dimensionKey === self::DIM_NETWORKING_PRIVATE) {
            // Private net toggle → in-place, non-destructive (§4 row 7).
            $flags = [
                'destructive_change'        => 0,
                'allowed_on_create'         => 1,
                'allowed_on_post_provision' => 1,
                'provisioning_action'       => 'toggle_private_net',
            ];
        } else {
            // Unknown dimension → conservative: assume destructive and gate on
            // admin approval, but still allow it on create.
            $flags = [
                'requires_admin_approval' => 1,
                'destructive_change'      => 1,
                'allowed_on_create'       => 1,
            ];
        }

        // Every default is an assumption until a Phase C deploy-API check
        // promotes it to api_verified. Pin it explicitly so the auto-apply gate
        // (amendment #6) treats every destructive default as approval-gated.
        $flags['capability_source'] = ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION;

        return $flags;
    }

    /**
     * Seed default capabilities for one plan from DimensionParser specs.
     *
     * Each spec is `{dimension_key, values: [{value_key, ...}], ...}`; for every
     * value of every spec this upserts {@see defaultsFor} for that spec's
     * dimension into the repository, keyed by (planSlug, dimension_key,
     * value_key). Upserts are idempotent by that key, so re-seeding a plan does
     * not duplicate rows.
     *
     * @param list<array{dimension_key:string,values:list<array{value_key:string}>}> $specs
     * @return int number of (dimension, value) capability rows upserted
     */
    public function seedForPlan(string $planSlug, array $specs, ConfigOptionCapabilityRepository $repo): int
    {
        $count = 0;
        foreach ($specs as $spec) {
            if (!is_array($spec) || !isset($spec['dimension_key'])) {
                continue;
            }
            $dimensionKey = (string) $spec['dimension_key'];
            $defaults     = $this->defaultsFor($dimensionKey);

            $values = isset($spec['values']) && is_array($spec['values']) ? $spec['values'] : [];
            foreach ($values as $value) {
                if (!is_array($value) || !isset($value['value_key'])) {
                    continue;
                }
                $repo->upsertCapability($planSlug, $dimensionKey, (string) $value['value_key'], $defaults);
                $count++;
            }
        }
        return $count;
    }
}
