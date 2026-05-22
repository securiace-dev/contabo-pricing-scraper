<?php
declare(strict_types=1);

namespace ContaboPricing;

use WHMCS\Database\Capsule;

/**
 * Phase A.6.1 — amendment 5 (foundation / READ-ONLY scaffold).
 *
 * The TRUE recurring revenue of a hosting service is NOT `tblhosting.recurringamount`
 * alone. WHMCS stores the base product recurring there, but the customer also pays
 * for:
 *   - selected configurable options (tblhostingconfigoptions → tblpricing
 *     type=configoptions, the cycle column matching the service billingcycle);
 *   - product addons (tblhostingaddons.recurring);
 *   - (later) discounts / promo overrides.
 *
 * This resolver sums those into a breakdown so Phase B's RenewalEngine prices the
 * WHOLE configuration when it decides whether a vendor cost increase warrants a
 * renewal bump — never a silent reprice and never a wrong margin from a bare
 * recurringamount.
 *
 * THIS CLASS IS READ-ONLY. It performs zero writes. It is a scaffold: A.6.x wires
 * snapshot capture, Phase B wires resolveFromSnapshot into the renewal decision.
 *
 * Testability: the raw DB reads live in small overridable protected methods
 * (fetchBase / fetchConfigOptions / fetchAddons) so unit tests can inject fixtures
 * without depending on join/sum support in the test Capsule. Production overrides
 * nothing and reads through WHMCS\Database\Capsule.
 *
 * PHP 7.4 polyglot.
 */
class ServiceRevenueResolver
{
    /**
     * Map a WHMCS billingcycle string to the tblpricing cycle column.
     *
     * @var array<string,string>
     */
    private const CYCLE_COLUMN = [
        'monthly'        => 'monthly',
        'quarterly'      => 'quarterly',
        'semi-annually'  => 'semiannually',
        'semiannually'   => 'semiannually',
        'annually'       => 'annually',
        'biennially'     => 'biennially',
        'triennially'    => 'triennially',
    ];

    /**
     * Resolve true recurring revenue for a live service straight from the WHMCS
     * tables.
     *
     * @return array{base:float, config_options:float, addons:float, total:float, breakdown:array<string,mixed>}
     */
    public function resolveForService(int $serviceId): array
    {
        $base = $this->fetchBase($serviceId);
        $baseAmount = (float) ($base['base'] ?? 0.0);
        $currentCharge = (float) ($base['current_charge'] ?? 0.0);
        $billingCycle = (string) ($base['billingcycle'] ?? 'monthly');
        $cycleColumn = $this->cycleColumn($billingCycle);

        $configRows = $this->fetchConfigOptions($serviceId);
        $configTotal = 0.0;
        $configBreakdown = [];
        foreach ($configRows as $r) {
            $qty = (int) ($r['qty'] ?? 1);
            if ($qty < 1) {
                $qty = 1;
            }
            $unit = (float) ($r[$cycleColumn] ?? 0.0);
            $line = $unit * $qty;
            $configTotal += $line;
            $configBreakdown[] = [
                'sub_id' => (int) ($r['sub_id'] ?? ($r['relid'] ?? 0)),
                'qty'    => $qty,
                'unit'   => $unit,
                'line'   => $line,
            ];
        }

        $addonRows = $this->fetchAddons($serviceId);
        $addonTotal = 0.0;
        $addonBreakdown = [];
        foreach ($addonRows as $r) {
            $recurring = (float) ($r['recurring'] ?? 0.0);
            $addonTotal += $recurring;
            $addonBreakdown[] = [
                'addon_id'  => (int) ($r['id'] ?? 0),
                'name'      => (string) ($r['name'] ?? ''),
                'recurring' => $recurring,
            ];
        }

        return $this->assemble($baseAmount, $configTotal, $addonTotal, [
            'source'         => 'service',
            'service_id'     => $serviceId,
            'billing_cycle'  => $billingCycle,
            'cycle_column'   => $cycleColumn,
            'current_charge' => $currentCharge, // stale tblhosting.recurringamount, for drift comparison
            'config_options' => $configBreakdown,
            'addons'         => $addonBreakdown,
        ]);
    }

    /**
     * Resolve true recurring revenue from a stored snapshot row
     * (mod_contabo_service_config_snapshot). Preferred for renewal margin so the
     * computation is reproducible and immune to later catalog drift.
     *
     * Snapshot carries the resolved figures directly:
     *   base_price_snapshot           = base recurring at order time
     *   config_option_price_snapshot  = sum of selected option recurrings
     *   addon_price_snapshot          = sum of addon recurrings (optional)
     *
     * @param array<string,mixed> $snapshotRow
     * @return array{base:float, config_options:float, addons:float, total:float, breakdown:array<string,mixed>}
     */
    public function resolveFromSnapshot(array $snapshotRow): array
    {
        $base = (float) ($snapshotRow['base_price_snapshot'] ?? 0.0);
        $config = (float) ($snapshotRow['config_option_price_snapshot'] ?? 0.0);
        // Addon snapshot is optional in the v5 schema; default 0.0.
        $addons = (float) ($snapshotRow['addon_price_snapshot'] ?? 0.0);

        return $this->assemble($base, $config, $addons, [
            'source'                       => 'snapshot',
            'service_id'                   => (int) ($snapshotRow['service_id'] ?? 0),
            'snapshot_id'                  => isset($snapshotRow['id']) ? (int) $snapshotRow['id'] : null,
            'pricing_version_snapshot'     => $snapshotRow['pricing_version_snapshot'] ?? null,
            'base_price_snapshot'          => $base,
            'config_option_price_snapshot' => $config,
            'addon_price_snapshot'         => $addons,
        ]);
    }

    /**
     * Resolve the tblpricing cycle column for a billing cycle, defaulting to
     * monthly for unknown/empty cycles.
     */
    public function cycleColumn(string $billingCycle): string
    {
        $key = strtolower(trim($billingCycle));
        return self::CYCLE_COLUMN[$key] ?? 'monthly';
    }

    /**
     * @param array<string,mixed> $extra
     * @return array{base:float, config_options:float, addons:float, total:float, breakdown:array<string,mixed>}
     */
    private function assemble(float $base, float $config, float $addons, array $extra): array
    {
        $total = $base + $config + $addons;
        return [
            'base'           => $base,
            'config_options' => $config,
            'addons'         => $addons,
            'total'          => $total,
            'breakdown'      => array_merge([
                'base'           => $base,
                'config_options' => $config,
                'addons'         => $addons,
                'total'          => $total,
            ], $extra),
        ];
    }

    // ----------------------------------------------------------------------
    // READ-ONLY data access (overridable for unit tests). No writes anywhere.
    // ----------------------------------------------------------------------

    /**
     * Base recurring + billing cycle for the service.
     *
     * @return array{recurringamount:float, billingcycle:string}
     */
    protected function fetchBase(int $serviceId): array
    {
        $row = Capsule::table('tblhosting')
            ->where('id', $serviceId)
            ->first();
        if ($row === null) {
            return ['base' => 0.0, 'billingcycle' => 'monthly', 'current_charge' => 0.0];
        }
        $row = (array) $row; // real Capsule returns stdClass; normalize
        $cycle = (string) ($row['billingcycle'] ?? 'monthly');

        // The TRUE base is the PRODUCT's current catalog recurring price for the
        // cycle — NOT tblhosting.recurringamount. recurringamount is a snapshot
        // that already folds in config options and DRIFTS when their prices
        // change (preflight §5), so reading it here + adding config again would
        // double-count. We keep recurringamount only as `current_charge`: what
        // the customer is billed today, used by callers for drift comparison.
        $col = $this->cycleColumn($cycle);
        $pp = Capsule::table('tblpricing')
            ->where('type', 'product')
            ->where('relid', (int) ($row['packageid'] ?? 0))
            ->where('currency', WhmcsConfigOptionsAdapter::INR_CURRENCY_ID)
            ->first();
        $pp = $pp !== null ? (array) $pp : null;
        $base = $pp !== null ? (float) ($pp[$col] ?? 0.0) : 0.0;
        // A negative product price (-1.00 = cycle disabled in WHMCS) is not real
        // revenue; treat it as 0 so a disabled cycle never produces a phantom base.
        if ($base < 0.0) {
            $base = 0.0;
        }

        return [
            'base'           => $base,
            'billingcycle'   => $cycle,
            'current_charge' => (float) ($row['recurringamount'] ?? 0.0),
        ];
    }

    /**
     * Selected configurable options for the service, each joined to its
     * tblpricing(configoptions) row so all six cycle columns are available.
     * Returned rows carry: sub_id, qty, and the six cycle columns.
     *
     * @return list<array<string,mixed>>
     */
    protected function fetchConfigOptions(int $serviceId): array
    {
        $selected = Capsule::table('tblhostingconfigoptions')
            ->where('relid', $serviceId)
            ->get();

        $out = [];
        foreach ($selected as $sel) {
            $sel = (array) $sel;
            $subId = (int) ($sel['optionid'] ?? 0); // relid on tblpricing = sub-option id
            $qty = (int) ($sel['qty'] ?? 1);

            $price = Capsule::table('tblpricing')
                ->where('type', 'configoptions')
                ->where('relid', $subId)
                ->where('currency', WhmcsConfigOptionsAdapter::INR_CURRENCY_ID)
                ->first();
            $price = $price !== null ? (array) $price : null; // real Capsule returns stdClass

            $row = ['sub_id' => $subId, 'qty' => $qty];
            foreach (['monthly', 'quarterly', 'semiannually', 'annually', 'biennially', 'triennially'] as $col) {
                $row[$col] = $price !== null ? (float) ($price[$col] ?? 0.0) : 0.0;
            }
            $out[] = $row;
        }
        return $out;
    }

    /**
     * Product addons attached to the service. Returns rows with id, name,
     * recurring.
     *
     * @return list<array<string,mixed>>
     */
    protected function fetchAddons(int $serviceId): array
    {
        $rows = Capsule::table('tblhostingaddons')
            ->where('hostingid', $serviceId)
            ->get();

        $out = [];
        foreach ($rows as $r) {
            $r = (array) $r;
            $out[] = [
                'id'        => (int) ($r['id'] ?? 0),
                'name'      => (string) ($r['name'] ?? ''),
                'recurring' => (float) ($r['recurring'] ?? 0.0),
            ];
        }
        return $out;
    }
}
