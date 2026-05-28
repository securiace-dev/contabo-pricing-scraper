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

        // A.6.5 multi-currency guard (amendment 10): the resolver only knows INR
        // pricing. If the service is billed in another currency, the figures below
        // are NOT its real revenue — surface that explicitly so no caller (renewal
        // margin, snapshot) silently treats a non-INR service as correctly priced.
        // currency_id 0 = unknown/no client on file → treat as the INR default.
        $currencyId = (int) ($base['currency_id'] ?? 0);
        $currencySupported = ($currencyId === 0 || $currencyId === WhmcsConfigOptionsAdapter::INR_CURRENCY_ID);

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

        $discountResult = $this->fetchDiscounts($serviceId, $baseAmount, $currencySupported);
        $discountAmount = $discountResult['amount'];

        return $this->assemble($baseAmount, $configTotal, $addonTotal, [
            'source'             => 'service',
            'service_id'         => $serviceId,
            'billing_cycle'      => $billingCycle,
            'cycle_column'       => $cycleColumn,
            'current_charge'     => $currentCharge, // WHMCS service amount (tblhosting.amount) — drift comparison only, NOT the pricing base
            'service_amount'     => $currentCharge, // explicit label for the WHMCS recurring service amount
            'currency_id'        => $currencyId,
            'currency_supported' => $currencySupported, // false ⇒ figures are NOT real revenue (non-INR service)
            'config_options'     => $configBreakdown,
            'addons'             => $addonBreakdown,
            'discounts'          => $discountAmount,
            'discount_breakdown' => $discountResult,
            'discounts_partial'  => $discountResult['partial'],
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
     *
     * Addons are intentionally NOT part of the v1 snapshot revenue path: the
     * snapshot table has no addon column, so this path reports addons as 0.0. The
     * live {@see resolveForService} path is the one that sums tblhostingaddons.
     * (Backlog: add an addon_price_snapshot column + capture it once a product in
     * the catalog actually carries WHMCS product addons — today's VPS catalog has
     * none.) Reporting 0.0 explicitly here keeps the snapshot-vs-live difference
     * documented rather than a silent read of a column that doesn't exist.
     *
     * @param array<string,mixed> $snapshotRow
     * @return array{base:float, config_options:float, addons:float, total:float, breakdown:array<string,mixed>}
     */
    public function resolveFromSnapshot(array $snapshotRow): array
    {
        $base = (float) ($snapshotRow['base_price_snapshot'] ?? 0.0);
        $config = (float) ($snapshotRow['config_option_price_snapshot'] ?? 0.0);
        $addons = 0.0; // not snapshotted in v1 (see method docblock)

        return $this->assemble($base, $config, $addons, [
            'source'                       => 'snapshot',
            'service_id'                   => (int) ($snapshotRow['service_id'] ?? 0),
            'snapshot_id'                  => isset($snapshotRow['id']) ? (int) $snapshotRow['id'] : null,
            'pricing_version_snapshot'     => $snapshotRow['pricing_version_snapshot'] ?? null,
            'base_price_snapshot'          => $base,
            'config_option_price_snapshot' => $config,
            'addons_in_snapshot'           => false, // v1: addons resolved via the live path only
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

    /**
     * Resolve any applicable discount for a live INR service.
     *
     * Checks (in order):
     *   1. tblpromotions linked via the most recent tblorders row (recurring
     *      promos only; one-time-next-cycle promos already applied >1 time are
     *      skipped).
     *   2. tblclientdiscounts for the service owner (expiry checked in PHP).
     *
     * Whichever discount yields the larger monetary amount wins; client
     * discounts never combine with promo discounts here (greater-of logic).
     *
     * Non-INR services are skipped immediately — their figures are not real
     * revenue so applying a discount would compound the inaccuracy.
     *
     * @return array{amount:float, type:string, source:string, partial:bool}
     */
    protected function fetchDiscounts(int $serviceId, float $baseAmount, bool $currencySupported): array
    {
        // Non-INR services: discounts are meaningless (figures aren't real revenue).
        if (!$currencySupported) {
            return ['amount' => 0.0, 'type' => 'none', 'source' => 'non_inr_skipped', 'partial' => false];
        }

        $discountAmount = 0.0;
        $discountType   = 'none';
        $discountSource = 'none';
        $partial        = false;

        try {
            // Guard: tblpromotions schema varies by WHMCS version; the `recurring`
            // column was added in WHMCS 7.x — older installs lack it.
            if (!Capsule::schema()->hasColumn('tblpromotions', 'recurring')) {
                return ['amount' => 0.0, 'type' => 'none', 'source' => 'schema_guard', 'partial' => true];
            }

            // Find the most recent order for this service.
            $orderRaw = Capsule::table('tblorders')
                ->where('serviceid', $serviceId)
                ->orderByDesc('id')
                ->select(['id', 'userid', 'promoid'])
                ->first();
            $order = $orderRaw !== null ? (array) $orderRaw : null;

            if ($order !== null && (int) ($order['promoid'] ?? 0) > 0) {
                $promoRaw = Capsule::table('tblpromotions')
                    ->where('id', (int) ($order['promoid'] ?? 0))
                    ->first();
                $promo = $promoRaw !== null ? (array) $promoRaw : null;

                if ($promo !== null && !empty($promo['recurring'])) {
                    $skip = false;

                    // recurnextcycle = one-time next-cycle-only promo.
                    // If the same promo has been applied to >1 orders by this
                    // client it has already been used; skip it.
                    if (!empty($promo['recurnextcycle'])) {
                        $appliedCount = Capsule::table('tblorders')
                            ->where('promoid', (int) ($order['promoid'] ?? 0))
                            ->where('userid', (int) ($order['userid'] ?? 0))
                            ->count();
                        if ($appliedCount > 1) {
                            $skip = true;
                        }
                    }

                    if (!$skip) {
                        $type  = (string) ($promo['type'] ?? '');
                        $value = (float) ($promo['value'] ?? 0.0);

                        if ($type === 'percentage') {
                            $discountAmount = $baseAmount * $value / 100.0;
                        } elseif ($type === 'fixed_amount') {
                            $discountAmount = min($value, $baseAmount);
                        }

                        if ($discountAmount > 0.0) {
                            $discountType   = $type;
                            $discountSource = 'promo';
                        }
                    }
                }
            }

            // Check tblclientdiscounts (client-specific recurring percentage).
            // Expiry is filtered in PHP to avoid closure-where constructs that
            // require Eloquent features not available in every test environment.
            $clientId = (int) ($order !== null ? ($order['userid'] ?? 0) : 0);
            // A client discount applies to the client regardless of whether an
            // order row exists — fall back to the service's owner (tblhosting).
            if ($clientId === 0) {
                $svcRaw = Capsule::table('tblhosting')
                    ->where('id', $serviceId)
                    ->select(['userid'])
                    ->first();
                if ($svcRaw !== null) {
                    $clientId = (int) (((array) $svcRaw)['userid'] ?? 0);
                }
            }
            if ($clientId > 0) {
                $clientDiscs = Capsule::table('tblclientdiscounts')
                    ->where('clientid', $clientId)
                    ->get();
                $now = date('Y-m-d H:i:s');
                foreach ($clientDiscs as $cdRaw) {
                    $cd     = (array) $cdRaw;
                    $expiry = isset($cd['expiry']) && $cd['expiry'] !== null ? (string) $cd['expiry'] : null;
                    if ($expiry !== null && $expiry <= $now) {
                        continue; // expired
                    }
                    $clientDiscAmt = $baseAmount * (float) ($cd['value'] ?? 0.0) / 100.0;
                    // Apply whichever discount is greater (greater-of, not additive).
                    if ($clientDiscAmt > $discountAmount) {
                        $discountAmount = $clientDiscAmt;
                        $discountType   = 'percentage';
                        $discountSource = 'client_discount';
                    }
                }
            }
        } catch (\Throwable $e) {
            return ['amount' => 0.0, 'type' => 'none', 'source' => 'error', 'partial' => true];
        }

        return [
            'amount'  => round($discountAmount, 2),
            'type'    => $discountType,
            'source'  => $discountSource,
            'partial' => $partial,
        ];
    }

    // ----------------------------------------------------------------------
    // READ-ONLY data access (overridable for unit tests). No writes anywhere.
    // ----------------------------------------------------------------------

    /**
     * Base recurring + billing cycle for the service.
     *
     * @return array{base:float, billingcycle:string, current_charge:float, service_amount:float, currency_id:int}
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

        // The service's billing currency lives on its client (tblclients.currency);
        // tblhosting has no currency column. Used by the multi-currency guard so a
        // non-INR service isn't silently priced off INR catalog rows.
        $currencyId = 0;
        $userId = (int) ($row['userid'] ?? 0);
        if ($userId > 0) {
            $client = Capsule::table('tblclients')->where('id', $userId)->first();
            if ($client !== null) {
                $client = (array) $client;
                $currencyId = (int) ($client['currency'] ?? 0);
            }
        }

        // The TRUE base is the PRODUCT's current catalog recurring price for the
        // cycle — NOT the service's stored charge (tblhosting.amount). The stored
        // charge already folds in config options and DRIFTS when their prices
        // change (preflight §5), so reading it here + adding config again would
        // double-count. We keep it only as `current_charge` (read below from the
        // real `amount` column): what the customer is billed today, for drift
        // comparison.
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

        // current_charge = the WHMCS service recurring amount, read from the REAL
        // column `tblhosting.amount`. There is NO `recurringamount` column on a
        // live WHMCS install — that is an API/model field name, not a raw column;
        // the previous read of `recurringamount` silently resolved to 0.0 in prod
        // (caught by the production currency audit). The live-schema smoke now
        // guards this. current_charge is what the customer is billed today, used
        // by callers ONLY for drift comparison — it is NOT the pricing base (the
        // base comes from tblpricing above).
        if (!array_key_exists('amount', $row)) {
            // tblhosting.amount is MANDATORY WHMCS schema (the live-schema smoke
            // proves it). A missing column is a real schema mismatch — fail loud
            // rather than mask a monetary value as 0.0.
            throw new SchemaMismatchException(
                'tblhosting.amount missing for service #' . $serviceId
                . ' — WHMCS schema mismatch (mandatory recurring-charge column absent).'
            );
        }
        $currentCharge = (float) $row['amount'];

        return [
            'base'           => $base,
            'billingcycle'   => $cycle,
            'current_charge' => $currentCharge,
            'service_amount' => $currentCharge, // clearer alias: the WHMCS tblhosting.amount value
            'currency_id'    => $currencyId,
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
