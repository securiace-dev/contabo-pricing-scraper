<?php
declare(strict_types=1);

namespace ContaboPricing;

/**
 * The ONLY place in the codebase that performs money math for the renewal
 * engine. Lives in pure-static land: no DB, no I/O, no logging.
 *
 * Invariants (do not break — the acceptance tests will fail loudly):
 *
 *   - Percent ↔ ratio conversion happens here, exactly once.
 *     RenewalEngine and tests pass IN percents (e.g. `floor = 15.00`).
 *     This class converts to ratios internally (`floor / 100 = 0.15`).
 *   - The floor formula is `minimum_sell = landed / (1 - floorRatio)`,
 *     NOT `landed / (1 - floorPct)`. With cost ₹850 and floor 15 → ₹1000.00.
 *   - Landed cost is computed PER MONTH first, then multiplied by cycleMonths.
 *     Never compare a monthly cost to a `recurringamount` directly — use
 *     CycleNormalizer + landedCostForCycle().
 *   - Vendor tax inclusion in landed cost depends on `vendorTaxRecoverable`:
 *     recoverable → tax excluded; non-recoverable → tax included (current
 *     `unregistered_no_output_tax` mode).
 *   - Net revenue strips output tax ONLY when prices are gross-inclusive.
 *     Exclusive pricing: `gross == net revenue` (gross is already net of
 *     output tax because output tax was never bundled in).
 *
 * PHP 7.4 polyglot: typed properties OK; no match, no readonly, no enums.
 */
final class MarginCalculator
{
    /**
     * Landed cost PER MONTH in local currency (e.g. INR), including buffers
     * and non-recoverable vendor tax.
     *
     *   monthly_after_fx     = eurMonthly × fxRate
     *   fx_buffer            = monthly_after_fx × (fxBufferPct / 100)
     *   payment_buffer       = monthly_after_fx × (paymentBufferPct / 100)
     *   vendor_tax_amount    = monthly_after_fx × (vendorTaxRatePct / 100)
     *   non_recov_vendor_tax = vendorTaxRecoverable ? 0 : vendor_tax_amount
     *   landed_monthly       = monthly_after_fx
     *                        + non_recov_vendor_tax
     *                        + fx_buffer
     *                        + payment_buffer
     *
     * @param float $eurMonthly            Vendor base cost in EUR, monthly.
     * @param float $fxRate                EUR → local currency rate.
     * @param float $fxBufferPct           FX volatility buffer (percent, e.g. 2.00).
     * @param float $paymentBufferPct      Gateway/FX processing buffer (percent, e.g. 2.00).
     * @param float $vendorTaxRatePct      Vendor tax rate (percent, e.g. 18.00).
     * @param bool  $vendorTaxRecoverable  When true, vendor tax is NOT added to landed.
     * @return float Landed cost per month, in local currency.
     */
    public static function landedCostMonthly(
        float $eurMonthly,
        float $fxRate,
        float $fxBufferPct,
        float $paymentBufferPct,
        float $vendorTaxRatePct,
        bool $vendorTaxRecoverable
    ): float {
        $monthlyAfterFx       = $eurMonthly * $fxRate;
        $fxBuffer             = $monthlyAfterFx * ($fxBufferPct / 100.0);
        $paymentBuffer        = $monthlyAfterFx * ($paymentBufferPct / 100.0);
        $vendorTaxAmount      = $monthlyAfterFx * ($vendorTaxRatePct / 100.0);
        $nonRecoverableVendor = $vendorTaxRecoverable ? 0.0 : $vendorTaxAmount;

        return $monthlyAfterFx + $nonRecoverableVendor + $fxBuffer + $paymentBuffer;
    }

    /**
     * Landed cost for a full billing cycle in local currency = monthly × cycleMonths.
     *
     * @param float $eurMonthly            Vendor base cost in EUR, monthly.
     * @param float $fxRate                EUR → local currency rate.
     * @param float $fxBufferPct           FX volatility buffer (percent).
     * @param float $paymentBufferPct      Gateway/FX processing buffer (percent).
     * @param float $vendorTaxRatePct      Vendor tax rate (percent).
     * @param bool  $vendorTaxRecoverable  When true, vendor tax excluded from landed.
     * @param int   $cycleMonths           From CycleNormalizer (1/3/6/12/24/36).
     * @return float Landed cost for the whole cycle, in local currency.
     * @throws \InvalidArgumentException when cycleMonths <= 0.
     */
    public static function landedCostForCycle(
        float $eurMonthly,
        float $fxRate,
        float $fxBufferPct,
        float $paymentBufferPct,
        float $vendorTaxRatePct,
        bool $vendorTaxRecoverable,
        int $cycleMonths
    ): float {
        if ($cycleMonths <= 0) {
            throw new \InvalidArgumentException('MarginCalculator: cycleMonths must be > 0');
        }

        $monthly = self::landedCostMonthly(
            $eurMonthly,
            $fxRate,
            $fxBufferPct,
            $paymentBufferPct,
            $vendorTaxRatePct,
            $vendorTaxRecoverable
        );

        return $monthly * $cycleMonths;
    }

    /**
     * Phase B (§13 / amendment 5) — landed MONTHLY cost of a WHOLE configured
     * service: the base product plus every selected configurable option.
     *
     * landedCostMonthly is linear in EUR, so this equals
     * landedCostMonthly(baseEur + Σ selection EUR). Each selection's EUR is
     * clamped to >= 0 (a cheaper-than-default option never *reduces* landed
     * cost — amendment 1) and multiplied by its quantity (>= 1; matches the
     * ServiceRevenueResolver qty convention so revenue and cost stay aligned).
     *
     * Pair this with ServiceRevenueResolver's total (whole-config revenue) so a
     * renewal margin reflects the full configuration, never base-only.
     *
     * @param float $baseEurMonthly Vendor base cost in EUR, monthly.
     * @param list<array{eur_monthly:float,qty?:int}> $selections Per-option EUR
     *        deltas (monthly, per unit) + optional quantity.
     * @param float $fxRate
     * @param float $fxBufferPct
     * @param float $paymentBufferPct
     * @param float $vendorTaxRatePct
     * @param bool  $vendorTaxRecoverable
     * @return float Whole-config landed cost per month, local currency.
     */
    public static function landedCostWithSelections(
        float $baseEurMonthly,
        array $selections,
        float $fxRate,
        float $fxBufferPct,
        float $paymentBufferPct,
        float $vendorTaxRatePct,
        bool $vendorTaxRecoverable
    ): float {
        $totalEur = max(0.0, $baseEurMonthly);
        foreach ($selections as $sel) {
            $eur = max(0.0, (float) ($sel['eur_monthly'] ?? 0.0));
            $qty = (int) ($sel['qty'] ?? 1);
            if ($qty < 1) {
                $qty = 1;
            }
            $totalEur += $eur * $qty;
        }

        return self::landedCostMonthly(
            $totalEur,
            $fxRate,
            $fxBufferPct,
            $paymentBufferPct,
            $vendorTaxRatePct,
            $vendorTaxRecoverable
        );
    }

    /**
     * Sell price for a billing cycle in local currency, computed from the
     * landed MONTHLY cost and the profile's markup strategy. Result is
     * rounded to 2 decimal places (currency precision).
     *
     * Markup strategies (per profile_version):
     *   - 'cost_plus_pct'    — monthly = landed × (1 + markupValue/100)
     *   - 'cost_plus_amount' — monthly = landed + markupValue
     *   - 'fixed'            — monthly = sellPriceLocalMonthlyOrNull (required)
     *
     * @param float       $landedMonthly                Landed cost per month, local currency.
     * @param string      $markupStrategy               One of 'cost_plus_pct'|'cost_plus_amount'|'fixed'.
     * @param float       $markupValue                  Percent or fixed amount depending on strategy.
     * @param float|null  $sellPriceLocalMonthlyOrNull  Required iff strategy = 'fixed'.
     * @param int         $cycleMonths                  From CycleNormalizer (1/3/6/12/24/36).
     * @return float Sell price for the cycle, rounded to 2 decimals.
     * @throws \InvalidArgumentException for unknown strategy or missing fixed price.
     */
    public static function sellPriceForCycle(
        float $landedMonthly,
        string $markupStrategy,
        float $markupValue,
        ?float $sellPriceLocalMonthlyOrNull,
        int $cycleMonths
    ): float {
        if ($cycleMonths <= 0) {
            throw new \InvalidArgumentException('MarginCalculator: cycleMonths must be > 0');
        }

        if ($markupStrategy === 'cost_plus_pct') {
            $monthly = $landedMonthly * (1.0 + ($markupValue / 100.0));
        } elseif ($markupStrategy === 'cost_plus_amount') {
            $monthly = $landedMonthly + $markupValue;
        } elseif ($markupStrategy === 'fixed') {
            if ($sellPriceLocalMonthlyOrNull === null) {
                throw new \InvalidArgumentException(
                    'MarginCalculator: fixed markup requires sellPriceLocalMonthlyOrNull'
                );
            }
            $monthly = $sellPriceLocalMonthlyOrNull;
        } else {
            throw new \InvalidArgumentException(
                'MarginCalculator: unknown markup strategy "' . $markupStrategy . '"'
            );
        }

        return round($monthly * $cycleMonths, 2);
    }

    /**
     * Net revenue for a cycle, in local currency. Strips output tax ONLY if
     * the active tax mode bundles output tax inside the gross price (i.e.
     * "tax-inclusive" pricing). For exclusive pricing the gross IS the net
     * revenue — output tax was never bundled in.
     *
     *   inclusive: net = gross / (1 + outputTaxRatePct/100)
     *   exclusive: net = gross
     *
     * @param float $grossForCycle           Customer-facing gross for the cycle.
     * @param bool  $pricesIncludeOutputTax  From TaxModeEngine::summary($mode).
     * @param float $outputTaxRatePct        Output tax rate (percent, e.g. 18.00).
     * @return float Net revenue for the cycle.
     */
    public static function netRevenueForCycle(
        float $grossForCycle,
        bool $pricesIncludeOutputTax,
        float $outputTaxRatePct
    ): float {
        if (!$pricesIncludeOutputTax || $outputTaxRatePct <= 0.0) {
            return $grossForCycle;
        }

        $rate = $outputTaxRatePct / 100.0;
        return $grossForCycle / (1.0 + $rate);
    }

    /**
     * Current margin RATIO (NOT percent) for an in-flight service. Returns
     * null if revenue is non-positive (free service / one-time / corrupt row)
     * — the caller decides how to skip.
     *
     *   ratio = (netRevenueForCycle − landedForCycle) / netRevenueForCycle
     *
     * @param float $currentRecurring     `tblhosting.recurringamount` (gross, for the cycle).
     * @param float $netRevenueForCycle   Pre-computed via netRevenueForCycle().
     * @param float $landedForCycle       Pre-computed via landedCostForCycle().
     * @return float|null Margin ratio, or null if netRevenueForCycle <= 0.
     */
    public static function currentMarginRatio(
        float $currentRecurring,
        float $netRevenueForCycle,
        float $landedForCycle
    ): ?float {
        // $currentRecurring is included in the signature for symmetry with the
        // plan's algorithm; the actual ratio uses netRevenueForCycle which the
        // caller derived from currentRecurring + tax mode.
        unset($currentRecurring);

        if ($netRevenueForCycle <= 0.0) {
            return null;
        }

        return ($netRevenueForCycle - $landedForCycle) / $netRevenueForCycle;
    }

    /**
     * Minimum monthly sell price required to keep the margin AT the floor.
     *
     *   floorRatio   = floorPct / 100
     *   minimumSell  = landedMonthly / (1 - floorRatio)
     *
     * Example: landedMonthly = 850, floorPct = 15
     *          floorRatio    = 0.15
     *          minimumSell   = 850 / 0.85 = 1000.00 (exact)
     *
     * Throws when floorRatio >= 1 (math would explode / go negative — caller
     * must reject the policy upstream).
     *
     * @param float $landedMonthly Landed cost per month, local currency.
     * @param float $floorPct      Margin floor as PERCENT (e.g. 15.00, not 0.15).
     * @return float Minimum monthly sell price to hit the floor exactly.
     * @throws \InvalidArgumentException when floorPct >= 100 (ratio >= 1).
     */
    public static function minimumSellMonthlyForFloor(
        float $landedMonthly,
        float $floorPct
    ): float {
        $floorRatio = $floorPct / 100.0;
        if ($floorRatio >= 1.0) {
            throw new \InvalidArgumentException(
                'MarginCalculator: floorPct must be < 100 (got ' . $floorPct . ')'
            );
        }

        return $landedMonthly / (1.0 - $floorRatio);
    }
}
