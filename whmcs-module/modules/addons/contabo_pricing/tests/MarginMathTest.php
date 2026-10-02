<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\MarginCalculator;
use PHPUnit\Framework\TestCase;

/**
 * Pure-math tests for MarginCalculator. No DB. No WHMCS shims required.
 *
 * These are the gate tests for the renewal engine's margin discipline:
 *
 *   - floor 15.00 must convert to ratio 0.15 EXACTLY once, inside the helper.
 *   - minimum-sell math is `landed / (1 - ratio)`; with landed ₹850 + floor 15
 *     the answer is ₹1000.00 exact.
 *   - landed cost in `unregistered_no_output_tax` mode is 850 + 18% + 20 = 1023.
 *   - landed cost in a recoverable mode skips vendor tax → 850 + 20 = 870.
 *   - net revenue strips output tax ONLY when prices are gross-inclusive.
 *   - annual-cycle margin: landed ₹12,276 + revenue ₹14,400 → ratio 0.1475.
 *   - floorPct >= 100 throws.
 */
final class MarginMathTest extends TestCase
{
    public function testFloorRatioConversion(): void
    {
        // The conversion happens inside MarginCalculator. We verify the
        // observable consequence: with floor=15 and landed=850, the answer
        // is 1000 — which is ONLY true if 15 was treated as a percent
        // (ratio = 0.15) inside the helper.
        $min = MarginCalculator::minimumSellMonthlyForFloor(850.0, 15.00);
        $this->assertEqualsWithDelta(1000.00, $min, 0.0001);
    }

    public function testMinimumSellForFloorExact(): void
    {
        $min = MarginCalculator::minimumSellMonthlyForFloor(850.0, 15.00);
        $this->assertSame(1000.00, round($min, 2));
    }

    public function testLandedCostUnregisteredModeAddsVendorTax(): void
    {
        // Mode: unregistered_no_output_tax. Vendor tax (18%) is NON-recoverable
        // and lives inside landed cost. Inputs chosen so:
        //   monthly_after_fx = eurMonthly × fxRate = 850
        //   18% vendor tax   = 153
        //   buffers (fx 2% + payment-buffer-as-amount-via-2.353%) = 20
        //   landed_monthly   = 850 + 153 + 20 = 1023
        //
        // Plan's verified example uses ₹20 combined fx + payment buffer on
        // monthly_after_fx of 850. 20 / 850 ≈ 2.3529% — split however you
        // like as long as the sum is 20.
        $landed = MarginCalculator::landedCostMonthly(
            /* eurMonthly */            850.0,
            /* fxRate     */              1.0,    // 1:1 — keeps the example readable
            /* fxBufferPct */              1.0,   // 1% × 850 =  8.50
            /* paymentBufferPct */         (20.0 - 8.5) / 850.0 * 100.0, // remainder of 20
            /* vendorTaxRatePct */        18.0,
            /* vendorTaxRecoverable */    false
        );
        $this->assertEqualsWithDelta(1023.00, $landed, 0.0001);
    }

    public function testLandedCostRegisteredRecoverableExcludesVendorTax(): void
    {
        // Mode: registered_tax_exclusive_recoverable. Vendor tax IS
        // recoverable as ITC, so it does NOT belong in landed.
        //   monthly_after_fx = 850
        //   vendor tax       = 153 BUT excluded
        //   buffers          = 20
        //   landed_monthly   = 850 + 0 + 20 = 870
        $landed = MarginCalculator::landedCostMonthly(
            850.0,
            1.0,
            1.0,
            (20.0 - 8.5) / 850.0 * 100.0,
            18.0,
            /* vendorTaxRecoverable */ true
        );
        $this->assertEqualsWithDelta(870.00, $landed, 0.0001);
    }

    public function testNetRevenueRemovesOutputTaxWhenInclusive(): void
    {
        // Gross ₹1180 with 18% inclusive output tax → ₹1000 net revenue.
        $net = MarginCalculator::netRevenueForCycle(
            /* grossForCycle */ 1180.0,
            /* pricesIncludeOutputTax */ true,
            /* outputTaxRatePct */ 18.0
        );
        $this->assertEqualsWithDelta(1000.00, $net, 0.0001);
    }

    public function testNetRevenueIsGrossWhenExclusive(): void
    {
        $net = MarginCalculator::netRevenueForCycle(
            1180.0,
            /* pricesIncludeOutputTax */ false,
            18.0
        );
        $this->assertSame(1180.0, $net);

        // Also: even if includeOutputTax=true, when rate=0 the gross IS net.
        $net2 = MarginCalculator::netRevenueForCycle(1180.0, true, 0.0);
        $this->assertSame(1180.0, $net2);
    }

    public function testMarginRatioAnnualCycle(): void
    {
        // Plan acceptance criterion #6 + worked example in deliverable 4.
        //   landed for 12 months = ₹12,276
        //   gross revenue (Annually) = ₹14,400
        //   prices NOT inclusive of output tax → net revenue = ₹14,400
        //   margin amount = 14,400 − 12,276 = ₹2,124
        //   margin ratio  = 2,124 / 14,400 = 0.1475 exact
        $landedForCycle = 12276.0;
        $gross          = 14400.0;
        $netRev         = MarginCalculator::netRevenueForCycle($gross, false, 0.0);
        $ratio          = MarginCalculator::currentMarginRatio($gross, $netRev, $landedForCycle);

        $this->assertNotNull($ratio);
        $this->assertEqualsWithDelta(0.1475, (float) $ratio, 1e-9);
    }

    public function testFloorRatioGTE1Throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        MarginCalculator::minimumSellMonthlyForFloor(850.0, 100.0);
    }

    // ── Phase B: landedCostWithSelections ─────────────────────────────────────

    public function testLandedCostWithSelectionsSumsBaseAndOptions(): void
    {
        // base 10 EUR + Auto Backup 1.5 + extra storage 2.0 = 13.5 EUR; fx 90, no buffers/tax.
        $whole = MarginCalculator::landedCostWithSelections(
            10.0,
            [['eur_monthly' => 1.5], ['eur_monthly' => 2.0]],
            90.0, 0.0, 0.0, 0.0, false
        );
        $this->assertEqualsWithDelta(1215.0, $whole, 0.0001); // 13.5 × 90
        // Linearity: equals base landed + each option's landed.
        $base = MarginCalculator::landedCostMonthly(10.0, 90.0, 0.0, 0.0, 0.0, false);
        $o1   = MarginCalculator::landedCostMonthly(1.5, 90.0, 0.0, 0.0, 0.0, false);
        $o2   = MarginCalculator::landedCostMonthly(2.0, 90.0, 0.0, 0.0, 0.0, false);
        $this->assertEqualsWithDelta($base + $o1 + $o2, $whole, 0.0001);
    }

    public function testLandedCostWithSelectionsClampsNegativeDelta(): void
    {
        // A cheaper-than-default option (negative EUR) must NOT reduce landed cost.
        $whole = MarginCalculator::landedCostWithSelections(
            10.0, [['eur_monthly' => -5.0]], 90.0, 0.0, 0.0, 0.0, false
        );
        $this->assertEqualsWithDelta(900.0, $whole, 0.0001); // base 10 × 90, option clamped to 0
    }

    public function testLandedCostWithSelectionsMultipliesQuantity(): void
    {
        // IPv4 1.0 EUR/unit × qty 3 = 3.0 EUR; + base 10 = 13 EUR.
        $whole = MarginCalculator::landedCostWithSelections(
            10.0, [['eur_monthly' => 1.0, 'qty' => 3]], 90.0, 0.0, 0.0, 0.0, false
        );
        $this->assertEqualsWithDelta(1170.0, $whole, 0.0001); // 13 × 90
    }

    public function testLandedCostWithSelectionsLinearUnderBuffersAndTax(): void
    {
        // With FX buffer + non-recoverable vendor tax, the whole equals the
        // single-call landed cost of the summed EUR (proves linearity holds).
        $sel = [['eur_monthly' => 1.5], ['eur_monthly' => 0.5, 'qty' => 2]];
        $whole = MarginCalculator::landedCostWithSelections(10.0, $sel, 90.0, 2.0, 2.0, 18.0, false);
        $sumEur = 10.0 + 1.5 + (0.5 * 2);
        $single = MarginCalculator::landedCostMonthly($sumEur, 90.0, 2.0, 2.0, 18.0, false);
        $this->assertEqualsWithDelta($single, $whole, 0.0001);
    }

    public function testLandedCostWithSelectionsNoSelectionsEqualsBase(): void
    {
        $whole = MarginCalculator::landedCostWithSelections(10.0, [], 90.0, 2.0, 2.0, 18.0, false);
        $base  = MarginCalculator::landedCostMonthly(10.0, 90.0, 2.0, 2.0, 18.0, false);
        $this->assertEqualsWithDelta($base, $whole, 0.0001);
    }

    // ── H1 (addon side): corrected margin_engine.json vectors ────────────────
    //
    // Source: docs/test-vectors/margin_engine.json (securiace-vps-platform).
    // Addon mapping: term total = sellPriceForCycle(monthly cost, cost_plus_pct,
    // markup, null, months), which multiplies by months EXACTLY ONCE and rounds
    // ONCE. A "full"-mode period discount d is equivalent to scaling the cost by
    // (1 - d/100) before markup. The vectors' `period_discount_mode markup_only`
    // cases (gst_markup_only_annually, kwarg_override_full_to_markup_only_annually)
    // are SKIPPED: the addon has no such mode.

    /** @return array<string,array{0:string,1:float,2:float,3:float,4:float,5:int,6:float}> name, cost, tax%, discount%, markup%, months, term_total */
    public static function correctedTermVectors(): array
    {
        return [
            'annual_stacks_after_markup'  => ['annual_stacks_after_markup', 4.50, 0.0, 10.0, 40.0, 12, 68.04],
            'semiannually'                => ['semiannually', 4.50, 0.0, 5.0, 40.0, 6, 35.91],
            'zero_pct_quarterly'          => ['zero_pct_quarterly', 4.50, 0.0, 0.0, 40.0, 3, 18.90],
            'gst_full_annually'           => ['gst_full_annually', 4.50, 18.0, 10.0, 40.0, 12, 80.29],
            'biennially_xl_relaxed_floor' => ['biennially_xl_relaxed_floor', 50.00, 0.0, 15.0, 25.0, 24, 1275.00],
        ];
    }

    /**
     * @dataProvider correctedTermVectors
     */
    public function testCorrectedMarginEngineTermTotals(
        string $name,
        float $cost,
        float $upstreamTaxPct,
        float $discountPct,
        float $markupPct,
        int $months,
        float $expectedTermTotal
    ): void {
        $landed = MarginCalculator::landedCostMonthly($cost, 1.0, 0.0, 0.0, $upstreamTaxPct, false);
        $discounted = $landed * (1.0 - $discountPct / 100.0);
        $term = MarginCalculator::sellPriceForCycle($discounted, 'cost_plus_pct', $markupPct, null, $months);
        $this->assertEqualsWithDelta($expectedTermTotal, $term, 0.0001, $name);
    }

    public function testMonthsMultiplyExactlyOnceAndRoundOnce(): void
    {
        $monthly = MarginCalculator::landedCostMonthly(1.234, 1.0, 0.0, 0.0, 0.0, true);
        // landed cost for a cycle = monthly x months, once.
        $this->assertEqualsWithDelta($monthly * 12, MarginCalculator::landedCostForCycle(1.234, 1.0, 0.0, 0.0, 0.0, true, 12), 1e-12);
        // round-once: 1.234 x 1.4 x 12 = 20.7312 -> 20.73, NOT 12 x round(1.7276, 2) = 20.76.
        $once = MarginCalculator::sellPriceForCycle($monthly, 'cost_plus_pct', 40.0, null, 12);
        $this->assertSame(20.73, $once);
        $this->assertNotSame(round(round($monthly * 1.4, 2) * 12, 2), $once);
    }

    public function testUnknownCycleIsRejectedNotZero(): void
    {
        $this->assertNull(\ContaboPricing\CycleNormalizer::monthsForCycle('never_heard_of_it'));
        $this->assertSame(12, \ContaboPricing\CycleNormalizer::requireMonths('Annually'));
        $this->expectException(\InvalidArgumentException::class);
        \ContaboPricing\CycleNormalizer::requireMonths('never_heard_of_it');
    }

    public function testNonPositiveMonthsRejectedByMargin(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        MarginCalculator::sellPriceForCycle(10.0, 'cost_plus_pct', 40.0, null, 0);
    }
}
