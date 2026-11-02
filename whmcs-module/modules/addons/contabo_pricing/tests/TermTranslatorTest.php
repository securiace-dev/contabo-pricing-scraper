<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProfileVersionInput;
use ContaboPricing\SyncEngine;
use ContaboPricing\TermTranslator as T;
use PHPUnit\Framework\TestCase;

/**
 * Worked examples for the term-translation engine. Figures are ILLUSTRATIVE
 * (a Cloud VPS-like plan: 1 mo list EUR 4.50, 12 mo EUR 3.60, setup EUR 3.00 on
 * the 1-mo term only), not live Contabo prices.
 */
final class TermTranslatorTest extends TestCase
{
    /** @return array<string,mixed> */
    private function plan(): array
    {
        return [
            'base_monthly_price' => 4.50,
            'periods' => [
                ['months' => 1,  'effective_monthly' => 4.50, 'setup_fee' => 3.00],
                ['months' => 12, 'effective_monthly' => 3.60, 'setup_fee' => 0.00],
            ],
        ];
    }

    private function published(): array
    {
        return T::publishedTerms($this->plan());
    }

    // ── Base price only ────────────────────────────────────────────────────

    public function testNativeOneMonth(): void
    {
        $r = T::resolve($this->published(), 1);
        $this->assertSame(T::SOURCE_NATIVE, $r['source']);
        $this->assertFalse($r['derived']);
        $this->assertSame(4.50, $r['monthly_eur']);
        $this->assertSame(3.00, $r['setup_eur']);
        $this->assertSame(7.50, $r['total_eur']); // 4.50 x 1 + 3.00 setup, once
    }

    public function testNativeTwelveMonths(): void
    {
        $r = T::resolve($this->published(), 12);
        $this->assertFalse($r['derived']);
        $this->assertSame(3.60, $r['monthly_eur']);
        $this->assertSame(43.20, $r['total_eur']);
    }

    public function testThirtySixMonthsNearestTermUsesTwelveMonthRate(): void
    {
        $r = T::resolve($this->published(), 36, T::POLICY_NEAREST_TERM);
        $this->assertSame(T::SOURCE_DERIVED_NEAREST, $r['source']);
        $this->assertTrue($r['derived']);
        $this->assertSame(12, $r['source_months']);
        $this->assertSame(3.60, $r['monthly_eur']);
        $this->assertSame(129.60, $r['total_eur']); // 3.60 x 36
        $this->assertSame(0.0, $r['setup_eur']);     // setup of the SOURCE term (12 mo = 0), not x36
    }

    public function testThirtySixMonthsProportionalUsesListMonthly(): void
    {
        $r = T::resolve($this->published(), 36, T::POLICY_PROPORTIONAL);
        $this->assertSame(T::SOURCE_DERIVED_PROPORTIONAL, $r['source']);
        $this->assertSame(4.50, $r['monthly_eur']);
        $this->assertSame(162.00 + 3.00, $r['total_eur']); // 4.50 x 36 + one-time 3.00 from the 1-mo term
    }

    public function testTwentyFourMonths(): void
    {
        $this->assertSame(86.40, T::resolve($this->published(), 24, T::POLICY_NEAREST_TERM)['total_eur']);
        $this->assertSame(108.00 + 3.00, T::resolve($this->published(), 24, T::POLICY_PROPORTIONAL)['total_eur']);
    }

    public function testConfiguredExtraDiscountAppliesOnlyToDerivedTerms(): void
    {
        $adj = [36 => 5.0, 12 => 50.0];
        $r36 = T::resolve($this->published(), 36, T::POLICY_NEAREST_TERM, 4.50, $adj);
        $this->assertSame(3.42, $r36['monthly_eur']);      // 3.60 x 0.95
        $this->assertSame(123.12, $r36['total_eur']);      // 3.42 x 36
        $this->assertSame(5.0, $r36['adjust_pct']);
        // Native term is never adjusted, even if a pct is configured for it.
        $this->assertSame(3.60, T::resolve($this->published(), 12, T::POLICY_NEAREST_TERM, 4.50, $adj)['monthly_eur']);
    }

    public function testAdjustmentIsClampedTo50Percent(): void
    {
        $r = T::resolve($this->published(), 36, T::POLICY_NEAREST_TERM, null, [36 => 99.0]);
        $this->assertSame(1.80, $r['monthly_eur']);
    }

    public function testArbitraryTermBetweenPublishedTermsNeverGetsDeeperDiscount(): void
    {
        // 18 mo: nearest published AT OR BELOW is 12 (not a deeper tier).
        $r = T::resolve($this->published(), 18);
        $this->assertSame(12, $r['source_months']);
        $this->assertSame(3.60, $r['monthly_eur']);
        // 5 mo: nothing published <=5 except 1 -> list rate 4.50, not the 12-mo discount.
        $this->assertSame(4.50, T::resolve($this->published(), 5)['monthly_eur']);
    }

    public function testTermShorterThanEveryPublishedTermUsesListPriceNotLongTermDiscount(): void
    {
        $only12 = T::publishedTerms(['base_monthly_price' => 4.50, 'periods' => [
            ['months' => 12, 'effective_monthly' => 3.60, 'setup_fee' => 0.0],
        ]]);
        $r = T::resolve($only12, 2, T::POLICY_NEAREST_TERM, 4.50);
        $this->assertSame(T::SOURCE_DERIVED_PROPORTIONAL, $r['source']);
        $this->assertSame(4.50, $r['monthly_eur']);
        // Without any list price the shortest published term is the last resort.
        $this->assertSame(3.60, T::resolve($only12, 2, T::POLICY_NEAREST_TERM, null)['monthly_eur']);
    }

    public function testProportionalFallsBackToNearestWhenNoListPrice(): void
    {
        $only12 = T::publishedTerms(['periods' => [['months' => 12, 'effective_monthly' => 3.60]]]);
        $r = T::resolve($only12, 36, T::POLICY_PROPORTIONAL, null);
        $this->assertSame(T::SOURCE_DERIVED_NEAREST, $r['source']);
        $this->assertSame(3.60, $r['monthly_eur']);
    }

    public function testNothingPublishedReturnsNullAndNeverThrows(): void
    {
        $this->assertNull(T::resolve([], 36));
        $this->assertNull(T::resolve($this->published(), 0));
        $many = T::resolveMany([], [1, 12, 36]);
        $this->assertSame([], $many['terms']);
        $this->assertSame([1, 12, 36], $many['missing']);
    }

    public function testUnknownPolicyNormalisesToNearestTerm(): void
    {
        $this->assertSame(T::POLICY_NEAREST_TERM, T::normalizePolicy('bogus'));
        $this->assertSame(T::POLICY_PROPORTIONAL, T::normalizePolicy(' Proportional '));
    }

    // ── Add-ons ────────────────────────────────────────────────────────────

    /** Base + two monthly-only add-ons (storage 2.40, backup 1.20), nearest-term base policy. */
    public function testBasePlusAddonsMonthlyOnlyAddonsScaleProportionally(): void
    {
        $storage = 2.40;
        $backup = 1.20;
        $expect = [ // months => [base monthly, monthly total, term total]
            1  => [4.50, 8.10, 8.10],
            12 => [3.60, 7.20, 86.40],
            24 => [3.60, 7.20, 172.80],
            36 => [3.60, 7.20, 259.20],
        ];
        foreach ($expect as $m => $e) {
            $base = T::resolve($this->published(), $m, T::POLICY_NEAREST_TERM);
            $a1 = T::resolveAddon($storage, $m);
            $a2 = T::resolveAddon($backup, $m);
            $monthly = $base['monthly_eur'] + $a1['monthly_eur'] + $a2['monthly_eur'];
            $this->assertEqualsWithDelta($e[1], $monthly, 0.00001, "monthly @$m");
            $this->assertEqualsWithDelta($e[2], T::roundHalfUp($monthly * $m), 0.00001, "total @$m");
            $this->assertSame($m !== 1, $a1['derived'], "add-on derived flag @$m");
        }
    }

    public function testProportionalPolicyBasePlusAddons(): void
    {
        $base = T::resolve($this->published(), 36, T::POLICY_PROPORTIONAL);
        $monthly = $base['monthly_eur'] + 2.40 + 1.20;
        $this->assertSame(8.10, round($monthly, 2));
        $this->assertSame(291.60, T::roundHalfUp($monthly * 36));
        $monthly24 = T::resolve($this->published(), 24, T::POLICY_PROPORTIONAL)['monthly_eur'] + 3.60;
        $this->assertSame(194.40, T::roundHalfUp($monthly24 * 24));
    }

    public function testAddonWithNativeTermRateUsesItThenDerivesFromNearest(): void
    {
        $terms = [1 => 2.40, 12 => 2.00];
        $a12 = T::resolveAddon(2.40, 12, $terms, T::POLICY_NEAREST_TERM);
        $this->assertSame(T::SOURCE_NATIVE, $a12['source']);
        $this->assertSame(2.00, $a12['monthly_eur']);
        // 12-mo cart: base 3.60 + storage 2.00 (native) + backup 1.20 (monthly x12) = 6.80/mo = 81.60
        $monthly = 3.60 + $a12['monthly_eur'] + T::resolveAddon(1.20, 12)['monthly_eur'];
        $this->assertSame(81.60, T::roundHalfUp($monthly * 12));
        // 36 mo, nearest-term add-on policy -> 12-mo add-on rate; proportional -> monthly.
        $this->assertSame(2.00, T::resolveAddon(2.40, 36, $terms, T::POLICY_NEAREST_TERM)['monthly_eur']);
        $this->assertSame(2.40, T::resolveAddon(2.40, 36, $terms, T::POLICY_PROPORTIONAL)['monthly_eur']);
    }

    public function testNegativeAddonIsClampedToZero(): void
    {
        $this->assertSame(0.0, T::resolveAddon(-5.0, 12)['monthly_eur']);
    }

    // ── Setup / one-time ───────────────────────────────────────────────────

    public function testOneTimeChargesAreNeverMultipliedByTerm(): void
    {
        $this->assertSame(3.00, T::oneTime(3.00));
        $this->assertSame(0.0, T::oneTime(-1.0));
        $r = T::resolve($this->published(), 1);
        $this->assertSame(4.50 * 1 + 3.00, $r['total_eur']);
        // 36-mo nearest-term: setup comes from the 12-mo source term (0), still not x36
        $this->assertSame(0.0, T::resolve($this->published(), 36)['setup_eur']);
    }

    // ── Rounding, currency, tax ────────────────────────────────────────────

    public function testRoundHalfUpTwoDecimals(): void
    {
        $this->assertSame(1.01, T::roundHalfUp(1.005 + 0.0000001));
        $this->assertSame(2.35, T::roundHalfUp(2.345));
        $this->assertSame(-2.35, T::roundHalfUp(-2.345)); // away from zero
    }

    public function testInrConversionUsesExistingPipelineAndAddsNoGst(): void
    {
        // EUR 3.60/mo derived for 36 mo; FX 90.00 + 3.5% markup = 93.15; GST OFF.
        $inr = ProfileVersionInput::toLocalMonthly(3.60, 90.0, 3.5, 0.0, 'INR');
        $this->assertSame(335.34, $inr);
        $this->assertSame(12072.24, round($inr * 36, 2));
        // What must NOT stay on: GST 18% folded into cost basis.
        $this->assertSame(395.70, ProfileVersionInput::toLocalMonthly(3.60, 90.0, 3.5, 0.18, 'INR'));
    }

    // ── Vector + provenance (SyncEngine wiring) ────────────────────────────

    public function testDefaultVectorIsUnchangedFromLegacyBehaviour(): void
    {
        $v = SyncEngine::periodPriceVectorFromPlan($this->plan());
        $this->assertSame([1 => 4.50, 3 => 4.50, 6 => 4.50, 12 => 3.60, 24 => 3.60, 36 => 3.60], $v);
    }

    public function testProportionalVectorAndProvenance(): void
    {
        $v = SyncEngine::periodPriceVectorFromPlan($this->plan(), T::POLICY_PROPORTIONAL);
        $this->assertSame(4.50, $v[24]);
        $this->assertSame(4.50, $v[36]);
        $this->assertSame(3.60, $v[12]);
        $prov = SyncEngine::derivedCycleProvenance($this->plan(), T::POLICY_NEAREST_TERM, [36 => 5.0]);
        $this->assertSame([3, 6, 24, 36], array_keys($prov)); // 1 and 12 are native
        $this->assertSame('derived_nearest_term', $prov[36]['source']);
        $this->assertSame(12, $prov[36]['source_months']);
        $this->assertSame(3.42, $prov[36]['monthly_eur']);
        $this->assertStringContainsString('36', $prov[36]['formula']);
    }

    public function testHeadlineFromSnapshotAndClientSafeNote(): void
    {
        $snap = ['_term_provenance' => [
            ['months' => 36, 'derived' => true, 'source_months' => 12, 'formula' => 'monthly(36-mo) = monthly(12-mo)'],
            ['months' => 12, 'derived' => false, 'source_months' => 12, 'formula' => 'native'],
        ]];
        $hit = T::headlineFromSnapshot($snap, 36);
        $this->assertNotNull($hit);
        $this->assertSame(12, $hit['source_months']);
        $this->assertNull(T::headlineFromSnapshot($snap, 12));
        $this->assertNull(T::headlineFromSnapshot('{"_term_provenance":[]}', 36));
        $note = T::clientSafeNote(['source_months' => 12]);
        $this->assertStringContainsString('12-month published term', $note);
        $this->assertStringNotContainsString('Contabo', $note);
        $this->assertStringContainsString('Contabo', T::operatorNote(['source_months' => 12, 'formula' => 'x']));
    }
}
