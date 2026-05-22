<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CyclePricingMap;
use ContaboPricing\CycleSet;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.5 cycle bitmask + tblpricing column mapping tests.
 *
 * Tests 1–17 of the Phase A.5 brief (Agent A slice). Tests 18–19 covering the
 * `migrateTo3()` idempotency contract live in MigrationV3Test.
 *
 * Bit assignments are load-bearing for the v3 schema migration; if a test in
 * this file fails, the bitmask backfill is wrong and the migration will
 * produce inconsistent data.
 */
final class CycleSetTest extends TestCase
{
    // ── CycleSet ────────────────────────────────────────────────────────────

    public function testFromMaskContainsQuarterlyAndAnnually(): void
    {
        // 0b001010 = bit 1 (Quarterly) | bit 3 (Annually) = 10
        $set = CycleSet::fromMask(0b001010);
        $this->assertTrue($set->contains('Quarterly'));
        $this->assertTrue($set->contains('Annually'));
        $this->assertFalse($set->contains('Monthly'));
        $this->assertFalse($set->contains('Semi-Annually'));
        $this->assertFalse($set->contains('Biennially'));
        $this->assertFalse($set->contains('Triennially'));
        $this->assertSame(['Quarterly', 'Annually'], $set->enabledCycles());
    }

    public function testFromLegacyBooleansAllThreeIsThirteen(): void
    {
        // monthly | semiannually | annually = bit 0 | bit 2 | bit 3
        //                                   = 1     | 4     | 8
        //                                   = 13 = 0b001101
        $mask = CycleSet::fromLegacyBooleans(true, true, true);
        $this->assertSame(0b001101, $mask);
        $this->assertSame(13, $mask);
    }

    public function testFromLegacyBooleansAllFalseIsZero(): void
    {
        $this->assertSame(0, CycleSet::fromLegacyBooleans(false, false, false));
    }

    public function testBitForCycleFreeAccountIsNull(): void
    {
        // Free Account / One Time are non-recurring; they have no bit
        // assignment and must round-trip as null so the migration backfill
        // never tries to set a non-existent bit.
        $this->assertNull(CycleSet::bitForCycle('Free Account'));
    }

    public function testBitForCycleOneTimeIsNull(): void
    {
        $this->assertNull(CycleSet::bitForCycle('One Time'));
    }

    public function testBitForCycleAnnuallyIsThree(): void
    {
        $this->assertSame(3, CycleSet::bitForCycle('Annually'));
        $this->assertSame(CycleSet::BIT_ANNUALLY, CycleSet::bitForCycle('Annually'));
    }

    // ── CyclePricingMap — recurring columns ────────────────────────────────

    public function testRecurringColumnMonthly(): void
    {
        $this->assertSame('monthly', CyclePricingMap::getRecurringColumn('Monthly'));
    }

    public function testRecurringColumnQuarterly(): void
    {
        $this->assertSame('quarterly', CyclePricingMap::getRecurringColumn('Quarterly'));
    }

    public function testRecurringColumnSemiAnnually(): void
    {
        $this->assertSame('semiannually', CyclePricingMap::getRecurringColumn('Semi-Annually'));
    }

    public function testRecurringColumnAnnually(): void
    {
        $this->assertSame('annually', CyclePricingMap::getRecurringColumn('Annually'));
    }

    public function testRecurringColumnBiennially(): void
    {
        $this->assertSame('biennially', CyclePricingMap::getRecurringColumn('Biennially'));
    }

    public function testRecurringColumnTriennially(): void
    {
        $this->assertSame('triennially', CyclePricingMap::getRecurringColumn('Triennially'));
    }

    // ── CyclePricingMap — setup-fee columns ────────────────────────────────

    public function testSetupFeeColumnsAllSixCycles(): void
    {
        // One asserter rather than six identical-shape methods; the brief
        // (#13) called for "all 6 cycles" and a single test is the cleanest
        // way to express that.
        $this->assertSame('msetupfee', CyclePricingMap::getSetupFeeColumn('Monthly'));
        $this->assertSame('qsetupfee', CyclePricingMap::getSetupFeeColumn('Quarterly'));
        $this->assertSame('ssetupfee', CyclePricingMap::getSetupFeeColumn('Semi-Annually'));
        $this->assertSame('asetupfee', CyclePricingMap::getSetupFeeColumn('Annually'));
        $this->assertSame('bsetupfee', CyclePricingMap::getSetupFeeColumn('Biennially'));
        $this->assertSame('tsetupfee', CyclePricingMap::getSetupFeeColumn('Triennially'));
    }

    // ── CyclePricingMap — price-status classifier ──────────────────────────

    public function testPriceStatusNegativeIsDisabled(): void
    {
        $this->assertSame('disabled', CyclePricingMap::priceStatusFromValue(-1.00));
    }

    public function testPriceStatusZeroIsFree(): void
    {
        $this->assertSame('free', CyclePricingMap::priceStatusFromValue(0.00));
    }

    public function testPriceStatusPositiveIsPriced(): void
    {
        $this->assertSame('priced', CyclePricingMap::priceStatusFromValue(99.50));
    }

    public function testPriceStatusNullIsAbsent(): void
    {
        $this->assertSame('absent', CyclePricingMap::priceStatusFromValue(null));
    }

    // ── CycleSet — corroborating helpers (not numbered in brief, but
    //    these protect the public API contract used by Agents B/C/D) ───────

    public function testAllCyclesDisplayOrder(): void
    {
        $this->assertSame(
            ['Monthly', 'Quarterly', 'Semi-Annually', 'Annually', 'Biennially', 'Triennially'],
            CycleSet::allCycles()
        );
    }

    public function testFromCyclesIgnoresUnknown(): void
    {
        $set = CycleSet::fromCycles(['Monthly', 'Free Account', 'Triennially', 'NotACycle']);
        $this->assertSame(['Monthly', 'Triennially'], $set->enabledCycles());
        $this->assertSame(0b100001, $set->toMask());
    }

    public function testMaskClampsToValidRange(): void
    {
        // Anything above bit 5 is silently clamped — the migration
        // validation gate is what's supposed to reject bad data, not the
        // helper class.
        $set = CycleSet::fromMask(0b11111111);
        $this->assertSame(CycleSet::MASK_MAX, $set->toMask());
    }
}
