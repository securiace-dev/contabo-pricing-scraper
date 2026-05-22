<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CycleSet;
use ContaboPricing\PolicyResolver;
use ContaboPricing\RenewalEngine;
use ContaboPricing\Rounding;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.5 cycle-gate + per-cycle markup + rounding tests for RenewalEngine.
 *
 * Plan reference: amended Phase A.5 brief sections 6, 10, 11, 12.
 *
 * Test numbering follows the brief:
 *   25. testServiceMonthly_evaluatedWhenMonthlyInRenewalMask
 *   26. testServiceQuarterly_skippedWithCycleNotMappedWhenAbsentFromMask
 *   27. testServiceBiennially_evaluatedWhenInMask
 *   28. testServiceFreeAccount_skippedWithCycleUnsupported
 *   29. testServiceOneTime_skippedWithCycleUnsupported
 *   30. testPerCycleMarkupOverrideUsedWhenPresent
 *   31. testRoundingModeAppliedAndAuditedInMetadata
 */
final class RenewalEngineCycleTest extends TestCase
{
    /** PolicyResolver stub returning a plain current_term row — no DB. */
    private function stubResolver(): PolicyResolver
    {
        return new class extends PolicyResolver {
            protected function fetchServicePolicy(int $serviceId): ?array
            {
                return null; // forces use of profile.default_policy
            }
        };
    }

    /** Build a baseline service row that should reach the cycle-gate. */
    private function baseService(string $cycle, int $renewalMask, int $catalogMask = 0, ?string $overridesJson = null): array
    {
        return [
            'id'              => 4242,
            'status'          => 'Active',
            'billingcycle'    => $cycle,
            'recurringamount' => 1200.00,
            'subscriptionid'  => '',
            'nextduedate'     => '2099-12-31',  // far future → outside notice window
            'profile'         => [
                'id'                            => 7,
                'default_policy'                => 'current_term',
                'margin_floor_pct'              => 15.00,
                'fx_buffer_pct'                 => 2.00,
                'notice_days_default'           => 30,
                'large_increase_threshold_pct'  => 10.00,
                'max_increase_pct'              => 25.00,
                'allow_auto_decrease'           => false,
            ],
            'profile_version' => [
                'id'                       => 71,
                'base_monthly_eur'         => 10.0,
                'markup_strategy'          => 'cost_plus_pct',
                'markup_value'             => 50.0,
                'sell_price_local_monthly' => null,
            ],
            'mapping'         => [
                'renewal_cycles_mask'    => $renewalMask,
                'catalog_cycles_mask'    => $catalogMask,
                'markup_overrides_json'  => $overridesJson,
                'rounding_mode'          => Rounding::MODE_EXACT_2_DECIMALS,
            ],
        ];
    }

    private function settings(): array
    {
        return [
            'repricing_phase'              => 'observe',
            'tax_registration_mode'        => 'unregistered_no_output_tax',
            'vendor_tax_rate_pct'          => 18.00,
            'vendor_tax_recoverable'       => false,
            'prices_include_output_tax'    => false,
            'output_tax_rate_pct'          => 0.00,
            'payment_buffer_pct'           => 2.00,
            'fx_rate'                      => 90.0,
            'currency_iso'                 => 'INR',
            'cron_run_id'                  => 'test-cron-run-uuid',
        ];
    }

    /**
     * 25. Monthly cycle is in renewal_cycles_mask → engine proceeds past the
     *     cycle gate and lands in the policy path. We assert by checking that
     *     the resulting skip_reason is NOT 'cycle_not_mapped' and the metadata
     *     records renewal_cycle_enabled=true.
     */
    public function testServiceMonthly_evaluatedWhenMonthlyInRenewalMask(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $svc  = $this->baseService('Monthly', $mask, $mask);

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $this->assertNotSame('cycle_not_mapped', $d['skip_reason']);
        $this->assertNotSame('cycle_unsupported', $d['skip_reason']);

        $meta = json_decode((string) $d['metadata_json'], true);
        $this->assertIsArray($meta);
        $this->assertTrue($meta['renewal_cycle_enabled']);
        $this->assertTrue($meta['catalog_cycle_enabled']);
        $this->assertSame('monthly', $meta['cycle_recurring_column']);
    }

    /**
     * 26. Quarterly cycle ABSENT from renewal_cycles_mask → skip_reason must
     *     be 'cycle_not_mapped' (distinct from 'cycle_unsupported' which
     *     means Free Account / One Time / unknown).
     */
    public function testServiceQuarterly_skippedWithCycleNotMappedWhenAbsentFromMask(): void
    {
        // Mask contains Monthly + Annually but NOT Quarterly.
        $mask = CycleSet::fromCycles(['Monthly', 'Annually'])->toMask();
        $svc  = $this->baseService('Quarterly', $mask, $mask);

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $this->assertSame('cycle_not_mapped', $d['skip_reason']);
        $this->assertFalse((bool) $d['applied']);
        $this->assertSame('Quarterly', $d['billing_cycle']);
        $this->assertSame(3, $d['cycle_months']);

        // Distinct from cycle_unsupported: cycle_months is non-null (it IS a
        // supported cycle, just not in the renewal mask).
        $this->assertNotSame('cycle_unsupported', $d['skip_reason']);

        $meta = json_decode((string) $d['metadata_json'], true);
        $this->assertFalse($meta['renewal_cycle_enabled']);
    }

    /**
     * 27. Biennially cycle present in renewal_cycles_mask → engine proceeds.
     */
    public function testServiceBiennially_evaluatedWhenInMask(): void
    {
        $mask = CycleSet::fromCycles(['Biennially'])->toMask();
        $svc  = $this->baseService('Biennially', $mask, $mask);

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $this->assertNotSame('cycle_not_mapped', $d['skip_reason']);
        $this->assertSame(24, $d['cycle_months']);
        $this->assertSame('Biennially', $d['billing_cycle']);

        $meta = json_decode((string) $d['metadata_json'], true);
        $this->assertTrue($meta['renewal_cycle_enabled']);
        $this->assertSame('biennially', $meta['cycle_recurring_column']);
    }

    /**
     * 28. Free Account → skip 'cycle_unsupported' (NOT 'cycle_not_mapped').
     *     CycleNormalizer returns null for Free Account, so the engine bails
     *     at the cycle_unsupported gate before it ever reads the mask.
     */
    public function testServiceFreeAccount_skippedWithCycleUnsupported(): void
    {
        $svc = $this->baseService('Free Account', CycleSet::MASK_MAX, CycleSet::MASK_MAX);

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $this->assertSame('cycle_unsupported', $d['skip_reason']);
        $this->assertFalse((bool) $d['applied']);
        $this->assertNull($d['cycle_months']);
    }

    /**
     * 29. One Time → skip 'cycle_unsupported'.
     */
    public function testServiceOneTime_skippedWithCycleUnsupported(): void
    {
        $svc = $this->baseService('One Time', CycleSet::MASK_MAX, CycleSet::MASK_MAX);

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $this->assertSame('cycle_unsupported', $d['skip_reason']);
        $this->assertNotSame('cycle_not_mapped', $d['skip_reason']);
        $this->assertNull($d['cycle_months']);
    }

    /**
     * 30. Per-cycle markup override for Annually: strategy=fixed, value=9200,
     *     as_total=true → decision.metadata.rounded_price === 9200.
     *
     *     Without the override, the engine would compute landed × markup ×
     *     cycle_months. The override REPLACES that computation entirely.
     */
    public function testPerCycleMarkupOverrideUsedWhenPresent(): void
    {
        $overrides = json_encode([
            'Annually' => [
                'strategy' => 'fixed',
                'value'    => 9200.0,
                'as_total' => true,
            ],
        ]);
        $mask = CycleSet::fromCycles(['Annually'])->toMask();
        $svc  = $this->baseService('Annually', $mask, $mask, $overrides);

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $meta = json_decode((string) $d['metadata_json'], true);
        $this->assertSame('fixed', $meta['markup_strategy_used']);
        $this->assertEqualsWithDelta(9200.0, (float) $meta['markup_value_used'], 0.0001);
        $this->assertSame('mapping_override', $meta['markup_source']);
        $this->assertEqualsWithDelta(9200.0, (float) $meta['rounded_price'], 0.0001);
        $this->assertEqualsWithDelta(9200.0, (float) $meta['pre_round_price'], 0.0001);
    }

    /**
     * 31. nearest_99 rounding mode on a computed price of 1234.00 →
     *     rounded_price=1234.99, pre_round_price=1234.00, both visible in
     *     metadata_json, rounding_mode='nearest_99'.
     *
     *     We force pre_round=1234 by using a 'fixed' override with as_total
     *     so the math is deterministic across hosts (FX/buffer drift can't
     *     interfere).
     */
    public function testRoundingModeAppliedAndAuditedInMetadata(): void
    {
        $overrides = json_encode([
            'Monthly' => [
                'strategy' => 'fixed',
                'value'    => 1234.00,
                'as_total' => true,
            ],
        ]);
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $svc  = $this->baseService('Monthly', $mask, $mask, $overrides);
        $svc['mapping']['rounding_mode'] = Rounding::MODE_NEAREST_99;

        $engine = new RenewalEngine($this->settings(), $this->stubResolver());
        $d = $engine->decide($svc, new \DateTimeImmutable('2026-05-22'));

        $meta = json_decode((string) $d['metadata_json'], true);
        $this->assertSame('nearest_99', $meta['rounding_mode']);
        $this->assertEqualsWithDelta(1234.00, (float) $meta['pre_round_price'], 0.0001);
        $this->assertEqualsWithDelta(1234.99, (float) $meta['rounded_price'], 0.0001);
    }
}
