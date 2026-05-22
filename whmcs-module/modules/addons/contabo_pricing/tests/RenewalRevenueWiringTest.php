<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\PolicyResolver;
use ContaboPricing\RenewalEngine;
use ContaboPricing\Rounding;
use ContaboPricing\ServiceConfigSnapshot;
use ContaboPricing\ServiceRevenueResolver;
use PHPUnit\Framework\TestCase;

/**
 * Amendment 5 wiring — RenewalEngine records the TRUE revenue (resolver,
 * snapshot-preferred) + the drift from the stale recurringamount in metadata,
 * opt-in. Without the resolver, behaviour is unchanged (null fields).
 */
final class RenewalRevenueWiringTest extends TestCase
{
    private function policyStub(): PolicyResolver
    {
        return new class extends PolicyResolver {
            protected function fetchServicePolicy(int $serviceId): ?array { return null; }
        };
    }

    /** @return array<string,mixed> */
    private function service(): array
    {
        return [
            'id'              => 4242,
            'status'          => 'Active',
            'billingcycle'    => 'Monthly',
            'recurringamount' => 1200.00, // stale stored charge
            'subscriptionid'  => '',
            'nextduedate'     => '2099-12-31',
            'profile'         => ['id' => 7, 'default_policy' => 'current_term', 'margin_floor_pct' => 15.0, 'fx_buffer_pct' => 2.0, 'notice_days_default' => 30, 'large_increase_threshold_pct' => 10.0, 'max_increase_pct' => 25.0, 'allow_auto_decrease' => false],
            'profile_version' => ['id' => 71, 'base_monthly_eur' => 10.0, 'markup_strategy' => 'cost_plus_pct', 'markup_value' => 50.0, 'sell_price_local_monthly' => null],
            'mapping'         => ['renewal_cycles_mask' => 1, 'catalog_cycles_mask' => 1, 'markup_overrides_json' => null, 'rounding_mode' => Rounding::MODE_EXACT_2_DECIMALS],
        ];
    }

    /** @return array<string,mixed> */
    private function settings(): array
    {
        return ['repricing_phase' => 'observe', 'tax_registration_mode' => 'unregistered_no_output_tax', 'vendor_tax_rate_pct' => 18.0, 'vendor_tax_recoverable' => false, 'prices_include_output_tax' => false, 'output_tax_rate_pct' => 0.0, 'payment_buffer_pct' => 2.0, 'fx_rate' => 90.0, 'currency_iso' => 'INR', 'cron_run_id' => 'test'];
    }

    private function revenueStub(float $total, string $source): ServiceRevenueResolver
    {
        return new class($total, $source) extends ServiceRevenueResolver {
            private $total;
            private $source;
            public function __construct(float $t, string $s) { $this->total = $t; $this->source = $s; }
            public function resolveForService(int $serviceId): array
            {
                return ['base' => 10.0, 'config_options' => $this->total - 10.0, 'addons' => 0.0, 'total' => $this->total, 'breakdown' => ['source' => 'service']];
            }
            public function resolveFromSnapshot(array $snapshotRow): array
            {
                return ['base' => 10.0, 'config_options' => $this->total - 10.0, 'addons' => 0.0, 'total' => $this->total, 'breakdown' => ['source' => 'snapshot']];
            }
        };
    }

    public function testWithoutResolverRevenueFieldsAreNull(): void
    {
        $engine = new RenewalEngine($this->settings(), $this->policyStub());
        $meta = json_decode((string) $engine->decide($this->service(), new \DateTimeImmutable('2026-05-22'))['metadata_json'], true);
        $this->assertNull($meta['resolved_revenue']);
        $this->assertNull($meta['revenue_source']);
        $this->assertEqualsWithDelta(1200.0, $meta['stale_recurringamount'], 0.001);
    }

    public function testResolverRecordsTrueRevenueAndDriftLive(): void
    {
        $engine = new RenewalEngine($this->settings(), $this->policyStub(), null, $this->revenueStub(1500.0, 'live'), null);
        $meta = json_decode((string) $engine->decide($this->service(), new \DateTimeImmutable('2026-05-22'))['metadata_json'], true);
        $this->assertSame('live', $meta['revenue_source']);
        $this->assertEqualsWithDelta(1500.0, $meta['resolved_revenue'], 0.001);
        $this->assertEqualsWithDelta(300.0, $meta['revenue_drift'], 0.001); // 1500 true - 1200 stale
    }

    public function testSnapshotIsPreferredWhenAvailable(): void
    {
        $snapReader = new class([]) extends ServiceConfigSnapshot {
            public function latestForService(int $serviceId): ?array { return ['service_id' => $serviceId, 'base_price_snapshot' => 10.0, 'config_option_price_snapshot' => 1490.0]; }
        };
        $engine = new RenewalEngine($this->settings(), $this->policyStub(), null, $this->revenueStub(1500.0, 'snapshot'), $snapReader);
        $meta = json_decode((string) $engine->decide($this->service(), new \DateTimeImmutable('2026-05-22'))['metadata_json'], true);
        $this->assertSame('snapshot', $meta['revenue_source']);
        $this->assertEqualsWithDelta(1500.0, $meta['resolved_revenue'], 0.001);
    }

    public function testWholeConfigMarginRecordedFromSnapshotSelections(): void
    {
        // Snapshot supplies per-option EUR deltas → engine computes the
        // whole-config landed cost + margin ratio (Phase B), recorded in meta.
        $snapReader = new class([]) extends ServiceConfigSnapshot {
            public function latestForService(int $serviceId): ?array
            {
                return [
                    'service_id'                   => $serviceId,
                    'base_price_snapshot'          => 515.0,
                    'config_option_price_snapshot' => 985.0,
                    'selected_options_json'        => json_encode([
                        ['sub_id' => 101, 'qty' => 1, 'eur_monthly' => 1.5],
                        ['sub_id' => 102, 'qty' => 2, 'eur_monthly' => 1.0],
                    ]),
                ];
            }
        };
        $engine = new RenewalEngine($this->settings(), $this->policyStub(), null, $this->revenueStub(1500.0, 'snapshot'), $snapReader);
        $meta = json_decode((string) $engine->decide($this->service(), new \DateTimeImmutable('2026-05-22'))['metadata_json'], true);

        $this->assertSame('whole_config', $meta['margin_basis']);
        $this->assertNotNull($meta['whole_config_landed_for_cycle']);
        $this->assertNotNull($meta['whole_config_margin_ratio']);
        $this->assertIsBool($meta['whole_config_below_floor']);
        // base 10 EUR + (1.5 + 1.0×2) = 13.5 EUR; fx 90 + 2% fx + 2% pay buffers,
        // monthly, × 1 cycle month. Whole-config landed > base-only landed.
        $this->assertGreaterThan(0.0, (float) $meta['whole_config_landed_for_cycle']);
    }

    public function testWholeConfigNotComputedWithoutSnapshot(): void
    {
        // Resolver but no snapshot → margin_basis stays base_only (no EUR deltas).
        $engine = new RenewalEngine($this->settings(), $this->policyStub(), null, $this->revenueStub(1500.0, 'live'), null);
        $meta = json_decode((string) $engine->decide($this->service(), new \DateTimeImmutable('2026-05-22'))['metadata_json'], true);
        $this->assertSame('base_only', $meta['margin_basis']);
        $this->assertNull($meta['whole_config_landed_for_cycle']);
    }
}
