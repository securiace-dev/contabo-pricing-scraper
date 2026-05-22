<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CycleSet;
use ContaboPricing\DecisionLog;
use ContaboPricing\PolicyResolver;
use ContaboPricing\Rounding;
use ContaboPricing\ScheduledChangeProcessor;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5 ScheduledChangeProcessor tests — assert cycle-mask filtering and
 * the single-write-path invariant (#33).
 *
 * Plan reference: amended Phase A.5 brief sections 6, 10, 11, 12.
 *
 * Test numbering follows the brief:
 *   32. testScheduledChangeWithCycleMask_appliesOnlyToMatchingCycles
 *   33. testScheduledChangeRoutesThroughSamePathAsCron
 */
final class ScheduledChangeCycleTest extends TestCase
{
    /** PolicyResolver stub. */
    private function stubResolver(): PolicyResolver
    {
        return new class extends PolicyResolver {
            protected function fetchServicePolicy(int $serviceId): ?array
            {
                return null;
            }
        };
    }

    /** DecisionLog spy — records inserted decision rows for assertion. */
    private function spyDecisionLog(): DecisionLog
    {
        return new class extends DecisionLog {
            /** @var list<array<string,mixed>> */
            public $rows = [];

            protected function storeRow(array $row): int
            {
                $this->rows[] = $row;
                return count($this->rows);
            }
            protected function lookupRow(string $cronRunId, int $serviceId): ?array
            {
                return null;
            }
        };
    }

    private function makeService(int $id, string $cycle, int $renewalMask): array
    {
        return [
            'id'              => $id,
            'status'          => 'Active',
            'billingcycle'    => $cycle,
            'recurringamount' => 1000.00,
            'subscriptionid'  => '',
            'nextduedate'     => '2026-05-22',
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
                'catalog_cycles_mask'    => $renewalMask,
                'markup_overrides_json'  => null,
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
            'cron_run_id'                  => 'sched-cron-run-uuid',
        ];
    }

    protected function setUp(): void
    {
        Capsule::reset();
    }

    /**
     * 32. A schedule with cycles_mask={Annually} should ONLY evaluate
     *     services whose billingcycle === 'Annually'. Services on Monthly /
     *     Quarterly must NOT receive a decision row from this scheduled
     *     change pass.
     */
    public function testScheduledChangeWithCycleMask_appliesOnlyToMatchingCycles(): void
    {
        $annuallyMask = CycleSet::fromCycles(['Annually'])->toMask();
        $maskAll      = CycleSet::MASK_MAX;

        $change = [
            'id'                  => 99,
            'scope'               => 'profile',
            'profile_id'          => 7,
            'new_price'           => 9500.0,
            'currency'            => 'INR',
            'effective_at'        => '2026-05-22 00:00:00',
            'cycles_mask'         => $annuallyMask,
            'applies_to_catalog'  => 0,
            'applies_to_renewals' => 1,
        ];

        $monthlyService    = $this->makeService(1, 'Monthly',   $maskAll);
        $quarterlyService  = $this->makeService(2, 'Quarterly', $maskAll);
        $annuallyService   = $this->makeService(3, 'Annually',  $maskAll);

        $servicesByCycle = [
            'Annually'  => [$monthlyService, $quarterlyService, $annuallyService], // intentionally mixed → processor filters
            'Monthly'   => [], // not enabled in mask
            'Quarterly' => [],
        ];

        $spy = $this->spyDecisionLog();
        $processor = new ScheduledChangeProcessor(
            $this->settings(),
            $this->stubResolver(),
            $spy,
            /* scheduleFetcher */ static function () use ($change): array {
                return [$change];
            },
            /* servicesFetcher */ static function (array $args) use ($servicesByCycle): array {
                $cycle = $args['cycle'];
                return $servicesByCycle[$cycle] ?? [];
            }
        );

        $summary = $processor->run(new \DateTimeImmutable('2026-05-22 01:00:00'));

        // Only one decision row should have been written — for the Annually
        // service. The Monthly/Quarterly entries we deliberately included in
        // the unfiltered batch must be filtered out by the processor's
        // service.billingcycle === cycle check.
        $this->assertSame(1, $summary['services_evaluated']);
        $this->assertCount(1, $spy->rows);
        $this->assertSame(3, $spy->rows[0]['service_id']);
        $this->assertSame('Annually', $spy->rows[0]['billing_cycle']);
    }

    /**
     * 33. ScheduledChangeProcessor MUST route through the same RenewalEngine
     *     applyWithGuards path as the cron decide() flow. We assert this by
     *     proving the same guard order is honoured:
     *
     *       (a) The DecisionLog spy receives a row whose shape matches a
     *           normal cron decision (same keys, including metadata_json
     *           with markup_strategy_used / rounding_mode populated by the
     *           engine — proving applyWithGuards built it).
     *       (b) current_term policy → requires_notice=true → applyWithGuards
     *           short-circuits to skip_reason='notice_scheduled'. That is the
     *           EXACT same outcome a cron decide() call would produce for the
     *           same service inside the notice window. A second write path
     *           would have produced a different shape (or skipped the notice
     *           guard entirely).
     *       (c) An additional run with a default policy that bypasses the
     *           notice gate (reprice_renewal + small change) → reaches the
     *           all-clear branch and gets phase-gated to 'phase_observe_only'.
     *           Again proving the SAME phase gate fired.
     */
    public function testScheduledChangeRoutesThroughSamePathAsCron(): void
    {
        $maskAll = CycleSet::MASK_MAX;
        $change = [
            'id'                  => 100,
            'scope'               => 'service',
            'service_id'          => 11,
            'new_price'           => 1100.0,
            'currency'            => 'INR',
            'effective_at'        => '2026-05-22 00:00:00',
            'cycles_mask'         => CycleSet::fromCycles(['Monthly'])->toMask(),
            'applies_to_catalog'  => 0,
            'applies_to_renewals' => 1,
        ];

        $service = $this->makeService(11, 'Monthly', $maskAll);

        $spy = $this->spyDecisionLog();
        $processor = new ScheduledChangeProcessor(
            $this->settings(),
            $this->stubResolver(),
            $spy,
            static function () use ($change): array { return [$change]; },
            static function (array $args) use ($service): array { return [$service]; }
        );

        $summary = $processor->run(new \DateTimeImmutable('2026-05-22 01:00:00'));

        $this->assertSame(1, $summary['services_evaluated']);
        $this->assertCount(1, $spy->rows, 'exactly one decision row written via the shared write path');
        $row = $spy->rows[0];

        // current_term policy → notice required → applyWithGuards short-
        // circuits exactly the way a cron decide() would have. That's the
        // load-bearing proof: scheduled changes don't bypass guards.
        $this->assertFalse((bool) $row['applied']);
        $this->assertSame('notice_scheduled', $row['skip_reason']);

        // metadata_json populated by RenewalEngine::decideForScheduledChange
        // → decideInternal → applyWithGuards.
        $meta = json_decode((string) $row['metadata_json'], true);
        $this->assertIsArray($meta);
        $this->assertSame('scheduled_change', $meta['forced_candidate_source']);
        $this->assertArrayHasKey('rounding_mode', $meta);
        $this->assertArrayHasKey('markup_strategy_used', $meta);

        // Action ledger: deferred because applied=false, with the same skip
        // reason — proves the processor consumed the engine's verdict
        // verbatim rather than overriding it.
        $actions = array_values(array_filter(
            Capsule::$inserts,
            static fn ($call) => $call['table'] === 'mod_contabo_pricing_action'
        ));
        $this->assertCount(1, $actions);
        $this->assertSame('defer', $actions[0]['values']['action_type']);
        $this->assertSame('notice_scheduled', $actions[0]['values']['reason']);

        // Now repeat with a default policy where the notice gate does NOT
        // short-circuit (reprice_renewal + small change → requires_notice=false)
        // and prove we end up at phase_observe_only — same phase gate cron
        // hits.
        Capsule::reset();
        $service2 = $service;
        $service2['profile']['default_policy'] = 'reprice_renewal';
        // Force a 1% candidate so requires_notice stays false.
        $change2 = $change;
        $change2['new_price'] = $service2['recurringamount'] * 1.01;

        $spy2 = $this->spyDecisionLog();
        $processor2 = new ScheduledChangeProcessor(
            $this->settings(),
            $this->stubResolver(),
            $spy2,
            static function () use ($change2): array { return [$change2]; },
            static function (array $args) use ($service2): array { return [$service2]; }
        );
        $processor2->run(new \DateTimeImmutable('2026-05-22 01:00:00'));

        $this->assertCount(1, $spy2->rows);
        $this->assertFalse((bool) $spy2->rows[0]['applied']);
        $this->assertSame('phase_observe_only', $spy2->rows[0]['skip_reason']);
    }
}
