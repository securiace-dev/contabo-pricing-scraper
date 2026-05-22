<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionPricingContext;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;

/**
 * Phase A.6.2 — ConfigurableOptionsSyncer (observe / preview).
 *
 * Runs the syncer against a dry-run adapter (no DB) with a capturing audit log,
 * over a representative spec set: Image (dropdown) incl. a negative-delta value,
 * Networking:IPv4 (quantity) and Data Protection (yes/no, with a setup delta).
 */
final class ConfigurableOptionsSyncerTest extends TestCase
{
    /** @var array<int,array<string,mixed>> */
    private $audits = [];

    private function syncer(): ConfigurableOptionsSyncer
    {
        $captured = &$this->audits;
        $audit = new class('batch-x', $captured) extends OptionAuditLog {
            /** @var array<int,array<string,mixed>> */
            private $sink;
            public function __construct(string $b, array &$sink)
            {
                parent::__construct($b);
                $this->sink = &$sink;
            }
            protected function storeRow(array $row): int
            {
                $this->sink[] = $row;
                return count($this->sink);
            }
        };

        return new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(true), $audit);
    }

    /** @return list<array<string,mixed>> */
    private function specs(): array
    {
        return [
            [
                'dimension_key' => 'Image',
                'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN, // 1
                'values'        => [
                    ['value_key' => 'os:ubuntu-24-04', 'label' => '[OS] Ubuntu 24.04', 'category' => 'OS', 'monthly_eur_delta' => 0.0,  'setup_eur_delta' => 0.0, 'is_default' => true,  'sortorder' => 0],
                    ['value_key' => 'os:windows',      'label' => '[OS] Windows',       'category' => 'OS', 'monthly_eur_delta' => 10.0, 'setup_eur_delta' => 0.0, 'is_default' => false, 'sortorder' => 1],
                    ['value_key' => 'os:cheap',        'label' => '[OS] CheapDistro',   'category' => 'OS', 'monthly_eur_delta' => -2.0, 'setup_eur_delta' => 0.0, 'is_default' => false, 'sortorder' => 2],
                ],
            ],
            [
                'dimension_key' => 'Networking:IPv4',
                'optiontype'    => OptionTypeMapper::TYPE_QUANTITY, // 4
                'values'        => [
                    ['value_key' => 'ipv4-extra', 'label' => 'Additional IPv4', 'category' => 'IPv4', 'monthly_eur_delta' => 1.0, 'setup_eur_delta' => 0.0, 'is_default' => false, 'sortorder' => 0],
                ],
            ],
            [
                'dimension_key' => 'Data Protection',
                'optiontype'    => OptionTypeMapper::TYPE_YESNO, // 3
                'values'        => [
                    ['value_key' => 'none',        'label' => 'None',        'category' => 'Backup', 'monthly_eur_delta' => 0.0, 'setup_eur_delta' => 0.0, 'is_default' => true,  'sortorder' => 0],
                    ['value_key' => 'auto-backup', 'label' => 'Auto Backup', 'category' => 'Backup', 'monthly_eur_delta' => 1.5, 'setup_eur_delta' => 5.0, 'is_default' => false, 'sortorder' => 1],
                ],
            ],
        ];
    }

    private function ctx(int $currencyId = 1): ConfigOptionPricingContext
    {
        // landedMultiplier 90.0 (EUR→local landed monthly), cost+15%.
        return new ConfigOptionPricingContext($currencyId, 90.0, 'cost_plus_pct', 15.0, 'exact_2_decimals');
    }

    /** @return array<string,mixed> */
    private function valueByLabel(array $report, string $optionDim, string $label): array
    {
        foreach ($report['options'] as $opt) {
            if ($opt['dimension_key'] !== $optionDim) {
                continue;
            }
            foreach ($opt['values'] as $v) {
                if ($v['label'] === $label) {
                    return $v;
                }
            }
        }
        $this->fail("value $label not found under $optionDim");
    }

    public function testStructureGroupOptionsValues(): void
    {
        $report = $this->syncer()->observe(7, 'Contabo Cloud VPS 10', $this->specs(), $this->ctx());

        $this->assertTrue($report['dry_run']);
        $this->assertSame('dryrun', $report['group']['action']);
        $this->assertSame(1, $report['currency_id']);
        $this->assertSame(3, $report['totals']['options']);
        $this->assertArrayHasKey('group', $report);
        $this->assertSame('tblproductconfiggroups', $report['group']['table']);
    }

    public function testValueAndOptionCounts(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $this->assertSame(3, $report['totals']['options']);
        $this->assertSame(6, $report['totals']['values']); // 3 + 1 + 2
    }

    public function testOptiontypesArePassedThrough(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $byDim = [];
        foreach ($report['options'] as $o) {
            $byDim[$o['dimension_key']] = $o;
        }
        $this->assertSame(OptionTypeMapper::TYPE_DROPDOWN, $byDim['Image']['optiontype']);
        $this->assertSame(OptionTypeMapper::TYPE_QUANTITY, $byDim['Networking:IPv4']['optiontype']);
        $this->assertSame(OptionTypeMapper::TYPE_YESNO, $byDim['Data Protection']['optiontype']);
        $this->assertTrue($byDim['Networking:IPv4']['is_quantity']);
        $this->assertFalse($byDim['Image']['is_quantity']);
    }

    public function testQuantityOptionGetsBounds(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        foreach ($report['options'] as $o) {
            if ($o['dimension_key'] === 'Networking:IPv4') {
                $this->assertSame(0, $o['option']['payload']['qtyminimum']);
                $this->assertSame(1, $o['option']['payload']['qtymaximum']); // one value
            }
        }
    }

    public function testPositiveDeltaPricedAcrossCycles(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $windows = $this->valueByLabel($report, 'Image', '[OS] Windows');
        // landed 10*90 = 900/mo; cost+15%: monthly 1035.00; annually 900*1.15*12.
        $this->assertSame(1035.00, $windows['cycle_prices']['monthly']);
        $this->assertSame(12420.00, $windows['cycle_prices']['annually']);
        $this->assertSame(3105.00, $windows['cycle_prices']['quarterly']); // *3
    }

    public function testNegativeDeltaClampedToZero(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $cheap = $this->valueByLabel($report, 'Image', '[OS] CheapDistro');
        foreach ($cheap['cycle_prices'] as $p) {
            $this->assertSame(0.0, $p);
        }
    }

    public function testDefaultValuePricesToZero(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $ubuntu = $this->valueByLabel($report, 'Image', '[OS] Ubuntu 24.04');
        $this->assertSame(0.0, $ubuntu['cycle_prices']['monthly']);
        $this->assertSame(0.0, $ubuntu['cycle_prices']['triennially']);
    }

    public function testZeroDeltaIsFreeEvenUnderCostPlusAmount(): void
    {
        // cost_plus_amount would otherwise add a flat amount to a 0 delta.
        $ctx = new ConfigOptionPricingContext(1, 90.0, 'cost_plus_amount', 50.0, 'exact_2_decimals');
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $ctx);
        $ubuntu = $this->valueByLabel($report, 'Image', '[OS] Ubuntu 24.04');
        $this->assertSame(0.0, $ubuntu['cycle_prices']['monthly']);
        // a real delta does get the flat amount: IPv4 landed 90 + 50 = 140/mo.
        $ipv4 = $this->valueByLabel($report, 'Networking:IPv4', 'Additional IPv4');
        $this->assertSame(140.00, $ipv4['cycle_prices']['monthly']);
    }

    public function testSetupFeeReplicatedAcrossCycles(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $backup = $this->valueByLabel($report, 'Data Protection', 'Auto Backup');
        // setup 5 EUR * 90 = 450.00, same in every cycle column.
        $this->assertSame(450.00, $backup['setup_fees']['monthly']);
        $this->assertSame(450.00, $backup['setup_fees']['annually']);
        // a value with no setup delta emits no setup columns.
        $windows = $this->valueByLabel($report, 'Image', '[OS] Windows');
        $this->assertSame([], $windows['setup_fees']);
    }

    public function testNonBaseCurrencySkipsPricing(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx(2)); // currency 2 != base
        $this->assertNotSame([], $report['skipped']);
        // Every priced value should be flagged skipped with the non-base reason.
        $reasons = array_unique(array_column($report['skipped'], 'reason'));
        $this->assertContains(WhmcsConfigOptionsAdapter::SKIP_NON_INR, $reasons);
    }

    public function testAuditRowsRecorded(): void
    {
        $report = $this->syncer()->observe(7, 'G', $this->specs(), $this->ctx());
        $this->assertGreaterThan(0, $report['audit_count']);
        $this->assertNotEmpty($this->audits);
        $this->assertSame('observed', $this->audits[0]['action']);
        $this->assertSame(7, $this->audits[0]['profile_id']);
    }
}
