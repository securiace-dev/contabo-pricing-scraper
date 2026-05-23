<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionLinkRepository;
use ContaboPricing\ConfigOptionPricingContext;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 0.5.1 item 4 — value-level drift (sub-option + config-option pricing).
 *
 * The option-level drift guard already protects hand-edited tblproductconfigoptions
 * rows. This proves the guard now also covers a hand-edited sub-option row OR a
 * hand-edited cycle price: on re-apply it is flagged (drift_skipped) and skipped,
 * never clobbered. Drift baseline lives in value_link.expected_hash (schema v6).
 */
final class ConfigurableOptionsSyncerValueDriftTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function syncer(ConfigOptionLinkRepository $links): ConfigurableOptionsSyncer
    {
        $audit = new class('vdrift') extends OptionAuditLog {
            protected function storeRow(array $row): int { static $n = 0; return ++$n; }
        };
        return new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(false), $audit, $links);
    }

    private function ctx(): ConfigOptionPricingContext
    {
        return new ConfigOptionPricingContext(1, 90.0, 'cost_plus_pct', 15.0, 'exact_2_decimals');
    }

    /** @return list<array<string,mixed>> */
    private function specs(): array
    {
        return [[
            'dimension_key' => 'Image',
            'optiontype'    => OptionTypeMapper::TYPE_DROPDOWN,
            'values'        => [
                ['value_key' => 'os:ubuntu', 'label' => '[OS] Ubuntu', 'category' => 'OS', 'monthly_eur_delta' => 2.0, 'is_default' => true, 'sortorder' => 0],
            ],
        ]];
    }

    private function subIdAfterApply(ConfigOptionLinkRepository $links): int
    {
        $optLink = $links->findOptionLink(7, 'Image');
        $valLink = $links->findValueLink((int) $optLink['id'], 'os:ubuntu');
        return (int) ($valLink['whmcs_sub_id'] ?? 0);
    }

    public function testSubOptionHandEditIsDetectedAndSkipped(): void
    {
        $links = new ConfigOptionLinkRepository();
        $r1 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertSame(0, (int) ($r1['summary']['drift_skipped'] ?? 0));

        $subId = $this->subIdAfterApply($links);
        $this->assertGreaterThan(0, $subId);

        // Admin hand-edits the live SUB-OPTION (rename) — the option row is untouched.
        Capsule::table('tblproductconfigoptionssub')->where('id', $subId)->update(['optionname' => 'HAND EDITED SUB']);

        $r2 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertGreaterThanOrEqual(1, (int) $r2['summary']['drift_skipped']);

        $live = (array) Capsule::table('tblproductconfigoptionssub')->where('id', $subId)->first();
        $this->assertSame('HAND EDITED SUB', (string) $live['optionname'], 'sub-option hand-edit must NOT be clobbered');
    }

    public function testPricingHandEditIsDetectedAndSkipped(): void
    {
        $links = new ConfigOptionLinkRepository();
        $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $subId = $this->subIdAfterApply($links);

        // Admin hand-edits the live PRICING (monthly) — sub-option untouched.
        Capsule::table('tblpricing')->where('type', 'configoptions')->where('relid', $subId)->update(['monthly' => 999.99]);

        $r2 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertGreaterThanOrEqual(1, (int) $r2['summary']['drift_skipped']);

        $price = (array) Capsule::table('tblpricing')->where('type', 'configoptions')->where('relid', $subId)->first();
        $this->assertEqualsWithDelta(999.99, (float) $price['monthly'], 0.001, 'hand-edited price must NOT be clobbered');
    }

    public function testCleanReapplyHasNoValueDrift(): void
    {
        $links = new ConfigOptionLinkRepository();
        $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $r2 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertSame(0, (int) $r2['summary']['drift_skipped'], 'an untouched re-apply must report no drift');
    }
}
