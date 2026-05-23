<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionLinkRepository;
use ContaboPricing\ConfigOptionPricingContext;
use ContaboPricing\ConfigurableOptionsSyncer;
use ContaboPricing\OptionAuditLog;
use ContaboPricing\OptionTypeMapper;
use ContaboPricing\ServiceRevenueResolver;
use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Regression guard for the stdClass-vs-array trap.
 *
 * FakeCapsule returns arrays by default, but REAL WHMCS Capsule returns
 * stdClass from first()/get(). That gap has masked the same bug FOUR times
 * (adapter update branch, ServiceRevenueResolver::fetchBase/fetchConfigOptions,
 * the link repos) — each only surfaced in a manual real-WHMCS check. With
 * `Capsule::$returnStdClass = true`, these tests drive the stdClass-sensitive
 * read paths in the fast unit suite, so a future missing `(array)` cast fails
 * here instead of in production.
 */
final class StdClassModeTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$returnStdClass = true; // mimic real WHMCS return type
    }

    protected function tearDown(): void
    {
        Capsule::$returnStdClass = false;
    }

    /** @return list<array<string,mixed>> */
    private function specs(): array
    {
        return [
            ['dimension_key' => 'Image', 'optiontype' => OptionTypeMapper::TYPE_DROPDOWN, 'values' => [
                ['value_key' => 'os:ubuntu', 'label' => '[OS] Ubuntu', 'category' => 'OS', 'monthly_eur_delta' => 0.0, 'is_default' => true, 'sortorder' => 0],
            ]],
            ['dimension_key' => 'Networking:IPv4', 'optiontype' => OptionTypeMapper::TYPE_QUANTITY, 'values' => [
                ['value_key' => 'ipv4', 'label' => 'Additional IPv4', 'category' => 'IPv4', 'monthly_eur_delta' => 1.0, 'is_default' => false, 'sortorder' => 0],
            ]],
        ];
    }

    private function ctx(): ConfigOptionPricingContext
    {
        return new ConfigOptionPricingContext(1, 90.0, 'cost_plus_pct', 15.0, 'exact_2_decimals');
    }

    private function syncer(ConfigOptionLinkRepository $links): ConfigurableOptionsSyncer
    {
        $audit = new class('stdclass') extends OptionAuditLog {
            protected function storeRow(array $row): int { static $n = 0; return ++$n; }
        };
        return new ConfigurableOptionsSyncer(new WhmcsConfigOptionsAdapter(false), $audit, $links);
    }

    public function testLinkRepoFindReturnsUsableArrayUnderStdClass(): void
    {
        $repo = new ConfigOptionLinkRepository();
        $repo->upsertOptionLink(7, 'Image', 1, 9200, ['expose_to_customer' => true]);
        $row = $repo->findOptionLink(7, 'Image'); // first() yields stdClass; repo must (array)-cast
        $this->assertNotNull($row);
        $this->assertSame(9200, (int) $row['whmcs_option_id']);
        $this->assertSame(1, (int) $row['expose_to_customer']);
    }

    public function testApplyReapplyAndDriftSurviveStdClass(): void
    {
        $links = new ConfigOptionLinkRepository();

        // First apply — exercises insert branch + fetchOption (drift baseline).
        $r1 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertSame(2, $r1['options']);

        // Re-apply — exercises the adapter's UPDATE/noop branch (the stdClass bug
        // site) + the drift fetchOption read; must not crash + must be idempotent.
        $r2 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertSame(0, $r2['summary']['created']);

        // Drift — hand-edit the live option, re-apply: flagged + not clobbered.
        $optId = (int) $links->findOptionLink(7, 'Image')['whmcs_option_id'];
        Capsule::table('tblproductconfigoptions')->where('id', $optId)->update(['optionname' => 'X EDITED']);
        $r3 = $this->syncer($links)->apply(7, 501, 'g', 'G', $this->specs(), $this->ctx());
        $this->assertGreaterThanOrEqual(1, (int) $r3['summary']['drift_skipped']);
    }

    public function testServiceRevenueResolverSurvivesStdClass(): void
    {
        Capsule::table('tblhosting')->insert(['id' => 1, 'packageid' => 2, 'billingcycle' => 'monthly', 'amount' => 31.0]);
        Capsule::table('tblpricing')->insert(['type' => 'product', 'relid' => 2, 'currency' => 1, 'monthly' => 10.0]);
        Capsule::table('tblhostingconfigoptions')->insert(['relid' => 1, 'configid' => 1, 'optionid' => 100, 'qty' => 1]);
        Capsule::table('tblpricing')->insert(['type' => 'configoptions', 'relid' => 100, 'currency' => 1, 'monthly' => 1.5]);

        $r = (new ServiceRevenueResolver())->resolveForService(1); // fetchBase + fetchConfigOptions read stdClass
        $this->assertEqualsWithDelta(10.0, $r['base'], 0.0001);
        $this->assertEqualsWithDelta(1.5, $r['config_options'], 0.0001);
    }
}
