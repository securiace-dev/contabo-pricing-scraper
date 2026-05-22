<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ServiceConfigSnapshot;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * A.6.4 — ServiceConfigSnapshot::capture() against FakeCapsule.
 *
 * Seeds a mapped, configured service and proves the snapshot records the
 * resolver's base + config totals, the profile context, and the recovered
 * image/region (round-tripped via the value links).
 */
final class ServiceConfigSnapshotTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function seedMappedService(): void
    {
        // Service #1 on product #2, monthly. recurringamount is stale/irrelevant.
        Capsule::table('tblhosting')->insert(['id' => 1, 'packageid' => 2, 'billingcycle' => 'monthly', 'recurringamount' => 31.0]);
        // Product catalog base = 10.00/mo.
        Capsule::table('tblpricing')->insert(['type' => 'product', 'relid' => 2, 'currency' => 1, 'monthly' => 10.0]);

        // Selected: Image sub #100 (Ubuntu, 0 delta) + Backup sub #101 (1.50).
        Capsule::table('tblhostingconfigoptions')->insert(['relid' => 1, 'configid' => 1, 'optionid' => 100, 'qty' => 1]);
        Capsule::table('tblhostingconfigoptions')->insert(['relid' => 1, 'configid' => 2, 'optionid' => 101, 'qty' => 1]);
        Capsule::table('tblpricing')->insert(['type' => 'configoptions', 'relid' => 100, 'currency' => 1, 'monthly' => 0.0]);
        Capsule::table('tblpricing')->insert(['type' => 'configoptions', 'relid' => 101, 'currency' => 1, 'monthly' => 1.5]);

        // Mapping → profile → version.
        Capsule::table('mod_contabo_mapping')->insert(['profile_id' => 7, 'product_id' => 2, 'active' => 1]);
        Capsule::table('mod_contabo_profile')->insert(['id' => 7, 'plan_slug' => 'cloud-vps-10', 'profile_mode' => 'customer_configurable_product']);
        Capsule::table('mod_contabo_profile_version')->insert(['profile_id' => 7, 'version' => 3]);

        // Link tables: sub #100 → Image value, sub #101 → Data Protection value.
        Capsule::table('mod_contabo_config_option_link')->insert(['id' => 50, 'profile_id' => 7, 'dimension_key' => 'Image']);
        Capsule::table('mod_contabo_config_option_link')->insert(['id' => 51, 'profile_id' => 7, 'dimension_key' => 'Region']);
        Capsule::table('mod_contabo_config_option_value_link')->insert(['option_link_id' => 50, 'contabo_value_key' => 'os:ubuntu', 'contabo_label' => '[OS] Ubuntu 24.04', 'whmcs_sub_id' => 100]);
    }

    private function snap(): ServiceConfigSnapshot
    {
        return new ServiceConfigSnapshot(['tax_registration_mode' => 'unregistered_no_output_tax']);
    }

    public function testCaptureRecordsBaseConfigAndContext(): void
    {
        $this->seedMappedService();
        $id = $this->snap()->capture(1);
        $this->assertGreaterThan(0, $id);

        $row = $this->snap()->latestForService(1);
        $this->assertNotNull($row);
        $this->assertSame(1, (int) $row['service_id']);
        $this->assertSame(2, (int) $row['whmcs_product_id']);
        $this->assertSame(7, (int) $row['profile_id']);
        $this->assertSame('customer_configurable_product', (string) $row['profile_mode']);
        $this->assertSame('cloud-vps-10', (string) $row['plan_slug']);
        $this->assertEqualsWithDelta(10.0, (float) $row['base_price_snapshot'], 0.0001);
        $this->assertEqualsWithDelta(1.5, (float) $row['config_option_price_snapshot'], 0.0001); // 0 + 1.50
        $this->assertSame('3', (string) $row['pricing_version_snapshot']);
        $this->assertSame('unregistered_no_output_tax', (string) $row['tax_mode_snapshot']);
    }

    public function testCaptureRecoversSelectedImage(): void
    {
        $this->seedMappedService();
        $this->snap()->capture(1);
        $row = $this->snap()->latestForService(1);
        $this->assertSame('[OS] Ubuntu 24.04', (string) $row['selected_image']);
        $this->assertNull($row['selected_region']); // no value link for the region sub
    }

    public function testSelectedOptionsJsonCapturesLines(): void
    {
        $this->seedMappedService();
        $this->snap()->capture(1);
        $row = $this->snap()->latestForService(1);
        $decoded = json_decode((string) $row['selected_options_json'], true);
        $this->assertIsArray($decoded);
        $this->assertCount(2, $decoded); // two selected options
    }

    public function testUnmappedServiceStillSnapshots(): void
    {
        // Service with no mapping → still captures base + config, marked unmapped.
        Capsule::table('tblhosting')->insert(['id' => 9, 'packageid' => 999, 'billingcycle' => 'monthly', 'recurringamount' => 0.0]);
        Capsule::table('tblpricing')->insert(['type' => 'product', 'relid' => 999, 'currency' => 1, 'monthly' => 5.0]);

        $id = $this->snap()->capture(9);
        $this->assertGreaterThan(0, $id);
        $row = $this->snap()->latestForService(9);
        $this->assertSame('unmapped', (string) $row['profile_mode']);
        $this->assertNull($row['profile_id']);
        $this->assertEqualsWithDelta(5.0, (float) $row['base_price_snapshot'], 0.0001);
    }

    public function testMissingServiceReturnsZero(): void
    {
        $this->assertSame(0, $this->snap()->capture(12345));
    }

    public function testLatestForServiceReturnsMostRecent(): void
    {
        $this->seedMappedService();
        $first = $this->snap()->capture(1);
        $second = $this->snap()->capture(1);
        $this->assertGreaterThan($first, $second);
        $this->assertSame($second, (int) $this->snap()->latestForService(1)['id']);
    }
}
