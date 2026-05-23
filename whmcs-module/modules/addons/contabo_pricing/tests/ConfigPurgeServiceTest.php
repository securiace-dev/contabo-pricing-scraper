<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigPurgeService;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * A.6.5 — ConfigPurgeService (§19). Proves it deletes ONLY the WHMCS config
 * objects recorded in the link tables and leaves everything else (incl.
 * non-addon config objects) untouched. FakeCapsule, no DB.
 */
final class ConfigPurgeServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function seed(): void
    {
        // Addon-created objects (recorded in the link tables).
        Capsule::table('tblproductconfiggroups')->insert(['id' => 10, 'name' => 'Contabo']);
        Capsule::table('tblproductconfiglinks')->insert(['id' => 1, 'gid' => 10, 'pid' => 500]);
        Capsule::table('tblproductconfigoptions')->insert(['id' => 20, 'gid' => 10, 'optionname' => 'Image']);
        Capsule::table('tblproductconfigoptionssub')->insert(['id' => 30, 'configid' => 20, 'optionname' => 'Ubuntu']);
        Capsule::table('tblpricing')->insert(['id' => 40, 'type' => 'configoptions', 'relid' => 30, 'currency' => 1, 'monthly' => 5.0]);

        // A NON-addon config object (NOT in any link table) — must survive.
        Capsule::table('tblproductconfiggroups')->insert(['id' => 11, 'name' => 'Someone Else']);
        Capsule::table('tblproductconfigoptions')->insert(['id' => 21, 'gid' => 11, 'optionname' => 'Other']);
        Capsule::table('tblproductconfigoptionssub')->insert(['id' => 31, 'configid' => 21, 'optionname' => 'X']);
        Capsule::table('tblpricing')->insert(['id' => 41, 'type' => 'configoptions', 'relid' => 31, 'currency' => 1, 'monthly' => 9.0]);

        // Link tables recording ONLY the addon's ids.
        Capsule::table('mod_contabo_config_group_link')->insert(['id' => 1, 'profile_id' => 7, 'whmcs_product_id' => 500, 'group_key' => 'g', 'whmcs_group_id' => 10]);
        Capsule::table('mod_contabo_config_option_link')->insert(['id' => 1, 'profile_id' => 7, 'dimension_key' => 'Image', 'whmcs_option_id' => 20]);
        Capsule::table('mod_contabo_config_option_value_link')->insert(['id' => 1, 'option_link_id' => 1, 'contabo_value_key' => 'os:ubuntu', 'whmcs_sub_id' => 30]);
    }

    public function testRemovesOnlyAddonCreatedObjects(): void
    {
        $this->seed();
        $counts = (new ConfigPurgeService())->removeAddonCreatedWhmcsObjects();

        $this->assertSame(1, $counts['groups']);
        $this->assertSame(1, $counts['options']);
        $this->assertSame(1, $counts['subs']);
        $this->assertSame(1, $counts['sub_pricing']);
        $this->assertSame(1, $counts['product_links']);

        // Addon objects gone.
        $this->assertNull(Capsule::table('tblproductconfiggroups')->where('id', 10)->first());
        $this->assertNull(Capsule::table('tblproductconfigoptions')->where('id', 20)->first());
        $this->assertNull(Capsule::table('tblproductconfigoptionssub')->where('id', 30)->first());
        $this->assertNull(Capsule::table('tblpricing')->where('id', 40)->first());

        // Non-addon objects survive — the ownership scope held.
        $this->assertNotNull(Capsule::table('tblproductconfiggroups')->where('id', 11)->first());
        $this->assertNotNull(Capsule::table('tblproductconfigoptions')->where('id', 21)->first());
        $this->assertNotNull(Capsule::table('tblproductconfigoptionssub')->where('id', 31)->first());
        $this->assertNotNull(Capsule::table('tblpricing')->where('id', 41)->first());
    }

    public function testIdempotentSecondRunDeletesNothing(): void
    {
        $this->seed();
        (new ConfigPurgeService())->removeAddonCreatedWhmcsObjects();
        // Link tables still hold the (now-stale) ids; a 2nd run finds the WHMCS
        // objects already gone → all counts 0, no error.
        $counts = (new ConfigPurgeService())->removeAddonCreatedWhmcsObjects();
        $this->assertSame(['subs' => 0, 'sub_pricing' => 0, 'options' => 0, 'product_links' => 0, 'groups' => 0], $counts);
    }

    public function testNoLinksDeletesNothing(): void
    {
        Capsule::table('tblproductconfiggroups')->insert(['id' => 11, 'name' => 'Someone Else']);
        $counts = (new ConfigPurgeService())->removeAddonCreatedWhmcsObjects();
        $this->assertSame(0, array_sum($counts));
        $this->assertNotNull(Capsule::table('tblproductconfiggroups')->where('id', 11)->first());
    }

    public function testPreviewRemovalCountsButDeletesNothing(): void
    {
        $this->seed();
        $svc = new ConfigPurgeService();

        $preview = $svc->previewRemoval();
        // Same counts the real purge would produce…
        $this->assertSame(['subs' => 1, 'sub_pricing' => 1, 'options' => 1, 'product_links' => 1, 'groups' => 1], $preview);

        // …but NOTHING was deleted — every addon object is still present.
        $this->assertNotNull(Capsule::table('tblproductconfiggroups')->where('id', 10)->first());
        $this->assertNotNull(Capsule::table('tblproductconfigoptions')->where('id', 20)->first());
        $this->assertNotNull(Capsule::table('tblproductconfigoptionssub')->where('id', 30)->first());
        $this->assertNotNull(Capsule::table('tblpricing')->where('id', 40)->first());
    }

    public function testPreviewMatchesActualRemovalCounts(): void
    {
        $this->seed();
        $preview = (new ConfigPurgeService())->previewRemoval();
        $actual  = (new ConfigPurgeService())->removeAddonCreatedWhmcsObjects();
        $this->assertSame($preview, $actual, 'dry-run counts must equal the real purge counts on the same fixture');
    }

    public function testPreviewWithNoLinksIsAllZeros(): void
    {
        Capsule::table('tblproductconfiggroups')->insert(['id' => 11, 'name' => 'Someone Else']);
        $preview = (new ConfigPurgeService())->previewRemoval();
        $this->assertSame(['subs' => 0, 'sub_pricing' => 0, 'options' => 0, 'product_links' => 0, 'groups' => 0], $preview);
    }
}
