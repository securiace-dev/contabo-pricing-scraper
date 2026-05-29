<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ProfileManager;
use ContaboPricing\ProfilePurgeService;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase D §6 — per-profile delete / restore / guarded purge.
 *
 *   - softDelete stamps deleted_at; listProfiles excludes it; restore clears it.
 *   - purge is BLOCKED while an active mapping or a live service references the
 *     profile, and ALLOWED once neither does.
 *   - purge cascades ONLY the target profile's rows + its addon-created WHMCS
 *     config objects, leaving a sibling profile untouched.
 */
final class ProfilePurgeServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    private function settings(): Settings
    {
        return new Settings('http://x', '', 'manual', 'INR', true, 0.0, 365, '');
    }

    // ── soft-delete lifecycle ───────────────────────────────────────────────

    public function testSoftDeleteHidesFromListAndRestoreBringsBack(): void
    {
        Capsule::$tables['mod_contabo_profile'] = [
            ['id' => 1, 'slug' => 'a', 'name' => 'A', 'plan_slug' => 'p', 'period_months' => 1, 'active' => 1],
            ['id' => 2, 'slug' => 'b', 'name' => 'B', 'plan_slug' => 'p', 'period_months' => 1, 'active' => 1],
        ];
        $pm = new ProfileManager($this->settings());

        $this->assertCount(2, $pm->listProfiles(false));

        $pm->softDelete(1);
        $live = $pm->listProfiles(false);
        $this->assertCount(1, $live);
        $this->assertSame(2, (int) $live[0]['id']);
        $this->assertCount(1, $pm->listTrashed());
        $this->assertSame(1, (int) $pm->listTrashed()[0]['id']);

        $pm->restore(1);
        $this->assertCount(2, $pm->listProfiles(false));
        $this->assertCount(0, $pm->listTrashed());
    }

    public function testListProfilesActiveOnlyStillExcludesTrashed(): void
    {
        Capsule::$tables['mod_contabo_profile'] = [
            ['id' => 1, 'slug' => 'a', 'active' => 1, 'plan_slug' => 'p', 'period_months' => 1, 'deleted_at' => '2026-05-29 00:00:00'],
            ['id' => 2, 'slug' => 'b', 'active' => 1, 'plan_slug' => 'p', 'period_months' => 1],
        ];
        $pm = new ProfileManager($this->settings());
        $rows = $pm->listProfiles(true);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    // ── purge guard ─────────────────────────────────────────────────────────

    public function testPurgeBlockedByActiveMapping(): void
    {
        Capsule::$tables['mod_contabo_profile'] = [['id' => 1, 'slug' => 'a']];
        Capsule::$tables['mod_contabo_mapping'] = [
            ['id' => 9, 'profile_id' => 1, 'product_id' => 100, 'active' => 1],
        ];

        $assess = (new ProfilePurgeService())->assess(1);
        $this->assertFalse($assess['allowed']);
        $this->assertSame(1, $assess['active_mappings']);
        $this->assertNotEmpty($assess['reasons']);
    }

    public function testPurgeBlockedByLiveService(): void
    {
        Capsule::$tables['mod_contabo_profile'] = [['id' => 1, 'slug' => 'a']];
        // Mapping inactive (so it doesn't itself block), but a live service exists.
        Capsule::$tables['mod_contabo_mapping'] = [
            ['id' => 9, 'profile_id' => 1, 'product_id' => 100, 'active' => 0],
        ];
        Capsule::$tables['tblhosting'] = [
            ['id' => 555, 'packageid' => 100, 'domainstatus' => 'Active'],
        ];

        $assess = (new ProfilePurgeService())->assess(1);
        $this->assertFalse($assess['allowed']);
        $this->assertSame(0, $assess['active_mappings']);
        $this->assertSame(1, $assess['live_services']);
    }

    public function testPurgeAllowedWhenNoActiveMappingOrLiveService(): void
    {
        Capsule::$tables['mod_contabo_profile'] = [['id' => 1, 'slug' => 'a']];
        Capsule::$tables['mod_contabo_mapping'] = [
            ['id' => 9, 'profile_id' => 1, 'product_id' => 100, 'active' => 0],
        ];
        Capsule::$tables['tblhosting'] = [
            ['id' => 555, 'packageid' => 100, 'domainstatus' => 'Terminated'],
        ];

        $assess = (new ProfilePurgeService())->assess(1);
        $this->assertTrue($assess['allowed']);
        $this->assertSame([], $assess['reasons']);
    }

    public function testPurgeThrowsWhenGuardBlocks(): void
    {
        Capsule::$tables['mod_contabo_profile'] = [['id' => 1, 'slug' => 'a']];
        Capsule::$tables['mod_contabo_mapping'] = [
            ['id' => 9, 'profile_id' => 1, 'product_id' => 100, 'active' => 1],
        ];

        $this->expectException(\RuntimeException::class);
        (new ProfilePurgeService())->purge(1);
    }

    // ── scoped cascade ──────────────────────────────────────────────────────

    public function testPurgeCascadesOnlyTargetProfile(): void
    {
        // Two profiles. Profile 1 will be purged; profile 2 must be untouched.
        Capsule::$tables['mod_contabo_profile'] = [
            ['id' => 1, 'slug' => 'a'],
            ['id' => 2, 'slug' => 'b'],
        ];
        Capsule::$tables['mod_contabo_mapping'] = [
            ['id' => 11, 'profile_id' => 1, 'product_id' => 100, 'active' => 0],
            ['id' => 12, 'profile_id' => 2, 'product_id' => 200, 'active' => 1],
        ];
        Capsule::$tables['mod_contabo_profile_version'] = [
            ['id' => 71, 'profile_id' => 1],
            ['id' => 72, 'profile_id' => 1],
            ['id' => 73, 'profile_id' => 2],
        ];
        Capsule::$tables['mod_contabo_config_group_link'] = [
            ['id' => 1, 'profile_id' => 1, 'whmcs_group_id' => 900],
            ['id' => 2, 'profile_id' => 2, 'whmcs_group_id' => 901],
        ];
        Capsule::$tables['mod_contabo_config_option_link'] = [
            ['id' => 31, 'profile_id' => 1, 'whmcs_option_id' => 800],
            ['id' => 32, 'profile_id' => 2, 'whmcs_option_id' => 801],
        ];
        Capsule::$tables['mod_contabo_config_option_value_link'] = [
            ['id' => 41, 'option_link_id' => 31, 'whmcs_sub_id' => 700],
            ['id' => 42, 'option_link_id' => 32, 'whmcs_sub_id' => 701],
        ];
        Capsule::$tables['mod_contabo_config_option_audit'] = [
            ['id' => 1, 'profile_id' => 1],
            ['id' => 2, 'profile_id' => 2],
        ];
        // WHMCS objects: only profile 1's (group 900, option 800, sub 700) go.
        Capsule::$tables['tblproductconfiggroups'] = [['id' => 900], ['id' => 901]];
        Capsule::$tables['tblproductconfiglinks'] = [['gid' => 900], ['gid' => 901]];
        Capsule::$tables['tblproductconfigoptions'] = [['id' => 800], ['id' => 801]];
        Capsule::$tables['tblproductconfigoptionssub'] = [['id' => 700], ['id' => 701]];
        Capsule::$tables['tblpricing'] = [
            ['type' => 'configoptions', 'relid' => 700],
            ['type' => 'configoptions', 'relid' => 701],
        ];

        $counts = (new ProfilePurgeService())->purge(1);

        // Profile 1 fully gone.
        $this->assertSame(1, $counts['profile']);
        $this->assertSame(1, $counts['mappings']);
        $this->assertSame(2, $counts['versions']);
        $this->assertSame(1, $counts['groups']);
        $this->assertSame(1, $counts['options']);
        $this->assertSame(1, $counts['subs']);
        $this->assertSame(1, $counts['sub_pricing']);
        $this->assertSame(1, $counts['product_links']);

        // Profile 2 and all its rows untouched.
        $this->assertCount(1, Capsule::$tables['mod_contabo_profile']);
        $this->assertSame(2, (int) Capsule::$tables['mod_contabo_profile'][0]['id']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_mapping']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_profile_version']);
        $this->assertCount(1, Capsule::$tables['tblproductconfiggroups']);
        $this->assertSame(901, (int) Capsule::$tables['tblproductconfiggroups'][0]['id']);
        $this->assertCount(1, Capsule::$tables['tblproductconfigoptionssub']);
        $this->assertSame(701, (int) Capsule::$tables['tblproductconfigoptionssub'][0]['id']);
    }
}
