<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ConfigOptionCapabilityRepository;
use ContaboPricing\ConfigOptionCompatibilityRepository;
use ContaboPricing\ProfileIdentityResolver;
use ContaboPricing\ProfileManager;
use ContaboPricing\ProfileRepository;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Covers the v0.6.x profile-form wiring shipped with the A.6.3 admin UI:
 *   - profile_mode + expose_configurable_options round-trip on create,
 *   - ProfileManager::update() column whitelist (mass-assignment guard),
 *   - the capability + compatibility editor repository layer (listForPlan).
 *
 * PHP 7.4-compatible; runs against FakeCapsule (schemaless arrays).
 */
final class ProfileConfigWiringTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /** @return array<string,mixed> */
    private function baseInput(array $overrides = []): array
    {
        return array_merge([
            'name'          => 'CVPS 10 EU',
            'plan_slug'     => 'cloud-vps-10',
            'period_months' => 12,
            'region'        => 'EU',
            'os'            => 'Ubuntu 24.04',
            'sync_strategy' => 'notify',
        ], $overrides);
    }

    public function testProfileModeDefaultsToFixedAndExposeToOne(): void
    {
        $repo = new ProfileRepository('notify');
        $res  = $repo->createOrResolve($this->baseInput(['slug' => 'p-default']));
        $this->assertSame('created', $res['status']);

        $row = $repo->findBySlug('p-default');
        $this->assertNotNull($row);
        $this->assertSame(ProfileIdentityResolver::MODE_FIXED, $row['profile_mode']);
        $this->assertSame(1, (int) $row['expose_configurable_options']);
    }

    public function testConfigurableModeAndExposeZeroPersist(): void
    {
        $repo = new ProfileRepository('notify');
        $repo->createOrResolve($this->baseInput([
            'slug'                        => 'p-cfg',
            'profile_mode'                => ProfileIdentityResolver::MODE_CONFIGURABLE,
            'expose_configurable_options' => 0,
        ]));

        $row = $repo->findBySlug('p-cfg');
        $this->assertNotNull($row);
        $this->assertSame(ProfileIdentityResolver::MODE_CONFIGURABLE, $row['profile_mode']);
        $this->assertSame(0, (int) $row['expose_configurable_options']);
    }

    public function testUpdateWhitelistDropsUnknownColumns(): void
    {
        $repo = new ProfileRepository('notify');
        $repo->createOrResolve($this->baseInput(['slug' => 'p-upd']));
        $created = $repo->findBySlug('p-upd');
        $id = (int) $created['id'];

        $pm = new ProfileManager(Settings::fromVars([]));
        $pm->update($id, [
            'name'                        => 'Renamed',
            'expose_configurable_options' => 0,
            'profile_mode'                => ProfileIdentityResolver::MODE_CONFIGURABLE,
            'bogus_column'                => 'evil', // not whitelisted → must be dropped
            'id'                          => 999,    // not whitelisted → must be dropped
        ]);

        $row = $repo->findBySlug('p-upd');
        $this->assertSame('Renamed', $row['name']);
        $this->assertSame(0, (int) $row['expose_configurable_options']);
        $this->assertSame(ProfileIdentityResolver::MODE_CONFIGURABLE, $row['profile_mode']);
        $this->assertArrayNotHasKey('bogus_column', $row);
        $this->assertSame($id, (int) $row['id']); // id never reassigned
    }

    public function testCapabilityListForPlanRoundTrip(): void
    {
        $repo = new ConfigOptionCapabilityRepository();
        $repo->upsertCapability('cloud-vps-10', 'Image', 'os:ubuntu-24-04', [
            'destructive_change'      => 1,
            'requires_admin_approval' => 1,
            'provisioning_action'     => 'reinstall',
            'capability_source'       => ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED,
        ]);

        $rows = $repo->listForPlan('cloud-vps-10');
        $this->assertCount(1, $rows);
        $this->assertSame('Image', $rows[0]['dimension_key']);
        $this->assertSame(1, (int) $rows[0]['destructive_change']);
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_API_VERIFIED,
            $rows[0]['capability_source']
        );
    }

    public function testCompatibilityListForPlanAndValidate(): void
    {
        $repo = new ConfigOptionCompatibilityRepository();
        $repo->upsertRule('cloud-vps-10', 'Networking:IPv4', 'ipv4-extra', [
            'incompatible_with' => ['os:windows'],
            'required_values'   => [],
            'min_value'         => 0,
            'max_value'         => 5,
        ]);

        $rows = $repo->listForPlan('cloud-vps-10');
        $this->assertCount(1, $rows);
        $this->assertSame('Networking:IPv4', $rows[0]['dimension_key']);
        $this->assertSame(5, (int) $rows[0]['max_value']);

        // The saved rule must make windows + ipv4-extra an invalid combination.
        $res = $repo->validateCombination('cloud-vps-10', [
            ['dimension_key' => 'Networking:IPv4', 'value_key' => 'ipv4-extra'],
            ['dimension_key' => 'Image', 'value_key' => 'os:windows'],
        ]);
        $this->assertFalse($res['valid']);
        $this->assertNotEmpty($res['violations']);
    }
}
