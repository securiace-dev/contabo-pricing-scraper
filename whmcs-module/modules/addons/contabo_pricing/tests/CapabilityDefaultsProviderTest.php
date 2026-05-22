<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CapabilityDefaultsProvider;
use ContaboPricing\ConfigOptionCapabilityRepository;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * A.5.2 amendment #6 — CapabilityDefaultsProvider: the §4 default capability
 * classification per normalised dimension, and the seeder that fills the empty
 * mod_contabo_option_capability table through the repository. Runs against
 * FakeCapsule (no DB).
 */
final class CapabilityDefaultsProviderTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testImageDefaultsAreDestructiveReinstall(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Image');

        $this->assertSame(1, (int) $d['requires_reinstall']);
        $this->assertSame(1, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['data_loss_expected']);
        $this->assertSame(1, (int) $d['requires_backup_warning']);
        $this->assertSame(1, (int) $d['requires_admin_approval']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame(1, (int) $d['allowed_on_reinstall']);
        $this->assertSame(1, (int) $d['allowed_on_post_provision']);
        $this->assertSame('reinstall', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testRegionDefaultsAreDestructiveRecreate(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Region');

        $this->assertSame(1, (int) $d['requires_recreate']);
        $this->assertSame(1, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['data_loss_expected']);
        $this->assertSame(1, (int) $d['requires_backup_warning']);
        $this->assertSame(1, (int) $d['requires_admin_approval']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame('recreate', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testStorageTypeDefaultsAssumeDestructiveReinstall(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Storage Type');

        $this->assertSame(1, (int) $d['requires_reinstall']);
        $this->assertSame(1, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['data_loss_expected']);
        $this->assertSame(1, (int) $d['requires_admin_approval']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame('reinstall', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testDataProtectionDefaultsAreInPlaceSafe(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Data Protection');

        $this->assertSame(0, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame(1, (int) $d['allowed_on_post_provision']);
        $this->assertSame(1, (int) $d['allowed_on_upgrade']);
        $this->assertSame(1, (int) $d['allowed_on_downgrade']);
        $this->assertSame(1, (int) $d['billing_change_possible']);
        $this->assertSame('toggle_backup', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testIpv4DefaultsAreInPlaceSafe(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Networking:IPv4');

        $this->assertSame(0, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame(1, (int) $d['allowed_on_post_provision']);
        $this->assertSame(1, (int) $d['allowed_on_upgrade']);
        $this->assertSame(1, (int) $d['allowed_on_downgrade']);
        $this->assertSame(1, (int) $d['billing_change_possible']);
        $this->assertSame('adjust_ipv4', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testBandwidthDefaultsAreInPlaceWithBillingChange(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Networking:Bandwidth');

        $this->assertSame(0, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame(1, (int) $d['allowed_on_post_provision']);
        $this->assertSame(1, (int) $d['billing_change_possible']);
        $this->assertSame('adjust_bandwidth', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testPrivateNetworkingDefaultsAreInPlaceSafe(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Networking:Private Networking');

        $this->assertSame(0, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertSame(1, (int) $d['allowed_on_post_provision']);
        $this->assertSame('toggle_private_net', (string) $d['provisioning_action']);
        $this->assertManualAssumption($d);
    }

    public function testUnknownDimensionFallsBackToConservativeDefault(): void
    {
        $d = (new CapabilityDefaultsProvider())->defaultsFor('Some Future Dimension');

        $this->assertSame(1, (int) $d['requires_admin_approval']);
        $this->assertSame(1, (int) $d['destructive_change']);
        $this->assertSame(1, (int) $d['allowed_on_create']);
        $this->assertManualAssumption($d);
    }

    public function testSeedForPlanUpsertsOneRowPerSpecValueWithDefaults(): void
    {
        $provider = new CapabilityDefaultsProvider();
        $repo     = new ConfigOptionCapabilityRepository();

        // DimensionParser-shaped specs: Image (one value), Data Protection (two
        // values), Networking:IPv4 (one value) → 4 capability rows.
        $specs = [
            [
                'dimension_key' => 'Image',
                'values'        => [
                    ['value_key' => 'OS:Ubuntu 24.04'],
                ],
            ],
            [
                'dimension_key' => 'Data Protection',
                'values'        => [
                    ['value_key' => 'Backup:Auto Backup'],
                    ['value_key' => 'Backup:None'],
                ],
            ],
            [
                'dimension_key' => 'Networking:IPv4',
                'values'        => [
                    ['value_key' => 'IPv4:1 additional'],
                ],
            ],
        ];

        $n = $provider->seedForPlan('contabo-cloud-vps-10', $specs, $repo);

        $this->assertSame(4, $n, 'one capability row per spec value');
        $this->assertCount(4, Capsule::$tables['mod_contabo_option_capability']);

        // Destructive Image row landed with the right flags + manual_assumption.
        $image = $repo->find('contabo-cloud-vps-10', 'Image', 'OS:Ubuntu 24.04');
        $this->assertNotNull($image);
        $this->assertSame(1, (int) $image['destructive_change']);
        $this->assertSame(1, (int) $image['requires_reinstall']);
        $this->assertSame('reinstall', (string) $image['provisioning_action']);
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION,
            (string) $image['capability_source']
        );

        // In-place safe Data Protection row.
        $backup = $repo->find('contabo-cloud-vps-10', 'Data Protection', 'Backup:Auto Backup');
        $this->assertNotNull($backup);
        $this->assertSame(0, (int) $backup['destructive_change']);
        $this->assertSame('toggle_backup', (string) $backup['provisioning_action']);
    }

    public function testSeededDestructiveImageRowCannotAutoApply(): void
    {
        $provider = new CapabilityDefaultsProvider();
        $repo     = new ConfigOptionCapabilityRepository();

        $specs = [
            [
                'dimension_key' => 'Image',
                'values'        => [['value_key' => 'OS:Ubuntu 24.04']],
            ],
        ];
        $provider->seedForPlan('plan-a', $specs, $repo);

        $row = $repo->find('plan-a', 'Image', 'OS:Ubuntu 24.04');
        $this->assertNotNull($row);
        // manual_assumption + destructive change ⇒ amendment-6 gate denies
        // auto-apply (needs admin approval until promoted to api_verified).
        $this->assertFalse($repo->canAutoApply($row, true));
    }

    public function testSeedForPlanIsIdempotentBySameKeys(): void
    {
        $provider = new CapabilityDefaultsProvider();
        $repo     = new ConfigOptionCapabilityRepository();

        $specs = [
            [
                'dimension_key' => 'Networking:IPv4',
                'values'        => [['value_key' => 'IPv4:1 additional']],
            ],
        ];

        $provider->seedForPlan('plan-a', $specs, $repo);
        $provider->seedForPlan('plan-a', $specs, $repo); // re-seed, same keys

        // Re-seeding the same (plan, dimension, value) updates in place — no dupes.
        $this->assertCount(1, Capsule::$tables['mod_contabo_option_capability']);
    }

    public function testSeedForPlanSkipsMalformedSpecsAndValues(): void
    {
        $provider = new CapabilityDefaultsProvider();
        $repo     = new ConfigOptionCapabilityRepository();

        $specs = [
            ['no_dimension_key' => true],                 // skipped: no dimension_key
            ['dimension_key' => 'Region', 'values' => []], // no values → nothing upserted
            [
                'dimension_key' => 'Region',
                'values'        => [
                    ['value_key' => 'America:US Central'],
                    ['no_value_key' => true],            // skipped: no value_key
                ],
            ],
        ];

        $n = $provider->seedForPlan('plan-a', $specs, $repo);
        $this->assertSame(1, $n);
        $this->assertCount(1, Capsule::$tables['mod_contabo_option_capability']);
    }

    /** @param array<string,mixed> $defaults */
    private function assertManualAssumption(array $defaults): void
    {
        $this->assertSame(
            ConfigOptionCapabilityRepository::SOURCE_MANUAL_ASSUMPTION,
            (string) $defaults['capability_source'],
            'every default capability is a manual assumption until api-verified'
        );
    }
}
