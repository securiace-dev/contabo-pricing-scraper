<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ServiceRevenueResolver;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.6.1 — ServiceRevenueResolver (amendment 5, read-only scaffold).
 *
 * Proves the resolver sums base + selected config-option recurrings + addons
 * into a breakdown (not bare recurringamount), selects the right cycle column,
 * multiplies by quantity, and that resolveFromSnapshot reads the snapshot
 * fields. ServiceRevenueResolver is `final`, so these drive the real Capsule
 * read path: seed tblhosting / tblhostingconfigoptions / tblpricing /
 * tblhostingaddons and assert the resolved breakdown.
 */
final class ServiceRevenueResolverTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /**
     * Seed a service with its base, selected config options (with prices), and
     * addons. Each $configOptions entry: [sub_id, qty, prices(cycle=>float)].
     * Each $addons entry: [id, name, recurring].
     *
     * @param array{base:float,billingcycle:string} $base  base = the product's
     *        catalog recurring price for the cycle (NOT recurringamount, which
     *        is the stale stored charge — the resolver derives base from
     *        tblpricing type=product).
     * @param list<array{sub_id:int,qty:int,prices:array<string,float>}> $configOptions
     * @param list<array{id:int,name:string,recurring:float}> $addons
     */
    private function seedService(int $serviceId, array $base, array $configOptions, array $addons): void
    {
        $packageId = 7000 + $serviceId;
        Capsule::table('tblhosting')->insert([
            'id'              => $serviceId,
            'packageid'       => $packageId,
            // recurringamount is the stale stored charge → exposed as current_charge.
            'recurringamount' => $base['base'],
            'billingcycle'    => $base['billingcycle'],
        ]);

        // Product catalog price (the real base source). Set the service's cycle
        // column to the requested base.
        $cycleCol = [
            'monthly' => 'monthly', 'quarterly' => 'quarterly', 'semi-annually' => 'semiannually',
            'semiannually' => 'semiannually', 'annually' => 'annually', 'biennially' => 'biennially',
            'triennially' => 'triennially',
        ][strtolower($base['billingcycle'])] ?? 'monthly';
        $productPrice = [
            'type' => 'product', 'relid' => $packageId, 'currency' => 1,
            'monthly' => 0.0, 'quarterly' => 0.0, 'semiannually' => 0.0,
            'annually' => 0.0, 'biennially' => 0.0, 'triennially' => 0.0,
        ];
        $productPrice[$cycleCol] = $base['base'];
        Capsule::table('tblpricing')->insert($productPrice);

        foreach ($configOptions as $co) {
            // tblhostingconfigoptions: relid=service, optionid=sub-option id.
            Capsule::table('tblhostingconfigoptions')->insert([
                'relid'    => $serviceId,
                'configid' => 1,
                'optionid' => $co['sub_id'],
                'qty'      => $co['qty'],
            ]);

            $price = [
                'type'         => 'configoptions',
                'relid'        => $co['sub_id'],
                'currency'     => 1, // INR
                'monthly'      => 0.0,
                'quarterly'    => 0.0,
                'semiannually' => 0.0,
                'annually'     => 0.0,
                'biennially'   => 0.0,
                'triennially'  => 0.0,
            ];
            foreach ($co['prices'] as $cycle => $amount) {
                $price[$cycle] = $amount;
            }
            Capsule::table('tblpricing')->insert($price);
        }

        foreach ($addons as $addon) {
            Capsule::table('tblhostingaddons')->insert([
                'id'        => $addon['id'],
                'hostingid' => $serviceId,
                'name'      => $addon['name'],
                'recurring' => $addon['recurring'],
            ]);
        }
    }

    public function testResolveForServiceSumsBaseConfigAndAddons(): void
    {
        $this->seedService(1001,
            ['base' => 515.0, 'billingcycle' => 'monthly'],
            [
                ['sub_id' => 1, 'qty' => 1, 'prices' => ['monthly' => 191.0, 'annually' => 2292.0]], // Auto Backup
                ['sub_id' => 2, 'qty' => 1, 'prices' => ['monthly' => 121.0, 'annually' => 1452.0]], // US-Central region
            ],
            [['id' => 9, 'name' => 'Managed Support', 'recurring' => 300.0]]
        );

        $resolver = new ServiceRevenueResolver();
        $r = $resolver->resolveForService(1001);

        $this->assertSame(515.0, $r['base']);
        $this->assertSame(312.0, $r['config_options']); // 191 + 121
        $this->assertSame(300.0, $r['addons']);
        $this->assertSame(1127.0, $r['total']);          // base+config+addons
        $this->assertSame($r['base'] + $r['config_options'] + $r['addons'], $r['total']);
    }

    public function testTotalEqualsSumOfParts(): void
    {
        $this->seedService(1,
            ['base' => 100.0, 'billingcycle' => 'monthly'],
            [['sub_id' => 1, 'qty' => 1, 'prices' => ['monthly' => 25.0]]],
            [['id' => 1, 'name' => 'A', 'recurring' => 10.0]]
        );
        $r = (new ServiceRevenueResolver())->resolveForService(1);
        $this->assertEqualsWithDelta($r['base'] + $r['config_options'] + $r['addons'], $r['total'], 0.0001);
        $this->assertSame(135.0, $r['total']);
    }

    public function testConfigOptionQuantityMultipliesUnitPrice(): void
    {
        // IPv4 unit price 121/mo × qty 3 = 363.
        $this->seedService(1,
            ['base' => 515.0, 'billingcycle' => 'monthly'],
            [['sub_id' => 7, 'qty' => 3, 'prices' => ['monthly' => 121.0]]],
            []
        );
        $r = (new ServiceRevenueResolver())->resolveForService(1);
        $this->assertSame(363.0, $r['config_options']);
        $this->assertSame(878.0, $r['total']);
    }

    public function testCycleColumnSelectionForAnnualService(): void
    {
        // Annual service must pull the `annually` column, not `monthly`.
        $this->seedService(1,
            ['base' => 6180.0, 'billingcycle' => 'annually'],
            [['sub_id' => 1, 'qty' => 1, 'prices' => ['monthly' => 191.0, 'annually' => 2292.0]]],
            []
        );
        $r = (new ServiceRevenueResolver())->resolveForService(1);
        $this->assertSame(2292.0, $r['config_options']);
        $this->assertSame('annually', $r['breakdown']['cycle_column']);
    }

    public function testBreakdownContainsPerLineDetail(): void
    {
        $this->seedService(42,
            ['base' => 515.0, 'billingcycle' => 'monthly'],
            [['sub_id' => 5, 'qty' => 2, 'prices' => ['monthly' => 50.0]]],
            [['id' => 3, 'name' => 'Monitoring', 'recurring' => 75.0]]
        );
        $r = (new ServiceRevenueResolver())->resolveForService(42);

        $this->assertSame('service', $r['breakdown']['source']);
        $this->assertSame(42, $r['breakdown']['service_id']);
        $this->assertCount(1, $r['breakdown']['config_options']);
        $this->assertSame(5, $r['breakdown']['config_options'][0]['sub_id']);
        $this->assertSame(2, $r['breakdown']['config_options'][0]['qty']);
        $this->assertSame(100.0, $r['breakdown']['config_options'][0]['line']);
        $this->assertCount(1, $r['breakdown']['addons']);
        $this->assertSame('Monitoring', $r['breakdown']['addons'][0]['name']);
    }

    public function testResolveFromSnapshotSumsSnapshotFields(): void
    {
        $r = (new ServiceRevenueResolver())->resolveFromSnapshot([
            'id'                           => 77,
            'service_id'                   => 1001,
            'base_price_snapshot'          => 515.0,
            'config_option_price_snapshot' => 312.0,
            'addon_price_snapshot'         => 300.0,
            'pricing_version_snapshot'     => 'v0.5.0',
        ]);

        $this->assertSame(515.0, $r['base']);
        $this->assertSame(312.0, $r['config_options']);
        $this->assertSame(300.0, $r['addons']);
        $this->assertSame(1127.0, $r['total']);
        $this->assertSame('snapshot', $r['breakdown']['source']);
        $this->assertSame(77, $r['breakdown']['snapshot_id']);
        $this->assertSame('v0.5.0', $r['breakdown']['pricing_version_snapshot']);
    }

    public function testResolveFromSnapshotDefaultsAddonToZero(): void
    {
        // addon_price_snapshot is optional in v5; absence → 0.0.
        $r = (new ServiceRevenueResolver())->resolveFromSnapshot([
            'service_id'                   => 1,
            'base_price_snapshot'          => 200.0,
            'config_option_price_snapshot' => 50.0,
        ]);
        $this->assertSame(0.0, $r['addons']);
        $this->assertSame(250.0, $r['total']);
    }

    public function testRevenueIsNotBareRecurringAmount(): void
    {
        // The whole point of amendment 5: total > base when options/addons exist.
        $this->seedService(1,
            ['base' => 515.0, 'billingcycle' => 'monthly'],
            [['sub_id' => 1, 'qty' => 1, 'prices' => ['monthly' => 191.0]]],
            [['id' => 1, 'name' => 'X', 'recurring' => 100.0]]
        );
        $r = (new ServiceRevenueResolver())->resolveForService(1);
        $this->assertGreaterThan($r['base'], $r['total']);
        $this->assertNotSame($r['base'], $r['total']);
    }

    public function testServiceWithNoOptionsOrAddonsEqualsBase(): void
    {
        $this->seedService(1,
            ['base' => 515.0, 'billingcycle' => 'monthly'],
            [],
            []
        );
        $r = (new ServiceRevenueResolver())->resolveForService(1);
        $this->assertSame(515.0, $r['base']);
        $this->assertSame(0.0, $r['config_options']);
        $this->assertSame(0.0, $r['addons']);
        $this->assertSame(515.0, $r['total']);
    }

    public function testMissingServiceResolvesToZero(): void
    {
        // Unknown service id → empty base; total 0, no fatal.
        $r = (new ServiceRevenueResolver())->resolveForService(999999);
        $this->assertSame(0.0, $r['base']);
        $this->assertSame(0.0, $r['total']);
    }

    public function testCycleColumnHelperMapsKnownAndUnknown(): void
    {
        $resolver = new ServiceRevenueResolver();
        $this->assertSame('semiannually', $resolver->cycleColumn('Semi-Annually'));
        $this->assertSame('triennially', $resolver->cycleColumn('triennially'));
        $this->assertSame('monthly', $resolver->cycleColumn('made-up-cycle'));
    }
}
