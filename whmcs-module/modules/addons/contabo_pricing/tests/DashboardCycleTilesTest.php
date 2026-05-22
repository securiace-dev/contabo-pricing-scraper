<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\CycleSet;
use ContaboPricing\Settings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5 — repricing dashboard cycle tiles.
 *
 * Acceptance: "Cycle exposure" tile counts services whose billingcycle is a
 * recurring cycle but is NOT bit-set in the mapping's renewal_cycles_mask
 * (would skip with `cycle_not_mapped`).
 *
 * Fixture:
 *   - 1 mapping for product #100, renewal_cycles_mask = bit Annually (0b001000 = 8)
 *   - 5 tblhosting services on product #100 with billingcycle=Annually  → covered
 *   - 3 tblhosting services on product #100 with billingcycle=Quarterly → exposed
 *
 *  expected cycle_exposure = 3
 *  expected cycle_breakdown = ['Annually' => 5, 'Quarterly' => 3, …]
 */
final class DashboardCycleTilesTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        // Mark the tables as existing so the controller's hasTable() guards pass.
        Capsule::$columns['tblhosting'] = ['id', 'packageid', 'billingcycle'];
        Capsule::$columns['mod_contabo_mapping'] = [
            'id', 'profile_id', 'product_id', 'active',
            'catalog_cycles_mask', 'renewal_cycles_mask',
            'apply_to_monthly', 'apply_to_semiannually', 'apply_to_annually',
        ];
    }

    public function testCycleExposureCountsServicesNotInRenewalMask(): void
    {
        $annuallyMask  = 1 << CycleSet::BIT_ANNUALLY;   // 0b001000 = 8
        $quarterlyMask = 1 << CycleSet::BIT_QUARTERLY;  // 0b000010 = 2

        Capsule::$tables['mod_contabo_mapping'] = [
            [
                'id' => 1, 'profile_id' => 1, 'product_id' => 100,
                'active' => true,
                'catalog_cycles_mask' => $annuallyMask,
                'renewal_cycles_mask' => $annuallyMask, // only Annually
            ],
        ];

        $svcs = [];
        for ($i = 1; $i <= 5; $i++) {
            $svcs[] = ['id' => 1000 + $i, 'packageid' => 100, 'billingcycle' => 'Annually'];
        }
        for ($i = 1; $i <= 3; $i++) {
            $svcs[] = ['id' => 2000 + $i, 'packageid' => 100, 'billingcycle' => 'Quarterly'];
        }
        Capsule::$tables['tblhosting'] = $svcs;

        $controller = new AdminController(
            new Settings(
                'http://localhost:8080/api/v1', '', 'notify', 'INR',
                false, 3.5, 365, 'addonmodules.php?module=contabo_pricing'
            ),
            __DIR__ . '/../templates/admin'
        );

        $ref = new \ReflectionClass(AdminController::class);
        $m = $ref->getMethod('computeCycleStats');
        if (PHP_VERSION_ID < 80100) {
            $m->setAccessible(true); // no-op on 8.1+, but required on 7.4 / 8.0
        }
        $stats = $m->invoke($controller);

        // Breakdown reflects ONLY services whose product appears in
        // mod_contabo_mapping. Every recurring cycle key is present, even at zero.
        $this->assertArrayHasKey('breakdown', $stats);
        $this->assertSame(5, (int) $stats['breakdown']['Annually']);
        $this->assertSame(3, (int) $stats['breakdown']['Quarterly']);
        $this->assertSame(0, (int) $stats['breakdown']['Monthly']);
        $this->assertSame(0, (int) $stats['breakdown']['Semi-Annually']);
        $this->assertSame(0, (int) $stats['breakdown']['Biennially']);
        $this->assertSame(0, (int) $stats['breakdown']['Triennially']);

        // Exposure = 3 (Quarterly services not in mask) + 0 (Annually services
        // are covered) = 3.
        $this->assertArrayHasKey('exposure_count', $stats);
        $this->assertSame(3, (int) $stats['exposure_count']);

        // The exposed-service-id list contains all three Quarterly service ids.
        $exposed = $stats['exposure_services'];
        $this->assertCount(3, $exposed);
        sort($exposed);
        $this->assertSame([2001, 2002, 2003], $exposed);
    }

    public function testNoMappingsYieldsZeroExposure(): void
    {
        // No mappings → exposure is zero regardless of how many services exist.
        Capsule::$tables['mod_contabo_mapping'] = [];
        Capsule::$tables['tblhosting'] = [
            ['id' => 1, 'packageid' => 100, 'billingcycle' => 'Annually'],
            ['id' => 2, 'packageid' => 100, 'billingcycle' => 'Quarterly'],
        ];

        $controller = new AdminController(
            new Settings(
                'http://localhost:8080/api/v1', '', 'notify', 'INR',
                false, 3.5, 365, 'addonmodules.php?module=contabo_pricing'
            ),
            __DIR__ . '/../templates/admin'
        );
        $ref = new \ReflectionClass(AdminController::class);
        $m = $ref->getMethod('computeCycleStats');
        if (PHP_VERSION_ID < 80100) {
            $m->setAccessible(true); // no-op on 8.1+, but required on 7.4 / 8.0
        }
        $stats = $m->invoke($controller);

        $this->assertSame(0, (int) $stats['exposure_count']);
        foreach ($stats['breakdown'] as $n) {
            $this->assertSame(0, (int) $n);
        }
    }
}
