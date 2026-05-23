<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CronDriver;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 0.5.1 parity — CronDriver observe path.
 *
 * Proves candidate loading reads the REAL tblhosting.`amount` column (the old
 * code filtered on the non-existent `recurringamount`, so it would have found
 * ZERO candidates), and that the sweep no longer instantiates RenewalEngine with
 * a broken constructor/signature (it ran to completion incl. pruning).
 */
final class CronDriverTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testLoadActiveMappedServiceIdsUsesAmountColumn(): void
    {
        Capsule::table('mod_contabo_mapping')->insert(['id' => 1, 'product_id' => 501, 'profile_id' => 1, 'active' => 1]);
        Capsule::table('mod_contabo_mapping')->insert(['id' => 2, 'product_id' => 7,   'profile_id' => 2, 'active' => 0]); // inactive mapping

        Capsule::table('tblhosting')->insert(['id' => 100, 'packageid' => 501, 'domainstatus' => 'Active',    'amount' => 500.0]); // ✓ included
        Capsule::table('tblhosting')->insert(['id' => 101, 'packageid' => 501, 'domainstatus' => 'Active',    'amount' => 0.0]);   // amount 0 → excluded
        Capsule::table('tblhosting')->insert(['id' => 102, 'packageid' => 501, 'domainstatus' => 'Suspended', 'amount' => 300.0]); // not Active → excluded
        Capsule::table('tblhosting')->insert(['id' => 103, 'packageid' => 7,   'domainstatus' => 'Active',    'amount' => 200.0]); // unmapped/inactive product → excluded

        $ids = (new CronDriver())->loadActiveMappedServiceIds();
        $this->assertSame([100], $ids); // old code filtering on recurringamount would return []
    }

    public function testObserveSweepCompletesAndPrunesWithoutTypeError(): void
    {
        // A mapped active service (so the old code would have entered the
        // RenewalEngine loop and TypeError'd). Plus an old non-applied decision
        // that must get pruned — proving the sweep ran PAST observeMappedServices.
        Capsule::table('mod_contabo_mapping')->insert(['id' => 1, 'product_id' => 501, 'profile_id' => 1, 'active' => 1]);
        Capsule::table('tblhosting')->insert(['id' => 100, 'packageid' => 501, 'domainstatus' => 'Active', 'amount' => 500.0]);
        Capsule::table('mod_contabo_price_decision')->insert([
            'id' => 1, 'applied' => false, 'decided_at' => '2000-01-01 00:00:00',
        ]);
        Capsule::table('mod_contabo_pricing_action')->insert(['id' => 1]); // table presence

        (new CronDriver())->runObserveSweep();

        // The stale decision was pruned (sweep reached pruneOldDecisions).
        $this->assertSame(0, Capsule::table('mod_contabo_price_decision')->where('id', 1)->count());
    }
}
