<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\BackfillCommand;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * 0.5.1 parity — BackfillCommand candidate loading.
 *
 * Proves the backfill loads candidates via the REAL tblhosting.`amount` column
 * (the old code JOINed + selected the non-existent `recurringamount`), and that a
 * candidate-load failure is handled at the command level (logs + exits safely)
 * rather than throwing uncaught out of run().
 */
final class BackfillCommandTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testRunCreatesPoliciesFromAmountColumn(): void
    {
        Capsule::table('mod_contabo_mapping')->insert(['id' => 1, 'product_id' => 501, 'profile_id' => 1, 'active' => 1]);

        // On the mapped product: two billable with amount>0, one amount=0, one Cancelled.
        Capsule::table('tblhosting')->insert(['id' => 100, 'packageid' => 501, 'domainstatus' => 'Active',    'amount' => 500.0]);
        Capsule::table('tblhosting')->insert(['id' => 101, 'packageid' => 501, 'domainstatus' => 'Suspended', 'amount' => 300.0]);
        Capsule::table('tblhosting')->insert(['id' => 102, 'packageid' => 501, 'domainstatus' => 'Active',    'amount' => 0.0]);    // skipped (<=0)
        Capsule::table('tblhosting')->insert(['id' => 103, 'packageid' => 501, 'domainstatus' => 'Cancelled', 'amount' => 999.0]);  // excluded by loader

        $r = (new BackfillCommand())->run();

        $this->assertSame(2, $r['created']); // 100 + 101
        $this->assertSame(1, $r['skipped']); // 102 (amount 0)
        $this->assertSame(0, $r['errors']);

        // Locked price came from tblhosting.amount.
        $p = (array) Capsule::table('mod_contabo_service_policy')->where('service_id', 100)->first();
        $this->assertSame(500.0, (float) $p['locked_price']);
    }

    public function testCandidateLoadFailureExitsSafely(): void
    {
        // Simulate a schema/query failure in candidate loading: run() must catch
        // it, log, and return errors=1 — not throw uncaught.
        $cmd = new class extends BackfillCommand {
            protected function loadCandidateServices(): array
            {
                throw new \RuntimeException('Unknown column simulated');
            }
        };
        $r = $cmd->run();
        $this->assertSame(['created' => 0, 'skipped' => 0, 'errors' => 1], $r);
    }
}
