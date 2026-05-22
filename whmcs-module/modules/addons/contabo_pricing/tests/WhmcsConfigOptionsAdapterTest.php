<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\WhmcsConfigOptionsAdapter;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.6.1 — WhmcsConfigOptionsAdapter (amendment 3, sole write chokepoint).
 *
 * Covers:
 *   - dry-run default: every mutator returns action='dryrun' + payload and
 *     writes NOTHING (zero inserts, zero updates);
 *   - INR-only guard (amendment 10): non-INR currency pricing is skipped with
 *     skip_reason='non_inr_currency_unsupported_v1' and writes no tblpricing row;
 *   - verifySchema() returns the {ok, missing} shape;
 *   - real-write idempotency (created → noop → updated) with audit rows.
 */
final class WhmcsConfigOptionsAdapterTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    // ---- dry-run (the safe A.6.1 default) ------------------------------------

    public function testDefaultConstructorIsDryRun(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter();
        $this->assertTrue($adapter->isDryRun());
        $this->assertNotSame('', $adapter->syncBatchId());
    }

    public function testDryRunGroupReturnsDryrunAndWritesNothing(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(true);
        $r = $adapter->upsertGroup('Contabo Cloud VPS 10', 'Configurable options');

        $this->assertSame('dryrun', $r['action']);
        $this->assertSame('tblproductconfiggroups', $r['table']);
        $this->assertSame('Contabo Cloud VPS 10', $r['payload']['name']);
        $this->assertSame('Configurable options', $r['payload']['description']);
        $this->assertNull($r['id']);

        $this->assertCount(0, Capsule::$inserts, 'dry-run must insert nothing');
        $this->assertCount(0, Capsule::$calls, 'dry-run must update nothing');
    }

    public function testEveryMutatorIsDryrunAndWritesNothing(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(true);

        $results = [
            $adapter->upsertGroup('G', 'd'),
            $adapter->linkGroupToProduct(10, 501),
            $adapter->upsertOption(10, 'Image', 0, null, null, 1),
            $adapter->upsertSubOption(20, '[OS] Ubuntu 24.04', 0, false),
            $adapter->upsertConfigOptionPricing(30, WhmcsConfigOptionsAdapter::INR_CURRENCY_ID, ['monthly' => 191.0], ['monthly' => 0.0]),
        ];

        foreach ($results as $r) {
            $this->assertSame('dryrun', $r['action']);
            $this->assertArrayHasKey('payload', $r);
            $this->assertNotEmpty($r['payload']);
        }

        $this->assertCount(0, Capsule::$inserts, 'no inserts across all dry-run mutators');
        $this->assertCount(0, Capsule::$calls, 'no updates across all dry-run mutators');
    }

    public function testDryRunPricingPayloadCarriesAllSixCycleColumns(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(true);
        $r = $adapter->upsertConfigOptionPricing(
            42,
            WhmcsConfigOptionsAdapter::INR_CURRENCY_ID,
            ['monthly' => 191.0, 'annually' => 2292.0]
        );

        $this->assertSame('dryrun', $r['action']);
        $this->assertSame('configoptions', $r['payload']['type']);
        $this->assertSame(WhmcsConfigOptionsAdapter::INR_CURRENCY_ID, $r['payload']['currency']);
        $this->assertSame(42, $r['payload']['relid']);
        $this->assertSame(191.0, $r['payload']['monthly']);
        $this->assertSame(2292.0, $r['payload']['annually']);
        // Unspecified cycles default to 0.0.
        $this->assertSame(0.0, $r['payload']['quarterly']);
        $this->assertSame(0.0, $r['payload']['msetupfee']);
    }

    // ---- INR-only guard (amendment 10) ---------------------------------------

    public function testInrGuardSkipsNonInrCurrencyDryRun(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(true);
        $r = $adapter->upsertConfigOptionPricing(7, 2 /* USD */, ['monthly' => 5.0]);

        $this->assertSame('skipped', $r['action']);
        $this->assertSame(WhmcsConfigOptionsAdapter::SKIP_NON_INR, $r['skip_reason']);
        $this->assertSame('non_inr_currency_unsupported_v1', $r['skip_reason']);
        $this->assertNull($r['id']);
        $this->assertCount(0, Capsule::$inserts, 'non-INR pricing writes nothing');
    }

    public function testInrGuardSkipsNonInrCurrencyRealWriteWithAuditNoteAndNoPricingRow(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(false);
        $r = $adapter->upsertConfigOptionPricing(7, 3 /* EUR */, ['monthly' => 5.0]);

        $this->assertSame('skipped', $r['action']);
        $this->assertSame('non_inr_currency_unsupported_v1', $r['skip_reason']);

        // No tblpricing insert occurred.
        foreach (Capsule::$inserts as $ins) {
            $this->assertNotSame('tblpricing', $ins['table'], 'no tblpricing row for non-INR currency');
        }

        // Exactly one audit row, carrying the skip reason.
        $audit = Capsule::table('mod_contabo_config_option_audit')->get();
        $this->assertCount(1, $audit);
        $this->assertSame('skipped', $audit[0]['action']);
        $this->assertSame('non_inr_currency_unsupported_v1', $audit[0]['skip_reason']);
        $this->assertSame('tblpricing', $audit[0]['whmcs_table']);
    }

    // ---- verifySchema (read-only) --------------------------------------------

    public function testVerifySchemaReturnsOkMissingShape(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(true);
        $result = $adapter->verifySchema();

        $this->assertArrayHasKey('ok', $result);
        $this->assertArrayHasKey('missing', $result);
        $this->assertIsBool($result['ok']);
        $this->assertIsArray($result['missing']);
    }

    public function testVerifySchemaReportsMissingTables(): void
    {
        // No WHMCS tables seeded → everything missing, ok=false.
        $adapter = new WhmcsConfigOptionsAdapter(true);
        $result = $adapter->verifySchema();

        $this->assertFalse($result['ok']);
        $this->assertContains('tblproductconfiggroups', $result['missing']);
        $this->assertContains('tblpricing', $result['missing']);
    }

    public function testVerifySchemaOkWhenAllTablesAndColumnsPresent(): void
    {
        Capsule::$columns['tblproductconfiggroups']     = ['id', 'name', 'description'];
        Capsule::$columns['tblproductconfigoptions']    = ['id', 'gid', 'optionname', 'optiontype', 'qtyminimum', 'qtymaximum', 'order', 'hidden'];
        Capsule::$columns['tblproductconfigoptionssub'] = ['id', 'configid', 'optionname', 'sortorder', 'hidden'];
        Capsule::$columns['tblproductconfiglinks']      = ['id', 'gid', 'pid'];
        Capsule::$columns['tblpricing']                 = ['id', 'type', 'currency', 'relid', 'monthly', 'quarterly', 'semiannually', 'annually', 'biennially', 'triennially', 'msetupfee', 'qsetupfee', 'ssetupfee', 'asetupfee', 'bsetupfee', 'tsetupfee'];

        $adapter = new WhmcsConfigOptionsAdapter(true);
        $result = $adapter->verifySchema();

        $this->assertTrue($result['ok']);
        $this->assertSame([], $result['missing']);
    }

    public function testVerifySchemaReportsMissingColumn(): void
    {
        Capsule::$columns['tblproductconfiggroups']     = ['id', 'name']; // missing description
        Capsule::$columns['tblproductconfigoptions']    = ['id', 'gid', 'optionname', 'optiontype', 'qtyminimum', 'qtymaximum', 'order', 'hidden'];
        Capsule::$columns['tblproductconfigoptionssub'] = ['id', 'configid', 'optionname', 'sortorder', 'hidden'];
        Capsule::$columns['tblproductconfiglinks']      = ['id', 'gid', 'pid'];
        Capsule::$columns['tblpricing']                 = ['id', 'type', 'currency', 'relid', 'monthly', 'quarterly', 'semiannually', 'annually', 'biennially', 'triennially', 'msetupfee', 'qsetupfee', 'ssetupfee', 'asetupfee', 'bsetupfee', 'tsetupfee'];

        $adapter = new WhmcsConfigOptionsAdapter(true);
        $result = $adapter->verifySchema();

        $this->assertFalse($result['ok']);
        $this->assertContains('tblproductconfiggroups.description', $result['missing']);
    }

    // ---- real-write idempotency (mocked write through FakeCapsule) -----------

    public function testRealWriteGroupCreatedThenNoopThenUpdated(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(false, 'batch-1');

        $created = $adapter->upsertGroup('VPS10', 'first');
        $this->assertSame('created', $created['action']);
        $this->assertGreaterThan(0, (int) $created['id']);

        // Identical inputs → noop, no new group row.
        $noop = $adapter->upsertGroup('VPS10', 'first');
        $this->assertSame('noop', $noop['action']);
        $this->assertSame($created['id'], $noop['id']);
        $this->assertSame(1, Capsule::table('tblproductconfiggroups')->where('name', 'VPS10')->count());

        // Changed description → updated in place, same id.
        $updated = $adapter->upsertGroup('VPS10', 'second');
        $this->assertSame('updated', $updated['action']);
        $this->assertSame($created['id'], $updated['id']);
        $row = Capsule::table('tblproductconfiggroups')->where('name', 'VPS10')->first();
        $this->assertSame('second', $row['description']);
    }

    public function testRealWriteEmitsAuditRowPerWrite(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(false, 'batch-audit');
        $adapter->upsertGroup('AuditGroup', 'x');

        $audit = Capsule::table('mod_contabo_config_option_audit')->get();
        $this->assertCount(1, $audit);
        $this->assertSame('batch-audit', $audit[0]['sync_batch_id']);
        $this->assertSame('tblproductconfiggroups', $audit[0]['whmcs_table']);
        $this->assertSame('created', $audit[0]['action']);
        $this->assertNotEmpty($audit[0]['payload_json']);
    }

    public function testRealWriteLinkGroupToProductIsIdempotent(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(false);
        $first = $adapter->linkGroupToProduct(5, 100);
        $this->assertSame('created', $first['action']);
        $second = $adapter->linkGroupToProduct(5, 100);
        $this->assertSame('noop', $second['action']);
        $this->assertSame(1, Capsule::table('tblproductconfiglinks')->where('gid', 5)->where('pid', 100)->count());
    }

    public function testRealWritePricingInrCreatesAndUpdates(): void
    {
        $adapter = new WhmcsConfigOptionsAdapter(false);

        $created = $adapter->upsertConfigOptionPricing(
            55,
            WhmcsConfigOptionsAdapter::INR_CURRENCY_ID,
            ['monthly' => 191.0]
        );
        $this->assertSame('created', $created['action']);

        $row = Capsule::table('tblpricing')->where('relid', 55)->where('type', 'configoptions')->first();
        $this->assertNotNull($row);
        $this->assertSame(191.0, (float) $row['monthly']);

        // Same price → noop.
        $noop = $adapter->upsertConfigOptionPricing(55, 1, ['monthly' => 191.0]);
        $this->assertSame('noop', $noop['action']);

        // Changed price → updated.
        $updated = $adapter->upsertConfigOptionPricing(55, 1, ['monthly' => 200.0]);
        $this->assertSame('updated', $updated['action']);
        $row = Capsule::table('tblpricing')->where('relid', 55)->where('type', 'configoptions')->first();
        $this->assertSame(200.0, (float) $row['monthly']);
    }

    public function testStaticGrepGateHeaderPresent(): void
    {
        // The chokepoint file MUST carry the static-grep gate marker so the
        // grep that proves "no raw config writes elsewhere" has its anchor.
        $src = file_get_contents(__DIR__ . '/../lib/WhmcsConfigOptionsAdapter.php');
        $this->assertIsString($src);
        $this->assertStringContainsString('STATIC-GREP GATE', $src);
    }
}
