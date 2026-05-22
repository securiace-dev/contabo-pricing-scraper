<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\ApiClient;
use ContaboPricing\CatalogAuditLog;
use ContaboPricing\CycleSet;
use ContaboPricing\ProfileManager;
use ContaboPricing\ProfileVersionInput;
use ContaboPricing\Settings;
use ContaboPricing\SyncEngine;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Phase A.5 — SyncEngine 6-cycle catalog test suite.
 *
 * Covers tests 11 → 24 from the brief:
 *
 *   11. testWritesAllSelectedCyclesInBitmask
 *   12. testNeverOverwritesDisabledCycle
 *   13. testOverwritesDisabledIfRespectFlagOff
 *   14. testNeverOverwritesFreeCycle
 *   15. testOverwritesFreeIfOverwriteFlagOn
 *   16. testSkipsCyclesNotInMask
 *   17. testSetupFeeNotSyncedByDefault
 *   18. testSetupFeeSyncedWhenEnabled
 *   19. testRoundingExactByDefault
 *   20. testRoundingNearest99
 *   21. testPerCycleMarkupOverrideWins
 *   22. testSuspiciousChangeBlocked
 *   23. testCatalogAuditRowEveryDecision
 *   24. testSyncDoesNotMutateTblhosting           ← CRITICAL gate
 *
 * The engine is driven through ::applyCatalogForProfile() (public on the
 * rewritten SyncEngine) so we don't have to mock the entire API plumbing.
 */
final class SyncEngine6CycleTest extends TestCase
{
    /** @var CatalogAuditLogSpy */ private $audit;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->audit = new CatalogAuditLogSpy();
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 11 — writes every cycle present in the bitmask
    // ─────────────────────────────────────────────────────────────────────
    public function testWritesAllSelectedCyclesInBitmask(): void
    {
        $mask = CycleSet::fromCycles(['Monthly', 'Annually', 'Biennially'])->toMask();

        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id'                       => 10,
            'profile_id'               => 1,
            'product_id'               => 100,
            'active'                   => 1,
            'catalog_cycles_mask'      => $mask,
            'respect_disabled_cycles'  => 1,
            'overwrite_free_cycles'    => 0,
            'sync_setup_fees'          => 0,
            'rounding_mode'            => 'exact_2_decimals',
            'markup_overrides_json'    => '',
        ]);
        // Existing tblpricing row with all three cycles already priced.
        $this->seedTblpricingRow(100, 1, [
            'monthly'      => 1000.00,
            'annually'     => 10000.00,
            'biennially'   => 20000.00,
        ]);

        $engine = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0); // monthly = 1000
        $stats = $engine->applyCatalogForProfile(1, $this->loadMapping(10) + ['sync_strategy' => 'auto-apply'], $version, 'batch-test-11');

        $updates = $this->tblpricingUpdates();
        $this->assertCount(3, $updates, 'three columns updated (one per cycle in mask)');
        $cols = array_map(static fn ($u) => array_key_first($u['update']), $updates);
        sort($cols);
        $this->assertSame(['annually', 'biennially', 'monthly'], $cols);
        $this->assertSame(3, $stats['cycles_applied']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 12 — never overwrite a -1.00 cell when respect flag on
    // ─────────────────────────────────────────────────────────────────────
    public function testNeverOverwritesDisabledCycle(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask' => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => -1.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-12');

        $this->assertCount(0, $this->tblpricingUpdates(), 'no tblpricing update for disabled cycle');

        $monthlyRow = $this->findAuditRow('Monthly');
        $this->assertNotNull($monthlyRow);
        $this->assertSame(0, (int) $monthlyRow['applied']);
        $this->assertSame('catalog_skip_disabled_cycle', $monthlyRow['skipped_reason']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 13 — overwrite -1.00 when respect_disabled_cycles=0
    // ─────────────────────────────────────────────────────────────────────
    public function testOverwritesDisabledIfRespectFlagOff(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 0,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => -1.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-13');

        $updates = $this->tblpricingUpdates();
        $this->assertCount(1, $updates);
        $this->assertSame(['monthly' => 1000.00], $updates[0]['update']);

        $row = $this->findAuditRow('Monthly');
        $this->assertSame(1, (int) $row['applied']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 14 — never overwrite 0.00 (free) when overwrite flag off
    // ─────────────────────────────────────────────────────────────────────
    public function testNeverOverwritesFreeCycle(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => 0.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-14');

        $this->assertCount(0, $this->tblpricingUpdates());
        $row = $this->findAuditRow('Monthly');
        $this->assertSame(0, (int) $row['applied']);
        $this->assertSame('catalog_skip_free_cycle', $row['skipped_reason']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 15 — overwrite 0.00 (free) when overwrite_free_cycles=1
    // ─────────────────────────────────────────────────────────────────────
    public function testOverwritesFreeIfOverwriteFlagOn(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 1,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => 0.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-15');

        $updates = $this->tblpricingUpdates();
        $this->assertCount(1, $updates);
        $this->assertSame(1000.00, $updates[0]['update']['monthly']);
        $this->assertSame(1, (int) $this->findAuditRow('Monthly')['applied']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 16 — cycle outside the mask gets skipped with catalog_not_in_mask
    // ─────────────────────────────────────────────────────────────────────
    public function testSkipsCyclesNotInMask(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask(); // mask of just Monthly
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, [
            'monthly'      => 1000.00,
            'annually'     => 10000.00,
        ]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-16');

        $updates = $this->tblpricingUpdates();
        $this->assertCount(1, $updates, 'only Monthly cycle written');
        $this->assertArrayHasKey('monthly', $updates[0]['update']);

        $annual = $this->findAuditRow('Annually');
        $this->assertNotNull($annual);
        $this->assertSame(0, (int) $annual['applied']);
        $this->assertSame('catalog_not_in_mask', $annual['skipped_reason']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 17 — sync_setup_fees=0 → setup-fee column untouched
    // ─────────────────────────────────────────────────────────────────────
    public function testSetupFeeNotSyncedByDefault(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0, // OFF
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => 1000.00, 'msetupfee' => 250.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 99.00); // version has setup fee
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-17');

        $updates = $this->tblpricingUpdates();
        foreach ($updates as $u) {
            foreach (array_keys($u['update']) as $col) {
                $this->assertNotSame('msetupfee', $col, 'setup-fee column must not be written');
            }
        }
        $row = $this->findAuditRow('Monthly');
        $this->assertNull($row['new_setup_fee'] ?? null);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 18 — sync_setup_fees=1 → setup-fee column written + audited
    // ─────────────────────────────────────────────────────────────────────
    public function testSetupFeeSyncedWhenEnabled(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 1, // ON
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => 1000.00, 'msetupfee' => 250.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 99.00);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-18');

        $setupWrites = array_values(array_filter(
            $this->tblpricingUpdates(),
            static fn ($u) => array_key_exists('msetupfee', $u['update'])
        ));
        $this->assertCount(1, $setupWrites, 'one setup-fee column write');
        $this->assertSame(99.00, $setupWrites[0]['update']['msetupfee']);

        $row = $this->findAuditRow('Monthly');
        $this->assertSame(99.00, (float) $row['new_setup_fee']);
        $this->assertSame(250.00, (float) $row['old_setup_fee']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 19 — default rounding is exact_2_decimals
    // ─────────────────────────────────────────────────────────────────────
    public function testRoundingExactByDefault(): void
    {
        // markup 0% → catalog price == version.finalMonthly. Pick a value
        // that exposes the rounding mode unambiguously: 1234.567 → 1234.57.
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        // No existing tblpricing row → no suspicious-change guard triggered.
        // (price_status_before = 'absent', not 'priced'.)
        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1234.567, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-19');

        $updates = $this->tblpricingUpdates();
        $this->assertCount(1, $updates);
        $this->assertSame(1234.57, $updates[0]['update']['monthly']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 20 — nearest_99 rounding rounds up to the next 99-tail
    // ─────────────────────────────────────────────────────────────────────
    public function testRoundingNearest99(): void
    {
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'nearest_99',
            'markup_overrides_json'   => '',
        ]);
        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1234.567, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-20');

        $updates = $this->tblpricingUpdates();
        $this->assertCount(1, $updates);
        $this->assertSame(1299.00, (float) $updates[0]['update']['monthly']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 21 — per-cycle markup override wins over the profile default
    // ─────────────────────────────────────────────────────────────────────
    public function testPerCycleMarkupOverrideWins(): void
    {
        // Annually = fixed 30% markup; Monthly inherits (= cost_plus_pct 0).
        $mask = CycleSet::fromCycles(['Monthly', 'Annually'])->toMask();
        $overrides = json_encode([
            'Annually' => ['strategy' => 'cost_plus_pct', 'value' => 30.0],
        ]);
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => $overrides,
        ]);
        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-21');

        $monthlyRow = $this->findAuditRow('Monthly');
        $annualRow  = $this->findAuditRow('Annually');
        $this->assertNotNull($monthlyRow);
        $this->assertNotNull($annualRow);

        // Monthly: inherit → cost_plus_pct @ 0% × 1 month = 1000.00
        $this->assertSame(1000.00, (float) $monthlyRow['new_price']);
        $this->assertSame('cost_plus_pct', $monthlyRow['markup_strategy_used']);

        // Annually: cost_plus_pct @ 30% × 12 months = 1000 × 1.30 × 12 = 15600.00
        $this->assertSame(15600.00, (float) $annualRow['new_price']);
        $this->assertSame(30.0, (float) $annualRow['markup_value_used']);
        $this->assertSame('cost_plus_pct', $annualRow['markup_strategy_used']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 22 — > 50% jump from existing priced cell is "suspicious" → blocked
    // ─────────────────────────────────────────────────────────────────────
    public function testSuspiciousChangeBlocked(): void
    {
        // Existing monthly = 100; computed new = 900 (9×). 800% jump.
        $mask = CycleSet::fromCycles(['Monthly'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => 100.00]);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(900.00, 0.0);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-22');

        $this->assertCount(0, $this->tblpricingUpdates(), 'suspicious change must not write');
        $row = $this->findAuditRow('Monthly');
        $this->assertSame(0, (int) $row['applied']);
        $this->assertSame('suspicious_change_blocked', $row['skipped_reason']);
        $this->assertSame(900.00, (float) $row['rounded_price']);
        $this->assertSame(100.00, (float) $row['old_price']);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 23 — N cycles × M mappings = N×M audit rows (applied or skipped)
    // ─────────────────────────────────────────────────────────────────────
    public function testCatalogAuditRowEveryDecision(): void
    {
        // 2 mappings × 6 cycles × 1 currency = 12 audit rows expected.
        $mask = CycleSet::fromCycles(['Monthly', 'Annually'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedMapping([
            'id' => 11, 'profile_id' => 1, 'product_id' => 200, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 0,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 0.0);

        // Both mappings ran through the same batch:
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-23');
        // Note: applyCatalogForProfile already loops over all mappings for the
        // profile, so the second call would double-emit. Single call covers
        // both seeded mappings. Verify row count:
        $rows = $this->audit->rows;
        $this->assertCount(12, $rows, 'two mappings × six cycles = 12 audit rows');

        // Two cycles per mapping applied (Monthly + Annually), four skipped
        // (Quarterly, Semi-Annually, Biennially, Triennially).
        $applied = array_values(array_filter($rows, static fn ($r) => (int) $r['applied'] === 1));
        $skipped = array_values(array_filter($rows, static fn ($r) => (int) $r['applied'] === 0));
        $this->assertCount(4, $applied);
        $this->assertCount(8, $skipped);
        foreach ($skipped as $r) {
            $this->assertSame('catalog_not_in_mask', $r['skipped_reason']);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Test 24 — CRITICAL: SyncEngine must not mutate tblhosting
    // ─────────────────────────────────────────────────────────────────────
    public function testSyncDoesNotMutateTblhosting(): void
    {
        $mask = CycleSet::fromCycles(['Monthly', 'Annually'])->toMask();
        $this->seedCurrency(1, 'INR');
        $this->seedMapping([
            'id' => 10, 'profile_id' => 1, 'product_id' => 100, 'active' => 1,
            'catalog_cycles_mask'     => $mask,
            'respect_disabled_cycles' => 1,
            'overwrite_free_cycles'   => 0,
            'sync_setup_fees'         => 1,
            'rounding_mode'           => 'exact_2_decimals',
            'markup_overrides_json'   => '',
        ]);
        $this->seedTblpricingRow(100, 1, ['monthly' => 800.00, 'annually' => 9000.00]);

        // Seed two realistic-looking tblhosting rows. The engine must not
        // touch either of them — snapshot then re-compare.
        Capsule::$tables['tblhosting'] = [
            ['id' => 5001, 'recurringamount' => 1234.5678, 'billingcycle' => 'Monthly'],
            ['id' => 5002, 'recurringamount' => 9876.5432, 'billingcycle' => 'Annually'],
        ];
        $snapshotHash = self::hashTable(Capsule::$tables['tblhosting']);

        $engine  = $this->makeEngine();
        $version = $this->makeVersion(1000.00, 50.00);
        $engine->applyCatalogForProfile(1, $this->loadMapping(10), $version, 'batch-test-24');

        $postHash = self::hashTable(Capsule::$tables['tblhosting']);
        $this->assertSame($snapshotHash, $postHash,
            'tblhosting hash MUST be byte-identical before and after SyncEngine run');

        // Belt + braces: no Capsule::$calls / $inserts targeted tblhosting.
        foreach (Capsule::$calls as $call) {
            $this->assertNotSame('tblhosting', $call['table'],
                'SyncEngine emitted an update against tblhosting');
        }
        foreach (Capsule::$inserts as $ins) {
            $this->assertNotSame('tblhosting', $ins['table'],
                'SyncEngine emitted an insert against tblhosting');
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────

    private function makeEngine(): SyncEngine
    {
        $settings = new Settings(
            'http://localhost:8080/api/v1', // apiBaseUrl
            '',                              // apiToken
            'manual',                        // defaultSyncStrategy
            'INR',                           // currencyIso
            true,                            // applyGst18
            0.0,                             // fxMarkupPct — kept at 0 so version.finalMonthly matches the input exactly under EUR-passthrough
            365,                             // logRetentionDays
            ''                                // moduleLink
        );
        $api = new class extends ApiClient {
            public function __construct() { /* no-op — never called in unit tests */ }
        };
        $profileMgr = new class($settings) extends ProfileManager {
            public function listProfiles(bool $activeOnly = true): array { return []; }
            public function latestVersion(int $profileId): ?array { return null; }
            public function appendVersion(int $profileId, ProfileVersionInput $v): int { return 0; }
        };
        return new SyncEngine($settings, $api, $profileMgr, $this->audit);
    }

    /**
     * Build a ProfileVersionInput whose `finalMonthly` equals the desired
     * landed-monthly cost the test wants the engine to use. We pin currency=EUR
     * so the FX branch in ProfileVersionInput::computed is bypassed and
     * finalMonthly == configuredMonthlyEur.
     */
    private function makeVersion(float $monthly, float $setupFee): ProfileVersionInput
    {
        return ProfileVersionInput::computed(
            $monthly, $monthly, $setupFee, [], [],
            1.0,           // fxRate (ignored when currency=EUR)
            'test',         // fxSource
            0.0,           // fxMarkupPct
            false,         // applyGst18 (off so finalMonthly == configured exactly)
            'EUR',         // currencyIso (forces passthrough)
            'snap-test'
        );
    }

    private function seedCurrency(int $id, string $code): void
    {
        if (!isset(Capsule::$tables['tblcurrencies'])) {
            Capsule::$tables['tblcurrencies'] = [];
        }
        Capsule::$tables['tblcurrencies'][] = ['id' => $id, 'code' => $code];
    }

    /** @param array<string,mixed> $row */
    private function seedMapping(array $row): void
    {
        if (!isset(Capsule::$tables['mod_contabo_mapping'])) {
            Capsule::$tables['mod_contabo_mapping'] = [];
        }
        Capsule::$tables['mod_contabo_mapping'][] = $row;
    }

    /** @param array<string,mixed> $cols */
    private function seedTblpricingRow(int $productId, int $currencyId, array $cols): void
    {
        if (!isset(Capsule::$tables['tblpricing'])) {
            Capsule::$tables['tblpricing'] = [];
        }
        Capsule::$tables['tblpricing'][] = array_merge([
            'type'     => 'product',
            'currency' => $currencyId,
            'relid'    => $productId,
        ], $cols);
    }

    /** @return array<string,mixed> */
    private function loadMapping(int $id): array
    {
        foreach (Capsule::$tables['mod_contabo_mapping'] ?? [] as $m) {
            if ((int) ($m['id'] ?? 0) === $id) {
                return $m;
            }
        }
        return [];
    }

    /** @return list<array{table:string,where:array<string,mixed>,update:array<string,mixed>}> */
    private function tblpricingUpdates(): array
    {
        return array_values(array_filter(
            Capsule::$calls,
            static fn ($call) => $call['table'] === 'tblpricing'
        ));
    }

    /** @return array<string,mixed>|null */
    private function findAuditRow(string $cycle): ?array
    {
        foreach ($this->audit->rows as $r) {
            if (($r['cycle'] ?? null) === $cycle) {
                return $r;
            }
        }
        return null;
    }

    /** @param list<array<string,mixed>> $rows */
    private static function hashTable(array $rows): string
    {
        // Stable canonical serialisation — sort keys per row, sort rows by 'id'.
        usort($rows, static fn ($a, $b) => ((int) ($a['id'] ?? 0)) <=> ((int) ($b['id'] ?? 0)));
        $canon = array_map(static function (array $row): array {
            ksort($row);
            return $row;
        }, $rows);
        return sha1((string) json_encode($canon));
    }
}

/**
 * Test double for CatalogAuditLog that captures every insert into a public
 * array. Subclassing avoids DB I/O while preserving the exact same validation
 * surface as production code.
 */
final class CatalogAuditLogSpy extends CatalogAuditLog
{
    /** @var list<array<string,mixed>> */
    public array $rows = [];

    protected function storeRow(array $row): int
    {
        $id = count($this->rows) + 1;
        $row['id'] = $id;
        $this->rows[] = $row;
        return $id;
    }

    protected function fetchRecentBatches(int $limit): array
    {
        return [];
    }
}
