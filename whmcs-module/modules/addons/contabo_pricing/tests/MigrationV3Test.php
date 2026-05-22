<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Idempotency contract for Installer::migrateTo3().
 *
 * Two scenarios are exercised:
 *   - Fresh schema with no `mod_contabo_mapping` table at all — migrateTo3()
 *     must still succeed (the catalog audit table gets created and no other
 *     work happens).
 *   - Re-running migrateTo3() on an already-migrated schema is a no-op:
 *     no new columns added, no duplicate audit table, masks unchanged.
 *
 * Tests deliberately exercise only the schema-shape side via the FakeCapsule
 * column registry, not the data backfill. Bitmask backfill correctness is
 * proven by the CycleSet unit tests; this test proves the migration runner
 * itself doesn't crash, double-apply or leak state between runs.
 */
final class MigrationV3Test extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testMigrateTo3SucceedsOnFreshSchemaWithNoLegacyColumns(): void
    {
        // Fresh schema: no mod_contabo_mapping table at all. migrateTo3()
        // should skip the mapping-table work entirely and only create
        // mod_contabo_catalog_audit + bump no state.
        (new Installer())->migrateTo3();

        $this->assertContains(
            'mod_contabo_catalog_audit',
            array_keys(Capsule::$columns),
            'migrateTo3 must always create the catalog audit table'
        );

        // Sample a few required catalog_audit columns to prove the schema
        // builder ran end-to-end. Full column inventory is intentionally
        // not asserted to avoid an over-tight test.
        $auditCols = Capsule::$columns['mod_contabo_catalog_audit'];
        $this->assertContains('sync_batch_id', $auditCols);
        $this->assertContains('product_id', $auditCols);
        $this->assertContains('cycle', $auditCols);
        $this->assertContains('recurring_column', $auditCols);
        $this->assertContains('setup_fee_column', $auditCols);
        $this->assertContains('applied', $auditCols);
    }

    public function testMigrateTo3IsIdempotentOnAlreadyMigratedSchema(): void
    {
        // Seed a v3-shape mapping table directly: new columns present, no
        // legacy boolean columns. Re-running migrateTo3() must be a no-op.
        Capsule::$columns['mod_contabo_mapping'] = [
            'id', 'profile_id', 'product_id', 'product_group_id', 'active',
            'created_at', 'updated_at',
            // New v3 columns:
            'catalog_cycles_mask',
            'renewal_cycles_mask',
            'markup_overrides_json',
            'respect_disabled_cycles',
            'overwrite_free_cycles',
            'sync_setup_fees',
            'setup_fee_overrides_json',
            'rounding_mode',
            'cycle_pricing_notes',
        ];
        // One row so the "zero rows + no legacy + no new" partial-state
        // guard doesn't fire.
        Capsule::$tables['mod_contabo_mapping'] = [[
            'id'                       => 1,
            'profile_id'               => 100,
            'product_id'               => 200,
            'product_group_id'         => null,
            'active'                   => 1,
            'created_at'               => '2026-05-22 00:00:00',
            'updated_at'               => '2026-05-22 00:00:00',
            'catalog_cycles_mask'      => 0b001001, // Monthly + Annually
            'renewal_cycles_mask'      => 0b001001,
            'markup_overrides_json'    => null,
            'respect_disabled_cycles'  => 1,
            'overwrite_free_cycles'    => 0,
            'sync_setup_fees'          => 0,
            'setup_fee_overrides_json' => null,
            'rounding_mode'            => 'exact_2_decimals',
            'cycle_pricing_notes'      => null,
        ]];

        // Also pre-create catalog_audit so the second create() short-circuits.
        Capsule::$columns['mod_contabo_catalog_audit'] = ['id'];
        Capsule::$tables['mod_contabo_catalog_audit'] = [];

        $columnsBefore = Capsule::$columns;
        $tablesBefore  = Capsule::$tables;
        $statementsBefore = count(Capsule::$statements);

        (new Installer())->migrateTo3();

        // Same column set, same rows. No statements should have been emitted
        // because there are no legacy columns to backfill from.
        $this->assertSame(
            $columnsBefore['mod_contabo_mapping'],
            Capsule::$columns['mod_contabo_mapping'],
            'mapping table columns must be untouched on idempotent re-run'
        );
        $this->assertSame(
            $columnsBefore['mod_contabo_catalog_audit'],
            Capsule::$columns['mod_contabo_catalog_audit'],
            'catalog_audit table must not be recreated'
        );
        $this->assertSame(
            $tablesBefore['mod_contabo_mapping'],
            Capsule::$tables['mod_contabo_mapping'],
            'mapping rows must be unchanged on idempotent re-run'
        );
        $this->assertSame(
            $statementsBefore,
            count(Capsule::$statements),
            'no SQL statements should be issued on a fully-migrated schema'
        );

        // Run a THIRD time for good measure — masks must not double-OR
        // themselves into a different value, columns must not grow.
        (new Installer())->migrateTo3();
        $this->assertSame(
            0b001001,
            (int) Capsule::$tables['mod_contabo_mapping'][0]['catalog_cycles_mask']
        );
        $this->assertSame(
            $columnsBefore['mod_contabo_mapping'],
            Capsule::$columns['mod_contabo_mapping']
        );
    }

    public function testMigrateTo3BackfillsLegacyBooleansIntoBitmasks(): void
    {
        // Realistic v2→v3 upgrade scenario. Seed the v2 mapping shape and
        // run the migration; assert the new columns are added, masks
        // backfilled, and legacy columns dropped.
        Capsule::$columns['mod_contabo_mapping'] = [
            'id', 'profile_id', 'product_id', 'product_group_id',
            'apply_to_monthly', 'apply_to_annually', 'apply_to_semiannually',
            'active', 'created_at', 'updated_at',
        ];
        Capsule::$tables['mod_contabo_mapping'] = [
            [
                'id'                    => 1,
                'profile_id'            => 100,
                'product_id'            => 200,
                'product_group_id'      => null,
                'apply_to_monthly'      => 1,
                'apply_to_annually'     => 1,
                'apply_to_semiannually' => 0,
                'active'                => 1,
                'created_at'            => '2026-01-01 00:00:00',
                'updated_at'            => '2026-01-01 00:00:00',
                // New mask columns start at zero per the migration's default.
                'catalog_cycles_mask'   => 0,
                'renewal_cycles_mask'   => 0,
            ],
            [
                'id'                    => 2,
                'profile_id'            => 101,
                'product_id'            => 201,
                'product_group_id'      => null,
                'apply_to_monthly'      => 0,
                'apply_to_annually'     => 1,
                'apply_to_semiannually' => 1,
                'active'                => 1,
                'created_at'            => '2026-01-01 00:00:00',
                'updated_at'            => '2026-01-01 00:00:00',
                'catalog_cycles_mask'   => 0,
                'renewal_cycles_mask'   => 0,
            ],
        ];

        (new Installer())->migrateTo3();

        // Legacy columns must be gone.
        $this->assertNotContains('apply_to_monthly', Capsule::$columns['mod_contabo_mapping']);
        $this->assertNotContains('apply_to_semiannually', Capsule::$columns['mod_contabo_mapping']);
        $this->assertNotContains('apply_to_annually', Capsule::$columns['mod_contabo_mapping']);

        // New columns must be present.
        $this->assertContains('catalog_cycles_mask', Capsule::$columns['mod_contabo_mapping']);
        $this->assertContains('renewal_cycles_mask', Capsule::$columns['mod_contabo_mapping']);
        $this->assertContains('markup_overrides_json', Capsule::$columns['mod_contabo_mapping']);
        $this->assertContains('respect_disabled_cycles', Capsule::$columns['mod_contabo_mapping']);
        $this->assertContains('rounding_mode', Capsule::$columns['mod_contabo_mapping']);

        // Row 1: monthly+annually backfilled → bits 0+3 = 0b001001 = 9
        $row1 = Capsule::$tables['mod_contabo_mapping'][0];
        $this->assertSame(0b001001, (int) $row1['catalog_cycles_mask']);
        $this->assertSame(0b001001, (int) $row1['renewal_cycles_mask']);

        // Row 2: semiannually+annually → bits 2+3 = 0b001100 = 12
        $row2 = Capsule::$tables['mod_contabo_mapping'][1];
        $this->assertSame(0b001100, (int) $row2['catalog_cycles_mask']);
        $this->assertSame(0b001100, (int) $row2['renewal_cycles_mask']);

        // Backup table must have been created with the timestamp prefix.
        $backupCreated = false;
        foreach (array_keys(Capsule::$tables) as $tbl) {
            if (strpos($tbl, 'mod_contabo_mapping_backup_v3_') === 0) {
                $backupCreated = true;
                $this->assertCount(2, Capsule::$tables[$tbl], 'backup must mirror source row count');
                break;
            }
        }
        $this->assertTrue($backupCreated, 'backup table mod_contabo_mapping_backup_v3_YmdHis must be created');
    }
}
