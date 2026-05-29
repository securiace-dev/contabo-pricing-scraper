<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Idempotency + completeness contract for Installer::migrateTo5()
 * (Phase A.6.1 configurable-options foundation).
 *
 * Proves:
 *   - migrateTo5() runs end-to-end on a fresh schema without error.
 *   - All 7 new addon-owned tables exist afterwards, with their key columns.
 *   - The three additive profile scope columns are added.
 *   - Re-running migrateTo5() is a no-op: no duplicate tables, no duplicate
 *     columns (hasTable / hasColumn guards hold).
 *
 * Schema-shape only via the FakeCapsule column registry — matching the
 * MigrationV3Test approach. SCHEMA_VERSION is asserted to have advanced to 5.
 */
final class MigrationV5Test extends TestCase
{
    /** The seven addon-owned tables migrateTo5() must create. */
    private const V5_TABLES = [
        'mod_contabo_config_group_link',
        'mod_contabo_config_option_link',
        'mod_contabo_config_option_value_link',
        'mod_contabo_config_option_audit',
        'mod_contabo_option_capability',
        'mod_contabo_option_compatibility',
        'mod_contabo_service_config_snapshot',
    ];

    /** The three additive columns on mod_contabo_profile. */
    private const V5_PROFILE_COLUMNS = [
        'product_scope_key',
        'commercial_variant',
        'audience_segment',
    ];

    protected function setUp(): void
    {
        Capsule::reset();
    }

    /**
     * Seed a minimal v4-shape profile table (so the additive columns have a
     * table to attach to) and run migrateTo5() on an otherwise fresh schema.
     */
    private function seedV4Profile(): void
    {
        Capsule::$columns['mod_contabo_profile'] = [
            'id', 'slug', 'name', 'plan_slug', 'period_months', 'region', 'os',
            'options', 'tags', 'sync_strategy', 'active', 'latest_version_id',
            'created_at', 'updated_at',
            // v4 identity columns:
            'profile_mode', 'profile_fingerprint_hash', 'profile_identity_json',
        ];
        Capsule::$tables['mod_contabo_profile'] = [];
    }

    public function testMigrateTo5RunsOnFreshSchemaWithoutError(): void
    {
        $this->seedV4Profile();

        (new Installer())->migrateTo5();

        // All seven tables must now be present.
        foreach (self::V5_TABLES as $table) {
            $this->assertArrayHasKey(
                $table,
                Capsule::$columns,
                "migrateTo5 must create $table"
            );
        }

        // Sentinel column checks proving the schema builder ran end-to-end for
        // each table (full column inventories are deliberately not asserted to
        // avoid an over-tight test, except the load-bearing ones below).
        $this->assertContains('group_key', Capsule::$columns['mod_contabo_config_group_link']);
        $this->assertContains('whmcs_product_id', Capsule::$columns['mod_contabo_config_group_link']);
        $this->assertContains('enabled', Capsule::$columns['mod_contabo_config_group_link']);

        $this->assertContains('dimension_key', Capsule::$columns['mod_contabo_config_option_link']);
        $this->assertContains('expose_to_customer', Capsule::$columns['mod_contabo_config_option_link']);
        $this->assertContains('default_value', Capsule::$columns['mod_contabo_config_option_link']);

        // Round-trip provisioning keys must exist on the value-link table.
        $this->assertContains('contabo_value_key', Capsule::$columns['mod_contabo_config_option_value_link']);
        $this->assertContains('contabo_label', Capsule::$columns['mod_contabo_config_option_value_link']);
        $this->assertContains('whmcs_sub_id', Capsule::$columns['mod_contabo_config_option_value_link']);
        $this->assertContains('monthly_eur_delta', Capsule::$columns['mod_contabo_config_option_value_link']);

        $this->assertContains('sync_batch_id', Capsule::$columns['mod_contabo_config_option_audit']);
        $this->assertContains('action', Capsule::$columns['mod_contabo_config_option_audit']);

        // capability_source must exist (its 'manual_assumption' default lives in
        // the SQL DDL, not the FakeCapsule registry — proven in production).
        $this->assertContains('capability_source', Capsule::$columns['mod_contabo_option_capability']);
        $this->assertContains('provisioning_action', Capsule::$columns['mod_contabo_option_capability']);

        $this->assertContains('compatible_with_json', Capsule::$columns['mod_contabo_option_compatibility']);
        $this->assertContains('incompatible_with_json', Capsule::$columns['mod_contabo_option_compatibility']);

        $this->assertContains('service_id', Capsule::$columns['mod_contabo_service_config_snapshot']);
        $this->assertContains('contabo_payload_json', Capsule::$columns['mod_contabo_service_config_snapshot']);
    }

    public function testMigrateTo5AddsProfileScopeColumns(): void
    {
        $this->seedV4Profile();

        (new Installer())->migrateTo5();

        foreach (self::V5_PROFILE_COLUMNS as $column) {
            $this->assertContains(
                $column,
                Capsule::$columns['mod_contabo_profile'],
                "migrateTo5 must add mod_contabo_profile.$column"
            );
        }
    }

    public function testMigrateTo5IsIdempotent(): void
    {
        $this->seedV4Profile();

        // First run creates everything.
        (new Installer())->migrateTo5();

        $columnsAfterFirst = Capsule::$columns;
        $tablesAfterFirst  = Capsule::$tables;

        // Second + third runs must be pure no-ops: identical column registry,
        // identical table set, no duplicate columns on any table.
        (new Installer())->migrateTo5();
        (new Installer())->migrateTo5();

        $this->assertSame(
            $columnsAfterFirst,
            Capsule::$columns,
            'column registry must be byte-identical after idempotent re-runs'
        );
        $this->assertSame(
            array_keys($tablesAfterFirst),
            array_keys(Capsule::$tables),
            'no duplicate / extra tables on idempotent re-run'
        );

        // Explicit no-duplicate-column check for each v5 table + the profile
        // table (array_unique would shrink a list that had dupes).
        $checkTables = array_merge(self::V5_TABLES, ['mod_contabo_profile']);
        foreach ($checkTables as $table) {
            $cols = Capsule::$columns[$table];
            $this->assertSame(
                array_values(array_unique($cols)),
                array_values($cols),
                "table $table must contain no duplicate columns after re-runs"
            );
        }

        // Profile scope columns must each appear exactly once.
        foreach (self::V5_PROFILE_COLUMNS as $column) {
            $occurrences = count(array_keys(
                Capsule::$columns['mod_contabo_profile'],
                $column,
                true
            ));
            $this->assertSame(
                1,
                $occurrences,
                "mod_contabo_profile.$column must appear exactly once after re-runs"
            );
        }
    }

    public function testMigrateTo5SkipsProfileColumnsWhenProfileTableAbsent(): void
    {
        // No profile table seeded: the scope-column block must be skipped
        // gracefully and the seven tables must still be created.
        (new Installer())->migrateTo5();

        foreach (self::V5_TABLES as $table) {
            $this->assertArrayHasKey(
                $table,
                Capsule::$columns,
                "migrateTo5 must create $table even without a profile table"
            );
        }

        $this->assertArrayNotHasKey(
            'mod_contabo_profile',
            Capsule::$columns,
            'no phantom profile table should be created'
        );
    }

    /**
     * Drive the full upgrade() runner from a stale schema_version of 4 and
     * confirm it advances the recorded version to 5 (matching how migrateTo4
     * relies on the upgrade() loop tail to bump the version).
     */
    public function testUpgradeLoopBumpsSchemaVersionTo5(): void
    {
        $this->seedV4Profile();
        Capsule::$columns['mod_contabo_settings'] = ['key', 'value', 'updated_at'];
        Capsule::$tables['mod_contabo_settings'] = [
            ['key' => 'schema_version', 'value' => '4', 'updated_at' => '2026-05-22 00:00:00'],
        ];

        (new Installer())->upgrade('4');

        $recorded = (int) Capsule::table('mod_contabo_settings')
            ->where('key', 'schema_version')
            ->value('value');

        $this->assertSame(8, $recorded, 'upgrade() must record schema_version=8 (full chain through migrateTo8)');
        $this->assertSame(8, Installer::SCHEMA_VERSION, 'SCHEMA_VERSION constant must be 8');

        // The v5 tables came along for the ride.
        foreach (self::V5_TABLES as $table) {
            $this->assertArrayHasKey($table, Capsule::$columns);
        }
    }
}
