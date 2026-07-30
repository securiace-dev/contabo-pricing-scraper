<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use ContaboPricing\SchemaHealth;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Regression: a FRESH addon install (clean activate) must end with a fully
 * current schema, not a v1-shaped schema stamped as the current version.
 *
 * The bug this guards (caught on a clean local WHMCS, masked on prod by its
 * incremental v1→v5 upgrade history): Installer::install() created the v1
 * tables but recorded schema_version = SCHEMA_VERSION, so SchemaHealth saw
 * "already current" and never ran the migrations that add catalog_cycles_mask,
 * profile_mode, the config_* tables, etc. install() now records v1 then runs
 * the idempotent upgrade chain so a brand-new install is brought current.
 */
final class FreshInstallSchemaTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testFreshInstallEndsAtCurrentSchemaVersion(): void
    {
        (new Installer())->install();

        $recorded = (int) Capsule::table('mod_contabo_settings')
            ->where('key', 'schema_version')
            ->value('value');

        $this->assertSame(
            Installer::SCHEMA_VERSION,
            $recorded,
            'a fresh install must record the current schema version'
        );
        $this->assertSame(14, $recorded);
    }

    public function testFreshInstallSchemaIsHealthy(): void
    {
        (new Installer())->install();

        $health = SchemaHealth::requiredColumnsPresent();

        $this->assertTrue(
            $health['healthy'],
            'fresh install must satisfy every required column; missing: '
                . implode(',', $health['missing'])
        );
        $this->assertSame([], $health['missing']);
    }

    public function testFreshInstallAssertOrMigrateIsNoop(): void
    {
        (new Installer())->install();

        // Already current after install — assertOrMigrate must not need to do anything.
        $r = SchemaHealth::assertOrMigrate();

        $this->assertTrue($r['ok']);
        $this->assertSame($r['from'], $r['to'], 'no migration should be needed right after a fresh install');
        $this->assertSame(Installer::SCHEMA_VERSION, $r['to']);
    }

    public function testFreshInstallCreatesProvisioningSuiteSchema(): void
    {
        (new Installer())->install();

        $schema = Capsule::schema();
        $tables = [
            'mod_securiacevps_schema',
            'mod_securiacevps_order_snapshots',
            'mod_securiacevps_resources',
            'mod_securiacevps_operations',
            'mod_securiacevps_operation_attempts',
            'mod_securiacevps_provider_requests',
            'mod_securiacevps_service_locks',
            'mod_securiacevps_capabilities',
            'mod_securiacevps_reconciliation',
            'mod_securiacevps_adoption',
            'mod_securiacevps_billing_sagas',
            'mod_securiacevps_audit_events',
            'mod_securiacevps_operator_commands',
            'mod_contabo_catalog_versions',
            'mod_contabo_catalog_items',
            'mod_contabo_mapping_publications',
            'mod_contabo_publication_approvals',
            'mod_securiacevps_secrets',
            'mod_securiacevps_communications',
            'mod_securiacevps_snapshot_inventory',
        ];

        foreach ($tables as $table) {
            $this->assertTrue($schema->hasTable($table), $table . ' must exist after activation');
        }

        $this->assertSame(
            '5',
            Capsule::table('mod_securiacevps_schema')
                ->where('key', 'schema_version')
                ->value('value')
        );
        foreach ([
            'mod_securiacevps_operator_commands',
            'mod_securiacevps_communications',
        ] as $claimTable) {
            $this->assertTrue($schema->hasColumn($claimTable, 'claim_token'));
            $this->assertTrue($schema->hasColumn($claimTable, 'claim_expires_at'));
        }
    }

    public function testProvisioningSuiteMigrationIsIdempotent(): void
    {
        $installer = new Installer();
        $installer->install();
        $columns = Capsule::$columns;

        $installer->migrateTo9();

        $this->assertSame($columns, Capsule::$columns);
        $this->assertSame(
            1,
            Capsule::table('mod_securiacevps_schema')
                ->where('key', 'schema_version')
                ->count()
        );
    }
}
