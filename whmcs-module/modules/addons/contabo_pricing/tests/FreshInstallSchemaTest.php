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
        $this->assertSame(7, $recorded);
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
}
