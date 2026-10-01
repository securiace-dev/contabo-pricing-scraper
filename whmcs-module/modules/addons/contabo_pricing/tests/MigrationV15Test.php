<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/**
 * Idempotency + completeness contract for Installer::migrateTo15()
 * (WHMCS-native catalog scraping tables).
 */
final class MigrationV15Test extends TestCase
{
    private const TABLES = [
        'mod_contabo_scrape_sources',
        'mod_contabo_scrape_runs',
        'mod_contabo_scrape_run_attempts',
        'mod_contabo_decisions',
    ];

    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testCreatesTablesColumnAndSeeds(): void
    {
        Capsule::$columns['mod_contabo_catalog_versions'] = ['id', 'catalog_version'];
        Capsule::$tables['mod_contabo_catalog_versions'] = [];

        (new Installer())->migrateTo15();

        foreach (self::TABLES as $table) {
            $this->assertArrayHasKey($table, Capsule::$columns, $table);
        }
        $this->assertContains('envelope_json', Capsule::$columns['mod_contabo_catalog_versions']);
        $this->assertContains('api_key_enc', Capsule::$columns['mod_contabo_scrape_sources']);

        $rows = Capsule::table('mod_contabo_scrape_sources')->get();
        $ids = array_map(static function ($r): string { return (string) ((array) $r)['source_id']; }, $rows);
        sort($ids);
        $this->assertSame(['alterlab', 'tinyfish_agent', 'tinyfish_fetch', 'treg'], $ids);
        foreach ($rows as $r) {
            $this->assertSame(0, (int) ((array) $r)['enabled']);
        }
    }

    public function testIdempotentRerun(): void
    {
        Capsule::$columns['mod_contabo_catalog_versions'] = ['id', 'catalog_version'];
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        $installer = new Installer();

        $installer->migrateTo15();
        $columns = Capsule::$columns;
        $installer->migrateTo15();

        $this->assertSame($columns, Capsule::$columns);
        $this->assertSame(4, Capsule::table('mod_contabo_scrape_sources')->count());
    }

    public function testSchemaVersionIs15(): void
    {
        $this->assertSame(15, Installer::SCHEMA_VERSION);
    }
}
