<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\Installer;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/** Idempotency + completeness contract for Installer::migrateTo16() (family registry). */
final class MigrationV16Test extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    public function testCreatesFamilyRegistryAndAttemptColumn(): void
    {
        Capsule::$columns['mod_contabo_scrape_run_attempts'] = ['id', 'run_id'];
        (new Installer())->migrateTo16();

        $cols = Capsule::$columns['mod_contabo_scrape_families'];
        foreach ([
            'category_id', 'slug', 'title', 'nav_title', 'nav_href', 'nav_position', 'status', 'first_seen_at', 'last_seen_at',
            'last_plan_count', 'typical_plan_count', 'plan_count_history_json', 'title_history_json', 'plan_slugs_json',
            'sample_product_url', 'approved', 'admin_hidden', 'display_name', 'successor_of', 'notes',
        ] as $c) {
            $this->assertContains($c, $cols, $c);
        }
        $this->assertContains('nav_titles_json', Capsule::$columns['mod_contabo_scrape_run_attempts']);
    }

    public function testIdempotentRerun(): void
    {
        Capsule::$columns['mod_contabo_scrape_run_attempts'] = ['id', 'run_id'];
        $i = new Installer();
        $i->migrateTo16();
        $before = Capsule::$columns;
        $i->migrateTo16();
        $this->assertSame($before, Capsule::$columns);
    }

    public function testClearsOnlyTheLegacyDefaultPlanUrlList(): void
    {
        $legacy = [];
        for ($i = 0; $i < 16; $i++) {
            $legacy[] = 'https://contabo.com/en/vps/cloud-vps-' . ($i + 10) . '/';
        }
        $legacy[0] = 'https://contabo.com/en/vps/cloud-vps-10/';
        Capsule::table('mod_contabo_settings')->insert(['key' => 'scrape.plan_urls_json', 'value' => json_encode($legacy)]);
        (new Installer())->migrateTo16();
        $this->assertSame('[]', Capsule::table('mod_contabo_settings')->where('key', 'scrape.plan_urls_json')->value('value'));

        $custom = json_encode(['https://contabo.com/en/vps/cloud-vps-core-4/']);
        Capsule::table('mod_contabo_settings')->where('key', 'scrape.plan_urls_json')->update(['value' => $custom]);
        (new Installer())->migrateTo16();
        $this->assertSame($custom, Capsule::table('mod_contabo_settings')->where('key', 'scrape.plan_urls_json')->value('value'));
    }

    public function testSchemaVersionIs16(): void
    {
        $this->assertSame(16, Installer::SCHEMA_VERSION);
    }
}
