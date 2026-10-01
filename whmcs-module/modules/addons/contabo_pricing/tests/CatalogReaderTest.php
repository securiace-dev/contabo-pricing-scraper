<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\AdminController;
use ContaboPricing\Catalog\CatalogReader;
use ContaboPricing\CatalogImportService;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class CatalogReaderTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
    }

    /** @return array<string,mixed> */
    private function fixture(string $name): array
    {
        $d = json_decode((string) file_get_contents(__DIR__ . '/fixtures/api/' . $name . '.json'), true);
        $this->assertIsArray($d);
        return $d;
    }

    /** @return array<string,mixed> */
    private function envelope(array $configurations = []): array
    {
        $plans = $this->fixture('plans');
        $payload = $plans[0];
        $item = [
            'machine_id' => 'plan:cloud-vps-10',
            'provider_id' => 'V45',
            'item_type' => 'plan',
            'label' => 'Cloud VPS 10',
            'catalog_version' => 'catalog-test-1',
            'effective_at' => '2026-07-30T10:20:30Z',
            'availability_state' => 'observed',
            'deprecated' => false,
            'compatibility' => ['family' => 'Cloud VPS'],
            'source_observed_at' => '2026-07-30T10:20:30Z',
            'payload_hash' => hash('sha256', CatalogImportService::canonicalJson($payload)),
            'payload' => $payload,
        ];
        $catalog = [
            'schema_version' => '1.0',
            'source_version' => 'test',
            'source_observed_at' => '2026-07-30T10:20:30Z',
            'effective_at' => '2026-07-30T10:20:30Z',
            'catalog_version' => 'catalog-test-1',
            'plans' => $plans,
            'profiles' => [],
            'items' => [$item],
            'compatibility' => [],
            'configurations' => $configurations,
        ];
        $catalog['payload_hash'] = hash('sha256', CatalogImportService::canonicalJson($catalog));
        return $catalog;
    }

    public function testEmptyWhenNothingImported(): void
    {
        $r = new CatalogReader();
        $this->assertNull($r->latestVersion());
        $this->assertSame([], $r->plans());
        $this->assertNull($r->plan('cloud-vps-10'));
        $this->assertSame([], $r->configurator('x')['options']);
        $this->assertSame(0, $r->meta()['snapshot_meta']['plan_count']);
    }

    public function testPlansMatchGoldenPlansFixtureRows(): void
    {
        (new CatalogImportService())->import($this->envelope(), 1);
        $r = new CatalogReader();
        $this->assertEquals($this->fixture('plans'), $r->plans());
        $this->assertEquals($this->fixture('plans')[0], $r->plan('cloud-vps-10'));
        $this->assertCount(1, $r->plans('Cloud VPS'));
        $this->assertSame([], $r->plans('Storage VPS'));
        $this->assertNull($r->plan('nope'));
    }

    public function testMetaMatchesGoldenMetaShape(): void
    {
        (new CatalogImportService())->import($this->envelope(), 1);
        $meta = (new CatalogReader())->meta();
        $golden = $this->fixture('meta');
        $this->assertSame($golden['schema_version'], $meta['schema_version']);
        $this->assertSame('addon-' . AdminController::VERSION, $meta['scraper_version']);
        $this->assertSame($golden['snapshot_meta']['generated_at'], $meta['snapshot_meta']['generated_at']);
        $this->assertSame($golden['snapshot_meta']['plan_count'], $meta['snapshot_meta']['plan_count']);
        $this->assertSame($meta['scraper_version'], $meta['snapshot_meta']['scraper_version']);
    }

    public function testConfiguratorReadsStoredEnvelope(): void
    {
        $cfg = ['plans' => ['https://x/y' => [
            'slug' => 'cloud-vps-10',
            'options' => ['Region' => [['label' => 'EU', 'monthly' => 0.0]]],
        ]]];
        (new CatalogImportService())->import($this->envelope($cfg), 1);
        $r = new CatalogReader();
        $this->assertSame('EU', $r->configurator('cloud-vps-10')['options']['Region'][0]['label']);
        $this->assertSame([], $r->configurator('other')['options']);
    }
}
