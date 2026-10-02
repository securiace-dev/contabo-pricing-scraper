<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CatalogImportService;
use ContaboPricing\Installer;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class GoldenApiContractTest extends TestCase
{
    private const API_SCHEMA_VERSION = '1.1';

    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$columns['mod_contabo_catalog_versions'] = [
            'catalog_version', 'state', 'payload_hash', 'source_observed_at',
        ];
        Capsule::$columns['mod_contabo_catalog_items'] = [
            'catalog_version_id', 'machine_id', 'provider_id', 'item_type',
            'availability_state', 'payload_hash', 'payload_json',
        ];
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
    }

    public function testAllRequiredGoldenFixturesArePresentAndNestedTypesAreStable(): void
    {
        $fixtures = [];
        foreach (['meta', 'plans', 'catalog', 'quote'] as $name) {
            $fixtures[$name] = $this->fixture($name);
        }

        $this->assertSame(self::API_SCHEMA_VERSION, $fixtures['meta']['schema_version']);
        $this->assertIsArray($fixtures['meta']['snapshot_meta']);
        $this->assertIsString($fixtures['meta']['snapshot_meta']['generated_at']);
        $this->assertIsInt($fixtures['meta']['snapshot_meta']['plan_count']);

        $this->assertCount(1, $fixtures['plans']);
        $plan = $fixtures['plans'][0];
        $this->assertIsString($plan['product_slug']);
        $this->assertIsString($plan['provider_sku_id']);
        $this->assertIsFloat($plan['base_monthly_price']);
        $this->assertIsArray($plan['periods']);
        $this->assertIsInt($plan['periods'][0]['months']);
        $this->assertIsFloat($plan['periods'][0]['effective_monthly']);

        $catalog = $fixtures['catalog'];
        $this->assertSame(CatalogImportService::SUPPORTED_SCHEMA_VERSION, $catalog['schema_version']);
        $this->assertIsString($catalog['catalog_version']);
        $this->assertMatchesRegularExpression('/^[a-f0-9]{64}$/', $catalog['payload_hash']);
        $this->assertCount(1, $catalog['items']);
        $this->assertSame($catalog['catalog_version'], $catalog['items'][0]['catalog_version']);
        $this->assertSame($plan, $catalog['items'][0]['payload']);
        $this->assertIsArray($catalog['items'][0]['compatibility']);

        $quote = $fixtures['quote'];
        foreach ([
            'base_monthly_eur',
            'configured_monthly_eur',
            'setup_fee_eur',
            'gst_amount_eur',
            'fx_rate',
            'fx_markup',
            'final_monthly',
            'final_total',
        ] as $field) {
            $this->assertIsFloat($quote[$field], $field . ' must remain a JSON number');
        }
        $this->assertIsArray($quote['breakdown']);
        $this->assertNotSame([], $quote['breakdown']);
    }

    public function testGoldenCatalogIsAcceptedByTheWhmcsConsumerWithoutProductWrites(): void
    {
        $catalog = $this->fixture('catalog');
        $item = $catalog['items'][0];

        $this->assertSame(
            hash('sha256', CatalogImportService::canonicalJson($item['payload'])),
            $item['payload_hash']
        );
        $hashable = $catalog;
        unset($hashable['payload_hash']);
        $this->assertSame(
            hash('sha256', CatalogImportService::canonicalJson($hashable)),
            $catalog['payload_hash']
        );

        $result = (new CatalogImportService())->import($catalog, 7);

        $this->assertTrue($result['created']);
        $this->assertSame(1, $result['item_count']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_items']);
        $this->assertArrayNotHasKey('tblproducts', Capsule::$tables);
        $this->assertArrayNotHasKey('tblpricing', Capsule::$tables);
    }

    public function testDocumentedWhmcsSchemaVersionMatchesInstaller(): void
    {
        $this->assertSame(16, Installer::SCHEMA_VERSION);
        $documentation = file_get_contents($this->repositoryRoot() . '/SCHEMA_VERSION.md');
        $this->assertIsString($documentation);
        $this->assertStringContainsString('## WHMCS DB 16 — current', $documentation);
    }

    /**
     * @return array<string,mixed>
     */
    private function fixture(string $name): array
    {
        $path = $this->repositoryRoot()
            . '/tests/fixtures/api/'
            . 'v'
            . self::API_SCHEMA_VERSION
            . '/'
            . $name
            . '.json';
        $this->assertFileExists($path, 'Required golden contract fixture is missing.');

        $decoded = json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
        $this->assertIsArray($decoded);
        return $decoded;
    }

    private function repositoryRoot(): string
    {
        return dirname(__DIR__, 5);
    }
}
