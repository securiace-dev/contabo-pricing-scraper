<?php
declare(strict_types=1);

namespace ContaboPricing\Tests;

use ContaboPricing\CatalogImportService;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use WHMCS\Database\Capsule;

final class CatalogImportServiceTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
    }

    public function testImportsOnlyAddonOwnedVersionAndItems(): void
    {
        $catalog = $this->catalog();
        $result = (new CatalogImportService())->import($catalog, 9);

        $this->assertTrue($result['created']);
        $this->assertSame(1, $result['item_count']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_items']);
        $this->assertSame(
            'plan:cloud-vps-10',
            Capsule::$tables['mod_contabo_catalog_items'][0]['machine_id']
        );
        $this->assertArrayNotHasKey('tblproducts', Capsule::$tables);
        $this->assertArrayNotHasKey('tblpricing', Capsule::$tables);
    }

    public function testRepeatingSameVersionAndHashIsNoOp(): void
    {
        $service = new CatalogImportService();
        $catalog = $this->catalog();
        $service->import($catalog, 9);
        $result = $service->import($catalog, 9);

        $this->assertFalse($result['created']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_items']);
    }

    public function testTamperedEnvelopeIsRejectedBeforeWrite(): void
    {
        $catalog = $this->catalog();
        $catalog['items'][0]['label'] = 'Tampered';

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('payload hash does not match');
        try {
            (new CatalogImportService())->import($catalog, 9);
        } finally {
            $this->assertCount(0, Capsule::$tables['mod_contabo_catalog_versions']);
        }
    }

    /**
     * @return array<string,mixed>
     */
    private function catalog(): array
    {
        $payload = [
            'product_slug' => 'cloud-vps-10',
            'base_monthly_price' => 4.5,
        ];
        $item = [
            'machine_id' => 'plan:cloud-vps-10',
            'provider_id' => null,
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
            'plans' => [$payload],
            'profiles' => [],
            'items' => [$item],
            'compatibility' => [],
            'configurations' => [],
        ];
        $catalog['payload_hash'] = hash(
            'sha256',
            CatalogImportService::canonicalJson($catalog)
        );
        return $catalog;
    }
}
