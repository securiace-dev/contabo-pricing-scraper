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
        Capsule::$columns['mod_contabo_catalog_versions'] = [
            'catalog_version', 'state', 'payload_hash', 'source_observed_at',
        ];
        Capsule::$columns['mod_contabo_catalog_items'] = [
            'catalog_version_id', 'machine_id', 'provider_id', 'item_type',
            'availability_state', 'payload_hash', 'payload_json',
        ];
        Capsule::$tables['mod_contabo_catalog_versions'] = [];
        Capsule::$tables['mod_contabo_catalog_items'] = [];
        Capsule::$tables['mod_contabo_settings'] = [];
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

    public function testEmptyItemsRejectedAndNoVersionRowWritten(): void
    {
        $catalog = $this->catalogWithPlans(0, 'catalog-empty');
        $r = (new CatalogImportService())->import($catalog, 9);
        $this->assertFalse($r['created']);
        $this->assertSame('empty_catalog', $r['error']);
        $this->assertCount(0, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertCount(0, Capsule::$tables['mod_contabo_catalog_items']);
    }

    public function testPlanCountDropBeyondDefaultThresholdRejected(): void
    {
        $svc = new CatalogImportService();
        $this->assertTrue($svc->import($this->catalogWithPlans(10, 'catalog-a'), 9)['created']);
        // 10 -> 7 is a 30% drop; default max is 20%.
        $r = $svc->import($this->catalogWithPlans(7, 'catalog-b'), 9);
        $this->assertFalse($r['created']);
        $this->assertSame('plan_count_drop', $r['error']);
        $this->assertSame(10, $r['previous_plan_count']);
        $this->assertSame(7, $r['new_plan_count']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
    }

    public function testSmallDropWithinThresholdAndGrowthAreAccepted(): void
    {
        $svc = new CatalogImportService();
        $svc->import($this->catalogWithPlans(10, 'catalog-a'), 9);
        $this->assertTrue($svc->import($this->catalogWithPlans(8, 'catalog-b'), 9)['created']); // exactly 20%
        $this->assertTrue($svc->import($this->catalogWithPlans(12, 'catalog-c'), 9)['created']);
    }

    public function testForceOverridesDropGuard(): void
    {
        $svc = new CatalogImportService();
        $svc->import($this->catalogWithPlans(10, 'catalog-a'), 9);
        $r = $svc->import($this->catalogWithPlans(2, 'catalog-b'), 9, ['force' => true]);
        $this->assertTrue($r['created']);
        $this->assertArrayNotHasKey('error', $r);
    }

    public function testDropThresholdReadFromSettings(): void
    {
        Capsule::$tables['mod_contabo_settings'] = [
            ['key' => 'scrape.max_drop_pct', 'value' => '50'],
        ];
        $svc = new CatalogImportService();
        $svc->import($this->catalogWithPlans(10, 'catalog-a'), 9);
        $this->assertTrue($svc->import($this->catalogWithPlans(6, 'catalog-b'), 9)['created']);
        $r = $svc->import($this->catalogWithPlans(2, 'catalog-c'), 9);
        $this->assertSame('plan_count_drop', $r['error']);
    }

    public function testImportStoresEnvelopeJson(): void
    {
        $catalog = $this->catalog();
        (new CatalogImportService())->import($catalog, 9);
        $stored = Capsule::$tables['mod_contabo_catalog_versions'][0]['envelope_json'];
        $this->assertSame(CatalogImportService::canonicalJson($catalog), $stored);
    }

    /**
     * @return array<string,mixed>
     */
    private function catalogWithPlans(int $n, string $version): array
    {
        $items = [];
        for ($i = 1; $i <= $n; $i++) {
            $payload = ['product_slug' => 'plan-' . $i, 'base_monthly_price' => 1.0 * $i];
            $items[] = [
                'machine_id' => 'plan:plan-' . $i,
                'provider_id' => null,
                'item_type' => 'plan',
                'label' => 'Plan ' . $i,
                'catalog_version' => $version,
                'effective_at' => '2026-07-30T10:20:30Z',
                'availability_state' => 'observed',
                'deprecated' => false,
                'compatibility' => [],
                'source_observed_at' => '2026-07-30T10:20:30Z',
                'payload_hash' => hash('sha256', CatalogImportService::canonicalJson($payload)),
                'payload' => $payload,
            ];
        }
        $catalog = [
            'schema_version' => '1.0',
            'source_version' => 'test',
            'source_observed_at' => '2026-07-30T10:20:30Z',
            'effective_at' => '2026-07-30T10:20:30Z',
            'catalog_version' => $version,
            'plans' => [],
            'profiles' => [],
            'items' => $items,
            'compatibility' => [],
            'configurations' => [],
        ];
        $catalog['payload_hash'] = hash('sha256', CatalogImportService::canonicalJson($catalog));
        return $catalog;
    }

    public function testCompatibilityModeRejectsImportBeforeAnyWrite(): void
    {
        unset(Capsule::$columns['mod_contabo_catalog_versions'], Capsule::$columns['mod_contabo_catalog_items']);
        unset(Capsule::$tables['mod_contabo_catalog_versions'], Capsule::$tables['mod_contabo_catalog_items']);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Catalog import is disabled');
        (new CatalogImportService())->import($this->catalog(), 9);
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
