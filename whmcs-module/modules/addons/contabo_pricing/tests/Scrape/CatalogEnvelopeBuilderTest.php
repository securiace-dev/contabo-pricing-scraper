<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\CatalogImportService;
use ContaboPricing\Scrape\CatalogEnvelopeBuilder;
use ContaboPricing\Scrape\PlanExtractor;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class CatalogEnvelopeBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        Capsule::reset();
    }

    /** @return array<string,mixed> */
    private function rustFixture(): array
    {
        $path = __DIR__ . '/../../../../../../tests/fixtures/api/v1.1/catalog.json';
        $this->assertFileExists($path);
        $decoded = json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
        $this->assertIsArray($decoded);
        return $decoded;
    }

    public function testHashParityWithRustFixture(): void
    {
        $fixture = $this->rustFixture();

        $built = (new CatalogEnvelopeBuilder())->build(
            $fixture['plans'],
            $fixture['configurations'],
            $fixture['compatibility'],
            $fixture['source_observed_at'],
            $fixture['source_version'],
            'catalog-contract-v1'
        );

        $this->assertSame($fixture['payload_hash'], $built['payload_hash'], 'top-level payload_hash');
        $this->assertSame($fixture['catalog_version'], $built['catalog_version']);
        $this->assertCount(count($fixture['items']), $built['items']);
        foreach ($fixture['items'] as $i => $item) {
            $this->assertSame($item['payload_hash'], $built['items'][$i]['payload_hash'], 'item ' . $i);
            $this->assertSame($item['machine_id'], $built['items'][$i]['machine_id']);
            $this->assertSame($item['provider_id'], $built['items'][$i]['provider_id']);
        }
        $this->assertEquals($fixture, $built, 'whole envelope is structurally identical');
    }

    public function testCatalogVersionIsDeterministic(): void
    {
        $plans = [['product_slug' => 'cloud-vps-10', 'product_name' => 'Cloud VPS 10', 'family' => 'Cloud VPS', 'base_monthly_price' => 4.5]];
        $b = new CatalogEnvelopeBuilder();
        $a1 = $b->build($plans, ['plans' => []], ['option_catalog' => []], '2026-07-30T10:20:30Z', 'v1');
        $a2 = $b->build($plans, ['plans' => []], ['option_catalog' => []], '2026-07-30T10:20:30Z', 'v1');

        $this->assertSame($a1['catalog_version'], $a2['catalog_version']);
        $this->assertSame($a1['payload_hash'], $a2['payload_hash']);
        $this->assertMatchesRegularExpression('/^catalog-20260730102030-[a-f0-9]{16}$/', $a1['catalog_version']);

        $plans[0]['base_monthly_price'] = 4.6;
        $a3 = $b->build($plans, ['plans' => []], ['option_catalog' => []], '2026-07-30T10:20:30Z', 'v1');
        $this->assertNotSame($a1['catalog_version'], $a3['catalog_version']);
        $this->assertNotSame($a1['payload_hash'], $a3['payload_hash']);

        $empty = $b->build([], [], [], '', 'v1');
        $this->assertStringStartsWith('catalog-unknown-', $empty['catalog_version']);
    }

    public function testMarketingSlugIsNotPresentedAsProviderId(): void
    {
        $e = (new CatalogEnvelopeBuilder())->build(
            [['product_slug' => 'cloud-vps-10', 'product_name' => 'Cloud VPS 10', 'family' => 'Cloud VPS']],
            ['plans' => ['u' => ['slug' => 'cloud-vps-10', 'options' => ['Image' => [
                ['category' => 'OS', 'option_label' => 'Ubuntu 24.04', 'provider_id' => 'image-ubuntu-2404'],
            ]]]]],
            ['dimensions' => []],
            '2026-07-30T10:20:30Z',
            'test'
        );
        $this->assertSame('plan:cloud-vps-10', $e['items'][0]['machine_id']);
        $this->assertNull($e['items'][0]['provider_id']);
        $this->assertSame('option:cloud-vps-10:image:os:ubuntu-24-04', $e['items'][1]['machine_id']);
        $this->assertSame('image-ubuntu-2404', $e['items'][1]['provider_id']);
        $this->assertSame('configuration_option', $e['items'][1]['item_type']);
        $this->assertSame(
            ['plan_machine_id' => 'plan:cloud-vps-10', 'dimension' => 'Image', 'category' => 'OS'],
            $e['items'][1]['compatibility']
        );
    }

    public function testMachinePartMatchesRust(): void
    {
        $this->assertSame('ubuntu-24-04', CatalogEnvelopeBuilder::machinePart('Ubuntu 24.04'));
        $this->assertSame('a-b', CatalogEnvelopeBuilder::machinePart('--A  &  B--'));
        $this->assertSame('', CatalogEnvelopeBuilder::machinePart('***'));
    }

    public function testEnvelopeImportsThroughExistingImporter(): void
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
        $plans = (new PlanExtractor())->extract($html, null, '2026-07-30T10:20:30.000Z')->plans;
        $this->assertCount(16, $plans);

        $envelope = (new CatalogEnvelopeBuilder())->build(
            $plans,
            ['plans' => []],
            ['option_catalog' => []],
            '2026-07-30T10:20:30Z',
            'whmcs-native-scrape'
        );

        $result = (new CatalogImportService())->import($envelope, 7);

        $this->assertTrue($result['created']);
        $this->assertSame(16, $result['item_count']);
        $this->assertSame($envelope['catalog_version'], $result['catalog_version']);
        $this->assertSame($envelope['payload_hash'], $result['payload_hash']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertCount(16, Capsule::$tables['mod_contabo_catalog_items']);

        // Re-importing the identical envelope is idempotent.
        $again = (new CatalogImportService())->import($envelope, 7);
        $this->assertFalse($again['created']);
        $this->assertCount(16, Capsule::$tables['mod_contabo_catalog_items']);
    }

    public function testConfigurationOptionsImportToo(): void
    {
        $envelope = (new CatalogEnvelopeBuilder())->build(
            [['product_slug' => 'cloud-vps-10', 'product_name' => 'Cloud VPS 10', 'family' => 'Cloud VPS']],
            ['plans' => ['u' => ['slug' => 'cloud-vps-10', 'options' => ['Region' => [
                ['category' => 'default', 'option_label' => 'European Union'],
                ['option_label' => 'United States'],
            ]]]]],
            ['option_catalog' => []],
            '2026-07-30T10:20:30Z',
            'x'
        );
        $result = (new CatalogImportService())->import($envelope);
        $this->assertSame(3, $result['item_count']);
    }
}
