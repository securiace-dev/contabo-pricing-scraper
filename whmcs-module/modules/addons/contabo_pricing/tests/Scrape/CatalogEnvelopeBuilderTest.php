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
        $this->assertCount(20, $plans);

        $envelope = (new CatalogEnvelopeBuilder())->build(
            $plans,
            ['plans' => []],
            ['option_catalog' => []],
            '2026-07-30T10:20:30Z',
            'whmcs-native-scrape'
        );

        $result = (new CatalogImportService())->import($envelope, 7);

        $this->assertTrue($result['created']);
        $this->assertSame(20, $result["item_count"]);
        $this->assertSame($envelope['catalog_version'], $result['catalog_version']);
        $this->assertSame($envelope['payload_hash'], $result['payload_hash']);
        $this->assertCount(1, Capsule::$tables['mod_contabo_catalog_versions']);
        $this->assertCount(20, Capsule::$tables["mod_contabo_catalog_items"]);

        // Re-importing the identical envelope is idempotent.
        $again = (new CatalogImportService())->import($envelope, 7);
        $this->assertFalse($again['created']);
        $this->assertCount(20, Capsule::$tables["mod_contabo_catalog_items"]);
    }

    public function testPlanOptionsBecomeConfigurationItemsThatImport(): void
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/round1_treg_anyapi.html');
        $plans = (new PlanExtractor())->extract($html, null, '2026-10-01T10:00:00.000Z')->plans;
        $this->assertCount(27, $plans);
        $configs = CatalogEnvelopeBuilder::configurationsFromPlans($plans);
        $this->assertCount(27, $configs['plans']);
        $c4 = $configs['plans']['cloud-vps-core-4'];
        $this->assertSame('Core VPS', $c4['family']);
        $labels = array_column($c4['options']['regions'], 'monthly_price', 'option_label');
        $this->assertEquals(2.4, $labels['Asia (India)']);
        $this->assertSame('regions', $c4['options']['regions'][0]['category']);
        $this->assertContains('Auto Backup', array_column($c4['options']['backup'], 'option_label'));

        $envelope = (new CatalogEnvelopeBuilder())->build($plans, $configs, ['option_catalog' => []], '2026-10-01T10:00:00Z', 'whmcs-native-scrape');
        $ids = array_column($envelope['items'], 'machine_id');
        $this->assertSame($ids, array_values(array_unique($ids)), 'machine ids are unique');
        $this->assertLessThanOrEqual(191, max(array_map('strlen', $ids)));
        $this->assertGreaterThan(27 * 20, count($envelope['items']));

        $r = (new CatalogImportService())->import($envelope, 1);
        $this->assertTrue($r['created']);
        $this->assertSame(count($envelope['items']), $r['item_count']);
        // payload.options rides along on the plan item itself
        $planItem = null;
        foreach ($envelope['items'] as $it) {
            if ($it['machine_id'] === 'plan:cloud-vps-core-4') {
                $planItem = $it;
            }
        }
        $this->assertArrayHasKey('options', $planItem['payload']);
        $this->assertSame(['EUR' => 5.5, 'USD' => 6.6, 'GBP' => 5.4], $planItem['payload']['prices']);
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
