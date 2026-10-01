<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\PlanExtractor;
use ContaboPricing\Scrape\PlanNormalizer;
use ContaboPricing\Scrape\RunValidator;
use ContaboPricing\Scrape\ScrapeSettings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

/** Round-2 lineup: dedicated + GPU specs, multi-currency, per-plan options. */
final class PlanNormalizerTest extends TestCase
{
    private const AT = '2026-10-01T10:00:00.000Z';

    /** @return array<string,array<string,mixed>> slug => plan of the live-shaped round-1 blob */
    private function round1(): array
    {
        $html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/round1_treg_anyapi.html');
        $by = [];
        foreach ((new PlanExtractor())->extract($html, null, self::AT)->plans as $p) {
            $by[$p['product_slug']] = $p;
        }
        return $by;
    }

    public function testCpuAcceptsVirtualCoresAndTheDedicatedGhzForm(): void
    {
        $this->assertSame(4, PlanNormalizer::parseCpuCount('4 vCPU Cores'));
        $this->assertSame(3, PlanNormalizer::parseCpuCount('3 Physical Cores'));
        $this->assertSame(6, PlanNormalizer::parseCpuCount('6 Virtual Cores'));
        $this->assertSame(32, PlanNormalizer::parseCpuCount('32 x 3.55 GHz (4.20 max)'));
        $this->assertSame(12, PlanNormalizer::parseCpuCount('12 x 3.70 GHz'));
        $this->assertNull(PlanNormalizer::parseCpuCount('AMD Ryzen'));
    }

    public function testStorageSumsDrivesTakesTheFirstOfADualLabelAndIgnoresTrailingSpace(): void
    {
        $this->assertSame(2000, PlanNormalizer::parseStorageGb('2 x 1 TB NVMe'));
        $this->assertSame(300, PlanNormalizer::parseStorageGb('300GB SSD / 150GB NVMe'));
        $this->assertSame(1000, PlanNormalizer::parseStorageGb('1 TB NVMe '));
        $this->assertSame(1400, PlanNormalizer::parseStorageGb('1.4 TB SSD'));
        $this->assertSame('NVMe', PlanNormalizer::parseStorageType('2 x 1 TB NVMe'));
        $this->assertSame('SSD', PlanNormalizer::parseStorageType('300GB SSD / 150GB NVMe'), 'primary = first');
        $this->assertSame('NVMe', PlanNormalizer::parseStorageType('75 GB NVMe or 150 GB SSD'));
        $this->assertSame('HDD', PlanNormalizer::parseStorageType('4 TB HDD'));
        $this->assertSame('SSD', PlanNormalizer::parseStorageType(''));
    }

    public function testNormalizesAGpuProduct(): void
    {
        $plan = (new PlanNormalizer())->normalize([
            'slug' => 'gpu-vps-plus-18', 'title' => 'GPU VPS', 'type' => 'vps',
            'price' => ['EUR' => 999, 'USD' => 1199, 'GBP' => 969], 'previousPrice' => null,
            'periods' => [['length' => 1, 'discount' => ['EUR' => 0]], ['length' => 12, 'discount' => ['EUR' => 1798.2]]],
            'specs' => [
                ['title' => 'GPU - NVIDIA RTX 6000', 'type' => null],
                ['title' => '18 vCPU Cores', 'type' => 'cpu'], ['title' => '96 GB RAM', 'type' => 'ram'],
                ['title' => '900 GB NVMe', 'type' => 'storage'], ['title' => '5 Snapshots', 'type' => 'snapshot'],
                ['title' => '1 Gbit/s Port', 'type' => 'port'], ['title' => 'Unlimited Traffic*', 'type' => 'traffic'],
            ],
        ], self::AT, null, ['family' => 'GPU VPS', 'family_key' => 'gpu-vps']);

        $sp = $plan['specs_parsed'];
        $this->assertSame('NVIDIA RTX 6000', $sp['gpu']);
        $this->assertSame([18, 96, 900, 'NVMe', 5, 1000, 'unlimited'], [
            $sp['cpu_count'], $sp['ram_gb'], $sp['storage_primary_gb'], $sp['storage_primary_type'],
            $sp['snapshot_count'], $sp['port_speed_mbps'], $sp['traffic'],
        ]);
        $this->assertSame(['EUR' => 999, 'USD' => 1199, 'GBP' => 969], $plan['prices']);
        $this->assertSame(999, $plan['base_monthly_price']);
        $this->assertNull($plan['previous_price']);
        $this->assertSame('GPU VPS', $plan['family']);
        $this->assertArrayNotHasKey('options', $plan, 'no add-ons in the record: no options key');

        $nonGpu = (new PlanNormalizer())->normalize(['slug' => 'x', 'type' => 'vps', 'price' => ['EUR' => 1]], self::AT, null);
        $this->assertArrayNotHasKey('gpu', $nonGpu['specs_parsed']);
    }

    public function testMultiCurrencyPricesAndPreviousPriceAreCarried(): void
    {
        $by = $this->round1();
        $ryzen = $by['amd-ryzen-12-cores'];
        $this->assertSame(['EUR' => 129, 'USD' => 155, 'GBP' => 126], $ryzen['prices']);
        $this->assertSame(['EUR' => 149, 'GBP' => 146, 'USD' => 179], $ryzen['previous_price']);
        $this->assertSame(129, $ryzen['base_monthly_price'], 'base_monthly_price stays EUR for compatibility');
        $this->assertNull($by['cloud-vps-core-4']['previous_price']);
    }

    public function testDedicatedAndGpuPlansAreSchemaComplete(): void
    {
        $by = $this->round1();
        $ds = $by['ds-40']['specs_parsed'];
        $this->assertSame([32, 128, 2000, 'NVMe'], [$ds['cpu_count'], $ds['ram_gb'], $ds['storage_primary_gb'], $ds['storage_primary_type']]);
        $this->assertSame('300GB SSD / 150GB NVMe', $by['storage-vps-10']['base_storage']);
        $this->assertSame([300, 'SSD'], [$by['storage-vps-10']['specs_parsed']['storage_primary_gb'], $by['storage-vps-10']['specs_parsed']['storage_primary_type']]);

        Capsule::reset();
        $r = (new RunValidator())->evaluate(array_values($by), [], null, new ScrapeSettings());
        $this->assertTrue($r['gates']['schema_completeness']['ok'], json_encode($r['gates']['schema_completeness']['detail']));
        $this->assertTrue($r['gates']['price_sanity']['ok']);
        // the dedicated plans have no snapshot spec at all: optional, never a completeness failure
        $this->assertNull($by['ds-40']['specs_parsed']['snapshot_count']);
    }

    public function testOptionsMatchTheCheckoutScreenshotGroundTruth(): void
    {
        $by = $this->round1();
        $o = $by['cloud-vps-core-4']['options'];
        $this->assertSame(
            ['regions', 'storage_upgrades', 'backup', 'object_storage', 'os_images', 'apps', 'panels', 'monitoring', 'other'],
            array_keys($o)
        );
        $india = array_column($o['regions'], null, 'name')['Asia (India)'];
        $this->assertEquals(2.4, $india['monthly_price']);
        $this->assertArrayHasKey('setup_price', $india);
        $this->assertEquals(1.5, array_column($o['storage_upgrades'], 'monthly_price', 'name')['200 GB SSD']);
        $this->assertEquals(1.65, $o['backup']['monthly_price']);
        $this->assertEquals(11.96, array_column($o['object_storage'], 'monthly_price', 'name')['1 TB Object Storage in Asia'], '2.99 x 1000 / 250');
        $this->assertSame(14.11, round(11.96 * 1.18, 2));
        $this->assertNotEmpty($o['os_images']);
        $this->assertNotEmpty($o['panels']);
        $this->assertSame(999, $by['gpu-vps-plus-18']['prices']['EUR']);
    }
}
