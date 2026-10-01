<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\RunValidator;
use ContaboPricing\Scrape\ScrapeSettings;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class RunValidatorTest extends TestCase
{
    /** @var ScrapeSettings */
    private $s;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->s = new ScrapeSettings();
        $this->s->set('scrape.min_plans_cloud_vps', '2');
        $this->s->set('scrape.min_plans_storage_vps', '1');
        $this->s->set('scrape.min_plans_cloud_vds', '1');
    }

    /** @return array<string,mixed> */
    public static function plan(string $slug, string $family, float $base, array $over = []): array
    {
        return array_replace_recursive([
            'family' => $family,
            'product_slug' => $slug,
            'product_name' => strtoupper($slug),
            'base_monthly_price' => $base,
            'periods' => [['months' => 1, 'effective_monthly' => $base], ['months' => 12, 'effective_monthly' => round($base * 0.8, 2)]],
            'specs_parsed' => ['cpu_count' => 4, 'ram_gb' => 8, 'storage_primary_gb' => 200, 'port_speed_mbps' => 200, 'storage_primary_type' => 'SSD'],
            'cpu' => '4 vCPU Cores', 'ram' => '8 GB RAM', 'base_storage' => '200 GB SSD', 'snapshots' => '1 Snapshot', 'port' => '200 Mbit/s',
        ], $over);
    }

    /** @return list<array<string,mixed>> */
    private function good(): array
    {
        return [
            self::plan('cloud-vps-10', 'Cloud VPS', 5.0),
            self::plan('cloud-vps-20', 'Cloud VPS', 9.0),
            self::plan('storage-vps-10', 'Storage VPS', 6.0),
            self::plan('vds-s', 'Cloud VDS', 30.0),
        ];
    }

    private function eval(array $plans, ?array $last = null, array $attempts = []): array
    {
        return (new RunValidator())->evaluate($plans, $attempts, $last, $this->s);
    }

    public function testAllGatesPassOnFirstRun(): void
    {
        $r = $this->eval($this->good());
        $this->assertTrue($r['passed']);
        $this->assertSame(
            ['min_plans_per_family', 'count_drop_pct', 'schema_completeness', 'price_sanity', 'cross_page_consistency', 'dom_cross_check', 'label_guard'],
            array_keys($r['gates'])
        );
        $this->assertSame([], $r['anomalies']);
        $this->assertCount(4, $r['safe']);
    }

    public function testMinPlansBoundary(): void
    {
        $plans = $this->good();
        $this->assertTrue($this->eval($plans)['gates']['min_plans_per_family']['ok']);
        unset($plans[1]); // cloud_vps now 1 < 2
        $r = $this->eval(array_values($plans));
        $this->assertFalse($r['gates']['min_plans_per_family']['ok']);
        $this->assertSame(1, $r['gates']['min_plans_per_family']['detail']['Cloud VPS']['count']);
    }

    public function testCountDropBoundary(): void
    {
        $this->s->set('scrape.max_drop_pct', '25');
        $last = $this->good(); // 4 plans
        $this->s->set('scrape.min_plans_cloud_vps', '1');
        $this->s->set('scrape.min_plans_storage_vps', '0');
        $this->s->set('scrape.min_plans_cloud_vds', '0');
        // 4 -> 3 is exactly 25%: allowed
        $three = array_slice($last, 0, 3);
        $this->assertTrue($this->eval($three, $last)['gates']['count_drop_pct']['ok']);
        // 4 -> 2 is 50%: blocked
        $this->assertFalse($this->eval(array_slice($last, 0, 2), $last)['gates']['count_drop_pct']['ok']);
        // growth never trips
        $this->assertTrue($this->eval($last, $three)['gates']['count_drop_pct']['ok']);
    }

    public function testSchemaCompleteness(): void
    {
        $cases = [
            'no periods' => ['periods' => []],
            'zero base' => ['base_monthly_price' => 0.0],
            'null cpu' => ['specs_parsed' => ['cpu_count' => null]],
            'null ram' => ['specs_parsed' => ['ram_gb' => null]],
            'null storage' => ['specs_parsed' => ['storage_primary_gb' => null]],
        ];
        foreach ($cases as $name => $over) {
            $plans = $this->good();
            $p = $plans[0];
            foreach ($over as $k => $v) {
                $p[$k] = is_array($v) && $k === 'specs_parsed' ? array_replace($p[$k], $v) : $v;
            }
            $plans[0] = $p;
            $this->assertFalse($this->eval($plans)['gates']['schema_completeness']['ok'], $name);
        }
    }

    public function testPriceSanityEffectiveBoundary(): void
    {
        $plans = $this->good();
        $plans[0]['periods'][0]['effective_monthly'] = 5.05; // exactly base*1.01
        $this->assertTrue($this->eval($plans)['gates']['price_sanity']['ok']);
        $plans[0]['periods'][0]['effective_monthly'] = 5.06;
        $this->assertFalse($this->eval($plans)['gates']['price_sanity']['ok']);
        $plans[0]['periods'][0]['effective_monthly'] = 0.0;
        $this->assertFalse($this->eval($plans)['gates']['price_sanity']['ok']);
    }

    public function testPriceChangeBoundary(): void
    {
        $this->s->set('scrape.max_price_change_pct', '50');
        $last = $this->good();
        $up = $this->good();
        $up[0] = self::plan('cloud-vps-10', 'Cloud VPS', 7.5); // +50% exactly
        $r = $this->eval($up, $last);
        $this->assertTrue($r['gates']['price_sanity']['ok']);
        $this->assertSame('cost_increase', $r['risky'][0]['kind']);
        $up[0] = self::plan('cloud-vps-10', 'Cloud VPS', 7.51);
        $this->assertFalse($this->eval($up, $last)['gates']['price_sanity']['ok']);
    }

    public function testCrossPageConsistency(): void
    {
        $a = self::plan('cloud-vps-10', 'Cloud VPS', 5.0);
        $b = self::plan('cloud-vps-10', 'Cloud VPS', 5.0);
        $ok = $this->eval($this->good(), null, [['plans' => [$a]], ['plans' => [$b]]]);
        $this->assertTrue($ok['gates']['cross_page_consistency']['ok']);
        $b['base_monthly_price'] = 5.5;
        $bad = $this->eval($this->good(), null, [['plans' => [$a]], ['plans' => [$b]]]);
        $this->assertFalse($bad['gates']['cross_page_consistency']['ok']);
        $this->assertSame(['cloud-vps-10'], $bad['gates']['cross_page_consistency']['detail']['conflicting_slugs']);
    }

    public function testDomCrossCheckOnlyFailsOnMismatch(): void
    {
        $info = $this->eval($this->good(), null, [['warnings' => ['dom_probe: could not read the page plan title/price']]]);
        $this->assertTrue($info['gates']['dom_cross_check']['ok']);
        $bad = $this->eval($this->good(), null, [['warnings' => ['dom_probe mismatch for cloud-vps-10: payload 5.00 vs page 4.00']]]);
        $this->assertFalse($bad['gates']['dom_cross_check']['ok']);
    }

    public function testLabelGuardScansDescriptiveFieldsNotUpstreamIdentifiers(): void
    {
        $plans = $this->good();
        $this->assertTrue($this->eval($plans)['gates']['label_guard']['ok'], 'slug/name "cloud-vps" is an upstream identifier');
        $plans[0]['display_name'] = 'Powered by Contabo';
        $r = $this->eval($plans);
        $this->assertFalse($r['gates']['label_guard']['ok']);
        $this->assertSame(['contabo'], $r['gates']['label_guard']['detail']['hits']['cloud-vps-10']['display_name']);
    }

    public function testUpstreamRedirectIsAWarningNotAFailure(): void
    {
        $r = $this->eval($this->good(), null, [[
            'url' => 'https://contabo.com/en/vps/cloud-vps-10/',
            'final_url' => 'https://contabo.com/en/vps/cloud-vps-core-4/',
        ]]);
        $this->assertTrue($r['passed']);
        $this->assertSame(['upstream redirect: cloud-vps-10 -> cloud-vps-core-4'], $r['warnings']);
        $this->assertSame([], $this->eval($this->good())['warnings']);
    }

    public function testBucketsAndScrapeFailed(): void
    {
        $last = $this->good();
        $r = $this->eval([], $last);
        $this->assertSame('scrape_failed', $r['anomalies'][0]['kind']);
        $this->assertFalse($r['passed']);

        $next = $this->good();
        array_pop($next); // retire vds-s => risky
        $next[0]['specs_parsed']['cpu_count'] = 8; // spec_increase => safe
        $this->s->set('scrape.max_drop_pct', '50');
        $r = $this->eval($next, $last);
        $this->assertSame(['plan_retired'], array_column($r['risky'], 'kind'));
        $this->assertSame(['spec_increase'], array_column($r['safe'], 'kind'));
    }

    public function testParserErrorBecomesAnomaly(): void
    {
        $last = $this->good();
        $next = $this->good();
        $next[0]['specs_parsed']['ram_gb'] = null;
        $r = $this->eval($next, $last);
        $this->assertSame(['parser_error'], array_column($r['anomalies'], 'kind'));
        $this->assertFalse($r['gates']['schema_completeness']['ok']);
    }
}
