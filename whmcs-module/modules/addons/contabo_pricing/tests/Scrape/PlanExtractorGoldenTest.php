<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\ExtractionResult;
use ContaboPricing\Scrape\PlanExtractor;
use ContaboPricing\Scrape\PlanNormalizer;
use ContaboPricing\Scrape\PlanUrlList;
use PHPUnit\Framework\TestCase;

final class PlanExtractorGoldenTest extends TestCase
{
    private const AT = '2026-07-30T10:20:30.000Z';

    /** @var string */
    private $html;

    protected function setUp(): void
    {
        $this->html = (string) file_get_contents(__DIR__ . '/../fixtures/scrape/cloud_vps_10.html');
    }

    /** @return array<string,array<string,mixed>> slug => plan */
    private function plansBySlug(ExtractionResult $r): array
    {
        $by = [];
        foreach ($r->plans as $p) {
            $by[$p['product_slug']] = $p;
        }
        return $by;
    }

    public function testExtractsAll16PlansViaSapperWithFamilyCounts(): void
    {
        $r = (new PlanExtractor())->extract($this->html, null, self::AT);

        $this->assertSame(ExtractionResult::STRATEGY_SAPPER, $r->strategy);
        $this->assertTrue($r->sapperPresent);
        $this->assertTrue($r->importable());
        $this->assertCount(16, $r->plans);
        $this->assertSame([], $r->warnings);

        $families = [];
        foreach ($r->plans as $p) {
            $families[$p['family']] = ($families[$p['family']] ?? 0) + 1;
        }
        $this->assertSame(['Cloud VPS' => 6, 'Storage VPS' => 5, 'Cloud VDS' => 5], $families);

        $slugs = array_map(static function (array $p): string {
            return $p['product_slug'];
        }, $r->plans);
        $expected = array_map([PlanUrlList::class, 'slugFromUrl'], PlanUrlList::DEFAULT_URLS);
        $this->assertSame($expected, $slugs);
    }

    public function testCloudVps10Golden(): void
    {
        $p = $this->plansBySlug((new PlanExtractor())->extract($this->html, null, self::AT))['cloud-vps-10'];

        $this->assertSame('Cloud VPS', $p['family']);
        $this->assertSame('Cloud VPS 10', $p['product_name']);
        $this->assertSame('https://contabo.com/en/vps/cloud-vps-10/', $p['product_url']);
        $this->assertSame(1, $p['plan_rank']);
        $this->assertSame(1, $p['plan_family_rank']);
        $this->assertSame(self::AT, $p['fetched_at']);
        $this->assertSame('sapper', $p['source']);
        $this->assertSame(4.5, $p['base_monthly_price']);
        $this->assertSame('4 vCPU Cores', $p['cpu']);
        $this->assertSame('8 GB RAM', $p['ram']);
        $this->assertSame('1 Snapshot', $p['snapshots']);
        $this->assertSame('200 Mbit/s Port', $p['port']);
        $this->assertStringStartsWith('75 GB NVMe', $p['base_storage']);

        $by = [];
        foreach ($p['periods'] as $per) {
            $by[$per['months']] = $per;
        }
        $this->assertSame([1, 3, 6, 12], array_keys($by));

        $this->assertSame(
            ['months' => 1, 'is_hidden_from_ui' => false, 'effective_monthly' => 4.5, 'setup_fee' => 4.5, 'total_period_cost' => 9, 'discount_total' => 0],
            $by[1]
        );
        $this->assertTrue($by[3]['is_hidden_from_ui'], 'the 3-month period is hidden from the UI');
        $this->assertFalse($by[1]['is_hidden_from_ui']);
        $this->assertSame(
            ['months' => 6, 'is_hidden_from_ui' => false, 'effective_monthly' => 4.05, 'setup_fee' => 2.25, 'total_period_cost' => 26.55, 'discount_total' => 2.7],
            $by[6]
        );
        $this->assertSame(
            ['months' => 12, 'is_hidden_from_ui' => false, 'effective_monthly' => 3.6, 'setup_fee' => 0, 'total_period_cost' => 43.2, 'discount_total' => 10.8],
            $by[12]
        );

        $this->assertSame(
            ['cpu_count' => 4, 'ram_gb' => 8, 'port_speed_mbps' => 200, 'storage_primary_gb' => 75, 'storage_primary_type' => 'NVMe'],
            $p['specs_parsed']
        );
        $this->assertSame(
            ['min_length' => 8, 'max_length' => 30, 'alphanumeric_only' => true, 'no_special_chars' => true],
            $p['password_rules']
        );
    }

    public function testWholeNumbersAreEmittedAsIntegersInJson(): void
    {
        $p = $this->plansBySlug((new PlanExtractor())->extract($this->html, null, self::AT))['cloud-vps-10'];
        $json = (string) json_encode($p['periods'][0]);
        $this->assertStringContainsString('"total_period_cost":9,', $json);
        $this->assertStringContainsString('"discount_total":0', $json);
        $this->assertStringContainsString('"effective_monthly":4.5', $json);
        $this->assertSame(8, $p['specs_parsed']['ram_gb']);
    }

    public function testVdsAndStorageSpecificGoldens(): void
    {
        $by = $this->plansBySlug((new PlanExtractor())->extract($this->html, null, self::AT));

        $s = $by['vds-s'];
        $this->assertSame('Cloud VDS', $s['family']);
        $this->assertSame(34.4, $s['base_monthly_price']);
        $this->assertSame('3 Physical Cores', $s['cpu']);
        $this->assertSame(3, $s['specs_parsed']['cpu_count']);
        $this->assertSame(180, $s['specs_parsed']['storage_primary_gb']);
        $this->assertSame('180 GB NVMe', $s['base_storage'], 'VDS keeps the title only and trims the trailing space');
        $this->assertSame(12, $s['plan_rank']);
        $this->assertSame(1, $s['plan_family_rank']);

        $st = $by['storage-vps-50'];
        $this->assertSame('Storage VPS', $st['family']);
        $this->assertSame(1400, $st['specs_parsed']['storage_primary_gb']);
        $this->assertSame('SSD', $st['specs_parsed']['storage_primary_type']);
        $this->assertSame('1.4 TB SSD', $st['base_storage']);
        $this->assertSame(11, $st['plan_rank']);
        $this->assertSame(5, $st['plan_family_rank']);
    }

    public function testEveryPlanHasConsistentPeriodMath(): void
    {
        $r = (new PlanExtractor())->extract($this->html, null, self::AT);
        foreach ($r->plans as $p) {
            $this->assertGreaterThan(0, $p['base_monthly_price'], $p['product_slug']);
            $this->assertNotSame([], $p['periods'], $p['product_slug']);
            foreach ($p['periods'] as $per) {
                $gross = $p['base_monthly_price'] * $per['months'];
                $this->assertEqualsWithDelta(
                    $gross - $per['discount_total'] + $per['setup_fee'],
                    $per['total_period_cost'],
                    0.011,
                    $p['product_slug'] . ' ' . $per['months']
                );
                $this->assertEqualsWithDelta(
                    ($gross - $per['discount_total']) / $per['months'],
                    $per['effective_monthly'],
                    0.0051,
                    $p['product_slug'] . ' ' . $per['months']
                );
            }
        }
    }

    // ── Strategy fallbacks ──────────────────────────────────────────────────

    public function testProviderJsonStrategyRecomputesEveryNumber(): void
    {
        $json = ['products' => [[
            'slug' => 'cloud-vps-10',
            'title' => 'Cloud VPS 10',
            'type' => 'vps',
            'price_eur' => 4.5,
            'total_period_cost' => 99999,
            'periods' => [
                ['length' => 1, 'discount_eur' => null, 'setup_eur' => 4.5, 'effective_monthly' => 123],
                ['length' => 6, 'discount_eur' => 2.7, 'setup_eur' => 2.25],
            ],
            'specs' => [
                ['type' => 'cpu', 'title' => '4 vCPU Cores', 'subtitle' => null],
                ['type' => 'storage', 'title' => '75 GB NVMe', 'subtitle' => 'or 150 GB SSD'],
            ],
        ]]];
        $r = (new PlanExtractor())->extract(null, $json, self::AT);

        $this->assertSame(ExtractionResult::STRATEGY_PROVIDER_JSON, $r->strategy);
        $this->assertTrue($r->importable());
        $p = $r->plans[0];
        $this->assertSame(9, $p['periods'][0]['total_period_cost']);
        $this->assertSame(4.5, $p['periods'][0]['effective_monthly']);
        $this->assertSame(26.55, $p['periods'][1]['total_period_cost']);
        $this->assertSame('75 GB NVMe or 150 GB SSD', $p['base_storage']);
        $this->assertNull($p['password_rules']);
        $this->assertContains('missing plan: cloud-vps-20', $r->warnings);
    }

    public function testProviderJsonWithMalformedProductsIsNotImportable(): void
    {
        $r = (new PlanExtractor())->extract(null, ['products' => [['slug' => 'cloud-vps-10', 'type' => 'vps']]], self::AT);
        $this->assertSame([], $r->plans);
        $this->assertFalse($r->importable());
    }

    public function testDomProbeOnlyIsNeverImportable(): void
    {
        $noSapper = (string) preg_replace('/__SAPPER__=/', '__NOPE__=', $this->html);
        $r = (new PlanExtractor())->extract($noSapper, null, self::AT);

        $this->assertSame(ExtractionResult::STRATEGY_DOM_PROBE, $r->strategy);
        $this->assertFalse($r->sapperPresent);
        $this->assertSame([], $r->plans);
        $this->assertFalse($r->importable());
        $this->assertSame('Cloud VPS 10', $r->probe['title']);
        $this->assertSame(4.5, $r->probe['monthly_eur']);
    }

    public function testSapperDecodeFailureFallsThroughWithWarning(): void
    {
        $r = (new PlanExtractor())->extract('<script>__SAPPER__={x:evil()}</script>', null, self::AT);
        $this->assertTrue($r->sapperPresent);
        $this->assertSame(ExtractionResult::STRATEGY_DOM_PROBE, $r->strategy);
        $this->assertStringContainsString('sapper:', $r->warnings[0]);
    }

    public function testNothingToExtract(): void
    {
        $r = (new PlanExtractor())->extract(null, null, self::AT);
        $this->assertSame(ExtractionResult::STRATEGY_NONE, $r->strategy);
        $this->assertFalse($r->importable());
    }

    public function testDomProbeCrossCheckFlagsPriceMismatch(): void
    {
        $html = $this->html;
        $needle = '<strong class="svelte-2dv52c">€4.50</strong>';
        $pos = strpos($html, $needle);
        $this->assertNotFalse($pos);
        $html = substr_replace($html, '<strong class="svelte-2dv52c">€9.99</strong>', (int) $pos, strlen($needle));

        $r = (new PlanExtractor())->extract($html, null, self::AT);
        $this->assertSame(ExtractionResult::STRATEGY_SAPPER, $r->strategy);
        $found = false;
        foreach ($r->warnings as $w) {
            if (strpos($w, 'dom_probe mismatch for cloud-vps-10') === 0) {
                $found = true;
            }
        }
        $this->assertTrue($found, implode(' | ', $r->warnings));
    }

    // ── Normalizer parity helpers ───────────────────────────────────────────

    /** Values verified against Rust `format!("{v:.2}")` parsed back to f64. */
    public function testRound2MatchesRustFormat(): void
    {
        $pairs = [
            [0.125, 0.12], [0.375, 0.38], [0.625, 0.62], [0.875, 0.88], [2.675, 2.67],
            [4.675, 4.67], [1.005, 1.0], [0.005, 0.01], [0.015, 0.01], [0.025, 0.03],
            [1.115, 1.11], [2.5, 2.5], [9.995, 9.99], [26.55, 26.55], [43.2, 43.2],
        ];
        foreach ($pairs as [$in, $out]) {
            $this->assertSame($out, PlanNormalizer::round2($in), (string) $in);
        }
    }

    public function testJsonNum(): void
    {
        $this->assertSame(8, PlanNormalizer::jsonNum(8.0));
        $this->assertSame(0, PlanNormalizer::jsonNum(0.0));
        $this->assertSame(4.5, PlanNormalizer::jsonNum(4.5));
    }

    public function testSpecParsersMatchRustRegexes(): void
    {
        $this->assertSame(4, PlanNormalizer::parseCpuCount('4 vCPU Cores'));
        $this->assertSame(3, PlanNormalizer::parseCpuCount('3 Physical Cores'));
        $this->assertSame(1, PlanNormalizer::parseCpuCount('1 Core'));
        $this->assertNull(PlanNormalizer::parseCpuCount('Cores 4'));
        $this->assertSame(8, PlanNormalizer::parseRamGb('8 GB RAM'));
        $this->assertSame(0.5, PlanNormalizer::parseRamGb('0.5GB RAM'));
        $this->assertSame(1000, PlanNormalizer::parsePortSpeedMbps('1 Gbit/s Port'));
        $this->assertSame(200, PlanNormalizer::parsePortSpeedMbps('200 Mbit/s Port'));
        $this->assertSame(1400, PlanNormalizer::parseStorageGb('1.4 TB SSD'));
        $this->assertSame(75, PlanNormalizer::parseStorageGb('75 GB NVMe'));
        $this->assertNull(PlanNormalizer::parseStorageGb(''));
        $this->assertSame('75 GB NVMe', PlanNormalizer::normalizeStorageLabel('75 GB NVMe SSD '));
        $this->assertSame('75 GB NVMe', PlanNormalizer::normalizeStorageLabel('75 GB NVME'));
    }

    public function testNullDiscountAndSetupAreTreatedAsZero(): void
    {
        $n = new PlanNormalizer();
        $plan = $n->normalize([
            'slug' => 'cloud-vps-10', 'title' => 'Cloud VPS 10', 'type' => 'vps',
            'price' => ['EUR' => 4.5],
            'periods' => [
                ['length' => 12, 'discount' => null, 'setup' => null],
                ['length' => 6, 'discount' => ['EUR' => null], 'setup' => ['EUR' => 2.25]],
                ['length' => 0],
                ['length' => 'x'],
            ],
        ], self::AT, null);
        $this->assertCount(2, $plan['periods']);
        $this->assertSame(54, $plan['periods'][0]['total_period_cost']);
        $this->assertSame(4.5, $plan['periods'][0]['effective_monthly']);
        $this->assertSame(29.25, $plan['periods'][1]['total_period_cost']);
    }

    public function testNormalizerRejectsUntrustworthyRecords(): void
    {
        $n = new PlanNormalizer();
        foreach ([
            ['slug' => 'cloud-vps-10', 'type' => 'vps', 'price' => ['EUR' => 0]],
            ['slug' => 'cloud-vps-10', 'type' => 'weird', 'price' => ['EUR' => 4.5]],
            ['slug' => '../x', 'type' => 'vps', 'price' => ['EUR' => 4.5]],
            ['type' => 'vps', 'price' => ['EUR' => 4.5]],
        ] as $bad) {
            try {
                $n->normalize($bad, self::AT, null);
                $this->fail('expected InvalidArgumentException');
            } catch (\InvalidArgumentException $e) {
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testPlanUrlListRanks(): void
    {
        $l = new PlanUrlList();
        $this->assertSame(16, count($l->urls()));
        $this->assertSame(12, $l->rankOf('https://contabo.com/en/vds/vds-s/'));
        $this->assertSame(1, $l->familyRankOf('https://contabo.com/en/vds/vds-s/'));
        $this->assertSame(0, $l->rankOf('https://contabo.com/en/vps/nope/'));
        $this->assertSame('Storage VPS', $l->familyOf('storage-vps-20'));
        $this->assertSame('Unknown', $l->familyOf('mystery'));
        $this->assertSame(
            [
                'Cloud VPS' => 'https://contabo.com/en/vps/cloud-vps-10/',
                'Storage VPS' => 'https://contabo.com/en/storage-vps/storage-vps-10/',
                'Cloud VDS' => 'https://contabo.com/en/vds/vds-s/',
            ],
            $l->firstUrlPerFamily()
        );
    }
}
