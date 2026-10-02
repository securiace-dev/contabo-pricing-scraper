<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\PlanDiffer;
use PHPUnit\Framework\TestCase;

/** Replays docs/test-vectors/differ.json (corrected expectations, not archived behaviour). */
final class PlanDifferTest extends TestCase
{
    /** @return array<string,array{0:array<string,mixed>}> */
    public function vectors(): array
    {
        $doc = json_decode((string) file_get_contents(__DIR__ . '/../fixtures/vectors/differ.json'), true);
        $out = [];
        foreach ($doc['cases'] as $c) {
            $out[$c['name']] = [$c];
        }
        return $out;
    }

    /**
     * @param list<array<string,mixed>> $rows
     * @return array<string,array<string,mixed>>
     */
    private static function toPlans(array $rows): array
    {
        $plans = [];
        foreach ($rows as $r) {
            $plans[(string) $r['upstream_sku']] = [
                'product_name' => $r['spec']['product_name'] ?? '',
                'specs_parsed' => $r['spec']['specs_parsed'] ?? [],
                'base_monthly_price' => $r['cost_base_currency'],
            ];
        }
        return $plans;
    }

    /** @dataProvider vectors
     * @param array<string,mixed> $case */
    public function testVector(array $case): void
    {
        $prev = self::toPlans($case['input']['prev']);
        $next = self::toPlans($case['input']['next']);
        $diffs = PlanDiffer::diff($prev, $next);

        $this->assertSame($case['expected']['kinds'], array_column($diffs, 'kind'), $case['name']);

        foreach ($case['expected']['diffs'] ?? [] as $i => $exp) {
            $got = $diffs[$i];
            $this->assertSame($exp['catalog_sku'], $got['catalog_sku']);
            foreach (['cost_before', 'cost_after'] as $k) {
                if ($exp[$k] === null) {
                    $this->assertNull($got[$k]);
                } else {
                    $this->assertEqualsWithDelta((float) $exp[$k], $got[$k], 1e-9);
                }
            }
            $this->assertEquals($exp['spec_before'], $got['spec_before']);
            $this->assertEquals($exp['spec_after'], $got['spec_after']);
            if (in_array($exp['kind'], ['cost_increase', 'cost_decrease'], true)) {
                $this->assertMatchesRegularExpression('/\d+\.\d+%/', $got['notes'], 'cost diffs carry magnitude');
            }
        }
    }

    public function testPureAndOrderIndependent(): void
    {
        $doc = $this->vectors()['mixed_batch_only_changed_skus'][0];
        $prev = self::toPlans($doc['input']['prev']);
        $next = self::toPlans($doc['input']['next']);
        $a = PlanDiffer::diff($prev, $next);
        $prevCopy = $prev;
        $b = PlanDiffer::diff(array_reverse($prev, true), array_reverse($next, true));
        $this->assertSame($a, $b);
        $this->assertSame($prevCopy, $prev);
        $this->assertSame($a, PlanDiffer::diff($prev, $next));
    }

    public function testScrapeFailedHasNullSkuAndNoPerPlanRetire(): void
    {
        $d = PlanDiffer::diff(['a' => ['product_name' => 'A', 'base_monthly_price' => 1, 'specs_parsed' => []]], []);
        $this->assertCount(1, $d);
        $this->assertSame('scrape_failed', $d[0]['kind']);
        $this->assertNull($d[0]['catalog_sku']);
    }

    public function testCostMagnitudeInNotes(): void
    {
        $p = static function ($c) {
            return ['x' => ['product_name' => 'X', 'base_monthly_price' => $c, 'specs_parsed' => []]];
        };
        $d = PlanDiffer::diff($p(10.0), $p(11.0));
        $this->assertStringContainsString('+10.00%', $d[0]['notes']);
    }

    public function testStorageTypeRanking(): void
    {
        $mk = static function (string $t) {
            return ['x' => ['product_name' => 'X', 'base_monthly_price' => 1, 'specs_parsed' => ['storage_primary_type' => $t]]];
        };
        $this->assertSame('spec_increase', PlanDiffer::diff($mk('HDD'), $mk('SSD'))[0]['kind']);
        $this->assertSame('spec_increase', PlanDiffer::diff($mk('SSD'), $mk('NVMe'))[0]['kind']);
        $this->assertSame('spec_decrease', PlanDiffer::diff($mk('NVMe'), $mk('SSD'))[0]['kind']);
        $this->assertSame('parser_error', PlanDiffer::diff($mk('NVMe'), $mk('floppy'))[0]['kind']);
    }
}
