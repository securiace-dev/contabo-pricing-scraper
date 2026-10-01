<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\CostLedger;
use ContaboPricing\Scrape\SourceRanker;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class SourceRankerTest extends TestCase
{
    private const NOW = 1790000000;

    /** @var SourceRanker */
    private $ranker;

    protected function setUp(): void
    {
        Capsule::reset();
        $this->ranker = new SourceRanker();
    }

    /** @param array<string,mixed> $o @return array<string,mixed> */
    private function row(string $id, array $o = []): array
    {
        return array_merge(['source_id' => $id, 'enabled' => 1, 'priority' => null, 'consecutive_failures' => 0, 'last_fail_at' => null], $o);
    }

    /** @param list<array<string,mixed>> $rows @return list<string> */
    private function ids(array $rows): array
    {
        return array_map(static function (array $r): string { return (string) $r['source_id']; }, $rows);
    }

    private function attempt(string $src, int $ok, int $ageSec, int $lat = 1000): void
    {
        Capsule::table('mod_contabo_scrape_run_attempts')->insert([
            'run_id' => 1, 'source_id' => $src, 'ok' => $ok, 'cost_micro' => 0, 'latency_ms' => $lat,
            'created_at' => date('Y-m-d H:i:s', self::NOW - $ageSec),
        ]);
    }

    public function testPriorsMatchSpike0(): void
    {
        $p = SourceRanker::PRIORS;
        $this->assertSame(['price' => 700, 'ok' => 0.80, 'p50' => 3000], $p['treg_anyapi']);
        $this->assertSame(['price' => 4000, 'ok' => 0.95, 'p50' => 16000], $p['alterlab']);
        $this->assertSame(['price' => 150, 'ok' => 0.78, 'p50' => 8000], $p['treg_litescrape']);
        $this->assertFalse($p['tinyfish_fetch']['sapper']);
        $this->assertTrue($p['tinyfish_agent']['manual']);
    }

    public function testAutoRanksByPriorScoreAndExcludesUnusable(): void
    {
        $rows = [
            $this->row('alterlab'), $this->row('treg_anyapi'), $this->row('tinyfish_fetch'),
            $this->row('tinyfish_agent'), $this->row('treg_litescrape', ['enabled' => 0]),
        ];
        $order = $this->ranker->order($rows, new CostLedger(self::NOW), 'auto', false, self::NOW);
        $this->assertSame(['treg_anyapi', 'alterlab'], $this->ids($order), 'agent is manual-only, tinyfish_fetch has no blob, disabled excluded');
    }

    public function testManualOnlyIncludedOnRequest(): void
    {
        $order = $this->ranker->order([$this->row('treg_anyapi'), $this->row('tinyfish_agent')], new CostLedger(self::NOW), 'auto', true, self::NOW);
        $this->assertContains('tinyfish_agent', $this->ids($order));
    }

    public function testRateFormulaBlendsPriorWithObservedStats(): void
    {
        // alterlab: prior ok .95, price 4000. treg_anyapi observed to fail 20/20 -> rate (16+0)/40 = .4
        // treg score = 700/.4 + 3 = 1753 ; still < alterlab 4000/.95 + 16 = 4226
        for ($i = 0; $i < 20; $i++) {
            $this->attempt('treg_anyapi', 0, 3 * 86400);
        }
        $order = $this->ranker->order([$this->row('alterlab'), $this->row('treg_anyapi')], new CostLedger(self::NOW), 'auto', false, self::NOW);
        $this->assertSame(['treg_anyapi', 'alterlab'], $this->ids($order));
        // rate floor 0.05: 200 failures -> rate = 16/220 = .0727 ; 700/.0727 = 9625 > 4226 -> alterlab first
        for ($i = 0; $i < 180; $i++) {
            $this->attempt('treg_anyapi', 0, 3 * 86400);
        }
        $order = $this->ranker->order([$this->row('alterlab'), $this->row('treg_anyapi')], new CostLedger(self::NOW), 'auto', false, self::NOW);
        $this->assertSame(['alterlab', 'treg_anyapi'], $this->ids($order));
    }

    public function testConsecutiveFailurePenaltyCapsAtSix(): void
    {
        // treg score 878.5; x(1+.5*3)=2.5 -> 2196 < alterlab 4226 ; x(1+.5*6)=4 -> 3514 < 4226 ; cap means 9 failures == 6
        $l = new CostLedger(self::NOW);
        $o3 = $this->ranker->order([$this->row('alterlab'), $this->row('treg_anyapi', ['consecutive_failures' => 3])], $l, 'auto', false, self::NOW);
        $this->assertSame('treg_anyapi', $o3[0]['source_id']);
        $o9 = $this->ranker->order([$this->row('alterlab'), $this->row('treg_anyapi', ['consecutive_failures' => 9])], $l, 'auto', false, self::NOW);
        $this->assertSame('treg_anyapi', $o9[0]['source_id'], 'capped at 6 => 3514 still beats 4226');
        // litescrape (150/.78+8=200.3): x4 = 801 ; vs treg 878 -> litescrape still first even with 6 failures
        $o = $this->ranker->order([$this->row('treg_anyapi'), $this->row('treg_litescrape', ['consecutive_failures' => 6])], $l, 'auto', false, self::NOW);
        $this->assertSame('treg_litescrape', $o[0]['source_id']);
        $o = $this->ranker->order([$this->row('treg_anyapi'), $this->row('treg_litescrape', ['consecutive_failures' => 7])], $l, 'auto', false, self::NOW);
        $this->assertSame('treg_litescrape', $o[0]['source_id'], '7 behaves like 6 (cap)');
    }

    public function testThreeFailuresIn24hParkForSixHours(): void
    {
        for ($i = 0; $i < 3; $i++) {
            $this->attempt('treg_anyapi', 0, 3600);
        }
        $failedAt = date('Y-m-d H:i:s', self::NOW - 3600);
        $rows = [$this->row('treg_anyapi', ['last_fail_at' => $failedAt]), $this->row('alterlab')];
        $o = $this->ranker->order($rows, new CostLedger(self::NOW), 'auto', false, self::NOW);
        $this->assertSame(['alterlab', 'treg_anyapi'], $this->ids($o), 'parked to the end');

        // 6h later the park lapses (the failures are still inside 24h)
        $o = $this->ranker->order($rows, new CostLedger(self::NOW + 21601), 'auto', false, self::NOW + 21601);
        $this->assertSame('treg_anyapi', $o[0]['source_id']);

        // only two failures: not parked
        Capsule::reset();
        for ($i = 0; $i < 2; $i++) {
            $this->attempt('treg_anyapi', 0, 3600);
        }
        $o = $this->ranker->order($rows, new CostLedger(self::NOW), 'auto', false, self::NOW);
        $this->assertSame('treg_anyapi', $o[0]['source_id']);
    }

    public function testManualModeUsesPriorityAscNullLast(): void
    {
        $rows = [
            $this->row('alterlab', ['priority' => null]),
            $this->row('treg_litescrape', ['priority' => 5]),
            $this->row('treg_anyapi', ['priority' => 2]),
            $this->row('tinyfish_agent', ['priority' => 1]),
            $this->row('tinyfish_fetch', ['priority' => 3, 'enabled' => 0]),
        ];
        $o = $this->ranker->order($rows, new CostLedger(self::NOW), 'manual', false, self::NOW);
        $this->assertSame(['treg_anyapi', 'treg_litescrape', 'alterlab'], $this->ids($o));
    }
}
