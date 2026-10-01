<?php
declare(strict_types=1);

namespace ContaboPricing\Tests\Scrape;

use ContaboPricing\Scrape\CostLedger;
use PHPUnit\Framework\TestCase;
use WHMCS\Database\Capsule;

final class CostLedgerTest extends TestCase
{
    private const NOW = 1790000000; // 2026-09-21 13:33:20 UTC

    protected function setUp(): void
    {
        Capsule::reset();
    }

    /** @param array<string,mixed> $o */
    private function attempt(string $src, int $cost, string $createdAt, array $o = []): void
    {
        Capsule::table('mod_contabo_scrape_run_attempts')->insert(array_merge([
            'run_id' => 1, 'family' => 'Cloud VPS', 'url' => 'u', 'source_id' => $src, 'ok' => 1,
            'cost_micro' => $cost, 'latency_ms' => 1000, 'created_at' => $createdAt,
        ], $o));
    }

    public function testMonthSpendOnlyCountsCurrentMonth(): void
    {
        $ledger = new CostLedger(self::NOW);
        $month = date('Y-m-01 00:00:00', self::NOW);
        $this->attempt('treg_anyapi', 700, $month);                       // boundary: first second counts
        $this->attempt('treg_anyapi', 700, date('Y-m-d H:i:s', self::NOW));
        $this->attempt('alterlab', 4000, date('Y-m-d H:i:s', self::NOW));
        $this->attempt('alterlab', 9999, date('Y-m-d H:i:s', strtotime($month) - 1)); // previous month
        $this->assertSame(['treg_anyapi' => 1400, 'alterlab' => 4000], $ledger->perSourceMonthSpend());
        $this->assertSame(5400, $ledger->monthSpendMicro());
        $this->assertSame(4000, $ledger->sourceMonthSpend('alterlab'));
        $this->assertSame(0, $ledger->sourceMonthSpend('nobody'));
    }

    public function testRecordSumsAttemptsInMemory(): void
    {
        $r = (new CostLedger())->record([
            ['source_id' => 'a', 'cost_micro' => 5], ['source_id' => 'a', 'cost_micro' => 7], ['source_id' => 'b', 'cost_micro' => 1],
        ]);
        $this->assertSame(['total' => 13, 'by_source' => ['a' => 12, 'b' => 1]], $r);
    }

    public function testStatsWindowsAndMedian(): void
    {
        $ledger = new CostLedger(self::NOW);
        $recent = date('Y-m-d H:i:s', self::NOW - 3600);
        $old = date('Y-m-d H:i:s', self::NOW - 10 * 86400);
        $tooOld = date('Y-m-d H:i:s', self::NOW - 31 * 86400);
        $this->attempt('s', 0, $recent, ['latency_ms' => 100]);
        $this->attempt('s', 0, $recent, ['latency_ms' => 300]);
        $this->attempt('s', 0, $old, ['latency_ms' => 200]);
        $this->attempt('s', 0, $recent, ['ok' => 0, 'latency_ms' => 5]);
        $this->attempt('s', 0, $old, ['ok' => 0]);
        $this->attempt('s', 0, $tooOld);
        $st = $ledger->stats()['s'];
        $this->assertSame(5, $st['attempts_30d']);
        $this->assertSame(3, $st['ok_30d']);
        $this->assertSame(1, $st['failures_24h']);
        $this->assertSame(200, $st['p50_latency_ms']);
    }
}
