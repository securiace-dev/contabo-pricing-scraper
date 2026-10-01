<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

use WHMCS\Database\Capsule;

/**
 * Spend and reliability accounting derived from mod_contabo_scrape_run_attempts
 * (the attempts table is the ledger: one row per paid or free provider call).
 * All amounts are micro-USD integers.
 */
class CostLedger
{
    private const TABLE = 'mod_contabo_scrape_run_attempts';

    /** @var int|null fixed clock for tests */
    private $now;

    public function __construct(?int $now = null)
    {
        $this->now = $now;
    }

    private function now(): int
    {
        return $this->now ?? time();
    }

    private function monthStart(): string
    {
        return date('Y-m-01 00:00:00', $this->now());
    }

    /** Spend this calendar month across every source. */
    public function monthSpendMicro(): int
    {
        return array_sum($this->perSourceMonthSpend());
    }

    /** @return array<string,int> source_id => micro-USD spent this calendar month */
    public function perSourceMonthSpend(): array
    {
        $out = [];
        foreach (Capsule::table(self::TABLE)->where('created_at', '>=', $this->monthStart())->get() as $r) {
            $r = (array) $r;
            $id = (string) ($r['source_id'] ?? '');
            $out[$id] = ($out[$id] ?? 0) + (int) ($r['cost_micro'] ?? 0);
        }
        return $out;
    }

    public function sourceMonthSpend(string $sourceId): int
    {
        return $this->perSourceMonthSpend()[$sourceId] ?? 0;
    }

    /**
     * Totals for one run's in-memory attempts.
     *
     * @param list<array<string,mixed>> $attempts
     * @return array{total:int, by_source:array<string,int>}
     */
    public function record(array $attempts): array
    {
        $by = [];
        $total = 0;
        foreach ($attempts as $a) {
            $c = (int) ($a['cost_micro'] ?? 0);
            $id = (string) ($a['source_id'] ?? '');
            $by[$id] = ($by[$id] ?? 0) + $c;
            $total += $c;
        }
        return ['total' => $total, 'by_source' => $by];
    }

    /**
     * Rolling reliability numbers per source for ranking.
     *
     * @return array<string,array{attempts_30d:int, ok_30d:int, failures_24h:int, p50_latency_ms:?int}>
     */
    public function stats(): array
    {
        $since30 = date('Y-m-d H:i:s', $this->now() - 30 * 86400);
        $since24 = date('Y-m-d H:i:s', $this->now() - 86400);
        $out = [];
        $lat = [];
        foreach (Capsule::table(self::TABLE)->where('created_at', '>=', $since30)->get() as $r) {
            $r = (array) $r;
            $id = (string) ($r['source_id'] ?? '');
            if (!isset($out[$id])) {
                $out[$id] = ['attempts_30d' => 0, 'ok_30d' => 0, 'failures_24h' => 0, 'p50_latency_ms' => null];
                $lat[$id] = [];
            }
            $out[$id]['attempts_30d']++;
            $ok = (int) ($r['ok'] ?? 0) === 1;
            if ($ok) {
                $out[$id]['ok_30d']++;
                $lat[$id][] = (int) ($r['latency_ms'] ?? 0);
            } elseif ((string) ($r['created_at'] ?? '') >= $since24) {
                $out[$id]['failures_24h']++;
            }
        }
        foreach ($lat as $id => $l) {
            if ($l !== []) {
                sort($l);
                $out[$id]['p50_latency_ms'] = $l[(int) floor((count($l) - 1) / 2)];
            }
        }
        return $out;
    }
}
