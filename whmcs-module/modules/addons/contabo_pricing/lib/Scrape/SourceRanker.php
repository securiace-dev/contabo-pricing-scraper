<?php
declare(strict_types=1);

namespace ContaboPricing\Scrape;

/**
 * Orders enabled source rows for S1 page fetches.
 *
 * auto:   rate  = (prior_ok*20 + ok_30d) / (20 + attempts_30d)
 *         score = price_micro / max(rate, 0.05) + p50_latency_ms / 1000
 *         score *= 1 + 0.5 * min(consecutive_failures, 6)       (lower is better)
 *         3 failures within 24 h park a source at the end for 6 h.
 * manual: priority ASC, NULL last (then seed order).
 *
 * Disabled sources are excluded. Manual-only sources (the agent) and sources
 * whose HTML cannot carry the sapper blob are never offered for S1 unless the
 * caller explicitly asks for manual-only sources.
 */
final class SourceRanker
{
    /**
     * Round-2 2026-10-01 priors (measured on the product page that carries the
     * catalogue blob). price in micro-USD, p50 in ms. `disabled` sources are
     * never auto-ranked (manual rank mode and only_source runs still may).
     */
    public const PRIORS = [
        'treg_anyapi' => ['price' => 700, 'ok' => 0.95, 'p50' => 10000],
        'alterlab' => ['price' => 4000, 'ok' => 0.95, 'p50' => 20000],
        // capacity 503 observed in round 2: kept for manual tests only
        'treg_litescrape' => ['price' => 150, 'ok' => 0.50, 'p50' => 9000, 'disabled' => true],
        'tinyfish_fetch' => ['price' => 0, 'ok' => 0.999, 'p50' => 5000, 'sapper' => false],
        'tinyfish_agent' => ['price' => 640000, 'ok' => 0.90, 'p50' => 60000, 'manual' => true],
    ];
    private const DEFAULT_PRIOR = ['price' => 1000, 'ok' => 0.5, 'p50' => 5000];

    public const PARK_FAILURES = 3;
    public const PARK_SECONDS = 21600;

    /** @return array{price:int, ok:float, p50:int, sapper?:bool, manual?:bool, disabled?:bool} */
    public static function prior(string $sourceId): array
    {
        return self::PRIORS[$sourceId] ?? self::DEFAULT_PRIOR;
    }

    /**
     * @param list<array<string,mixed>> $enabledSources raw source rows
     * @return list<array<string,mixed>> same rows, best first
     */
    public function order(
        array $enabledSources,
        CostLedger $ledger,
        string $rankMode,
        bool $includeManualOnly = false,
        ?int $now = null
    ): array {
        $now = $now ?? time();
        $rows = [];
        foreach ($enabledSources as $i => $row) {
            if ((int) ($row['enabled'] ?? 0) !== 1) {
                continue;
            }
            $prior = self::prior((string) ($row['source_id'] ?? ''));
            if (!$includeManualOnly && !empty($prior['manual'])) {
                continue;
            }
            // a `disabled` source (e.g. litescrape: capacity 503 observed) is never auto-ranked;
            // manual rank mode and explicit only_source runs respect the operator's choice
            if (!$includeManualOnly && $rankMode !== 'manual' && !empty($prior['disabled'])) {
                continue;
            }
            if (isset($prior['sapper']) && $prior['sapper'] === false) {
                continue;
            }
            $row['_seq'] = $i;
            $rows[] = $row;
        }

        if ($rankMode === 'manual') {
            usort($rows, static function (array $a, array $b): int {
                $pa = ($a['priority'] ?? null) === null ? PHP_INT_MAX : (int) $a['priority'];
                $pb = ($b['priority'] ?? null) === null ? PHP_INT_MAX : (int) $b['priority'];
                return $pa !== $pb ? $pa <=> $pb : $a['_seq'] <=> $b['_seq'];
            });
            return $this->strip($rows);
        }

        $stats = $ledger->stats();
        $scored = [];
        foreach ($rows as $row) {
            $id = (string) $row['source_id'];
            $prior = self::prior($id);
            $st = $stats[$id] ?? ['attempts_30d' => 0, 'ok_30d' => 0, 'failures_24h' => 0, 'p50_latency_ms' => null];
            $rate = ($prior['ok'] * 20 + $st['ok_30d']) / (20 + $st['attempts_30d']);
            $p50 = $st['p50_latency_ms'] ?? $prior['p50'];
            $score = $prior['price'] / max($rate, 0.05) + $p50 / 1000;
            $score *= 1 + 0.5 * min((int) ($row['consecutive_failures'] ?? 0), 6);

            $parked = false;
            $lastFail = isset($row['last_fail_at']) ? strtotime((string) $row['last_fail_at']) : false;
            if ($st['failures_24h'] >= self::PARK_FAILURES && $lastFail !== false && ($now - $lastFail) < self::PARK_SECONDS) {
                $parked = true;
            }
            $row['_score'] = $score;
            $row['_parked'] = $parked;
            $scored[] = $row;
        }
        usort($scored, static function (array $a, array $b): int {
            if ($a['_parked'] !== $b['_parked']) {
                return $a['_parked'] ? 1 : -1;
            }
            if ($a['_score'] !== $b['_score']) {
                return $a['_score'] <=> $b['_score'];
            }
            return $a['_seq'] <=> $b['_seq'];
        });
        return $this->strip($scored);
    }

    /**
     * @param list<array<string,mixed>> $rows
     * @return list<array<string,mixed>>
     */
    private function strip(array $rows): array
    {
        foreach ($rows as $i => $r) {
            unset($r['_seq'], $r['_score'], $r['_parked']);
            $rows[$i] = $r;
        }
        return $rows;
    }
}
